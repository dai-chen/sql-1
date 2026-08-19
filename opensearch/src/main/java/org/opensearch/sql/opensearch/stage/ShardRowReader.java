/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.mapper.Uid;
import org.opensearch.search.lookup.SourceLookup;

/**
 * Per-leaf reader that materializes one {@code Object[]} row per matching document. For each field
 * it tries doc_values first (keyed by OpenSearch type name); when doc_values are absent at runtime
 * it falls back to {@code _source} via {@link SourceLookup}.
 *
 * <p>Supported type-name → reader-kind mapping (must stay in agreement with {@link
 * RelFragmentCodec}'s OS_TYPE_TO_SQL_TYPE):
 *
 * <ul>
 *   <li>keyword → SortedSetDocValues (BytesRef → utf8ToString)
 *   <li>long → SortedNumericDocValues as Long (BIGINT)
 *   <li>integer → SortedNumericDocValues, cast to Integer (INTEGER)
 *   <li>date → SortedNumericDocValues as Long epoch millis (TIMESTAMP)
 *   <li>double → SortedNumericDocValues decoded via NumericUtils.sortableLongToDouble (DOUBLE)
 *   <li>float → SortedNumericDocValues decoded via NumericUtils.sortableIntToFloat (REAL)
 *   <li>boolean → SortedNumericDocValues, 0/1 → Boolean
 *   <li>text → _source only (text fields have no doc_values)
 * </ul>
 */
public class ShardRowReader {

  /** Per-field reader abstraction. */
  @FunctionalInterface
  interface FieldReader {
    Object read(int doc) throws IOException;
  }

  private final FieldReader[] readers;
  private final int fieldCount;

  private ShardRowReader(FieldReader[] readers) {
    this.readers = readers;
    this.fieldCount = readers.length;
  }

  /**
   * Creates a ShardRowReader for the given leaf context and field descriptors.
   *
   * @param ctx the leaf reader context
   * @param fields ordered list of field descriptors
   * @param sourceLookup SourceLookup positioned to this leaf (caller must call
   *     setSegmentAndDocument before reading)
   * @return a new ShardRowReader instance
   * @throws IOException if an I/O error occurs accessing doc_values
   */
  public static ShardRowReader create(
      LeafReaderContext ctx,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      SourceLookup sourceLookup)
      throws IOException {
    LeafReader leafReader = ctx.reader();
    FieldReader[] readers = new FieldReader[fields.size()];

    for (int i = 0; i < fields.size(); i++) {
      CalciteExecAggregationBuilder.FieldDescriptor fd = fields.get(i);
      String fieldName = fd.getName();
      String typeName = fd.getType().toLowerCase(Locale.ROOT);
      readers[i] = buildReader(leafReader, fieldName, typeName, sourceLookup);
    }
    return new ShardRowReader(readers);
  }

  /**
   * Reads one row for the given doc id. The caller must have already called
   * sourceLookup.setSegmentAndDocument(ctx, doc) before calling this method.
   */
  public Object[] readRow(int doc) throws IOException {
    Object[] row = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      row[i] = readers[i].read(doc);
    }
    return row;
  }

  private static FieldReader buildReader(
      LeafReader leafReader, String fieldName, String typeName, SourceLookup sourceLookup)
      throws IOException {
    // Metadata fields require special handling — they are not in _source or regular doc_values
    if (fieldName.equals("_id")) {
      // _id is stored as a binary value in Lucene; decode via Uid.decodeId which handles
      // the encoding used by OpenSearch's IdFieldMapper.
      return doc -> {
        var storedFields = leafReader.storedFields();
        var document = storedFields.document(doc);
        BytesRef binaryId = document.getBinaryValue("_id");
        if (binaryId != null) {
          return Uid.decodeId(binaryId.bytes, binaryId.offset, binaryId.length);
        }
        // Fallback: try as a string stored field (older format or test fixtures)
        String stringId = document.get("_id");
        return stringId;
      };
    }
    if (fieldName.equals("_index")) {
      // _index is the index name of the shard this reader is operating on. In the aggregation
      // context, the correct value comes from the field descriptor's context (the calling
      // CalciteExecAggregator knows the index name). However, ShardRowReader doesn't have access
      // to SearchContext here. For the PoC, fall back to _source metadata (not available) or null.
      // The integration tests for _index filtering work via the non-staged path because the
      // coordinator already routes to the correct index.
      return doc -> null;
    }
    if (fieldName.equals("_routing")) {
      // _routing is accessible via stored fields
      return doc -> {
        var storedFields = leafReader.storedFields();
        var document = storedFields.document(doc);
        return document.get("_routing");
      };
    }
    switch (typeName) {
      case "keyword":
        return buildKeywordReader(leafReader, fieldName, sourceLookup);
      case "long":
      case "date":
        // Date fields are stored as epoch millis (Long). Calcite's TIMESTAMP type represents
        // timestamps as Long internally. PPL UDFs (month, year etc.) receive the Long and
        // ExprValueUtils.fromObjectValue(Long, TIMESTAMP) converts it to ExprTimestampValue.
        return buildNumericLongReader(leafReader, fieldName, sourceLookup);
      case "integer":
        return buildNumericIntegerReader(leafReader, fieldName, sourceLookup);
      case "double":
        return buildNumericDoubleReader(leafReader, fieldName, sourceLookup);
      case "float":
        return buildNumericFloatReader(leafReader, fieldName, sourceLookup);
      case "boolean":
        return buildBooleanReader(leafReader, fieldName, sourceLookup);
      case "text":
        // text fields have no doc_values, always read from _source
        return doc -> sourceLookup.extractValue(fieldName, null);
      case "object":
      case "nested":
        // Struct/nested fields: navigate the _source using the dotted path to reach the scalar
        // leaf value. For array-of-objects (nested), extractValue returns a List; keep it opaque
        // for projection but filter equality on scalars won't match against a List.
        return doc -> extractNestedValue(sourceLookup, fieldName);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unrecognized OpenSearch field type '%s' for field '%s'", typeName, fieldName));
    }
  }

  private static FieldReader buildKeywordReader(
      LeafReader leafReader, String fieldName, SourceLookup sourceLookup) throws IOException {
    SortedSetDocValues dv = leafReader.getSortedSetDocValues(fieldName);
    if (dv == null) {
      // Runtime absence — fall back to _source
      return doc -> sourceLookup.extractValue(fieldName, null);
    }
    final SortedSetDocValues docValues = dv;
    return doc -> {
      if (docValues.advanceExact(doc)) {
        // PoC simplification: take the first value for multi-valued fields
        long ord = docValues.nextOrd();
        return docValues.lookupOrd(ord).utf8ToString();
      }
      return null;
    };
  }

  private static FieldReader buildNumericLongReader(
      LeafReader leafReader, String fieldName, SourceLookup sourceLookup) throws IOException {
    SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(fieldName);
    if (dv == null) {
      // Runtime absence — fall back to _source
      return doc -> {
        Object val = sourceLookup.extractValue(fieldName, null);
        if (val instanceof Number) {
          return ((Number) val).longValue();
        }
        // PoC limitation: _source may return List for multi-valued or String for numeric
        return val;
      };
    }
    final SortedNumericDocValues docValues = dv;
    return doc -> {
      if (docValues.advanceExact(doc)) {
        // PoC simplification: take the first value for multi-valued fields
        return docValues.nextValue();
      }
      return null;
    };
  }

  private static FieldReader buildNumericIntegerReader(
      LeafReader leafReader, String fieldName, SourceLookup sourceLookup) throws IOException {
    SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(fieldName);
    if (dv == null) {
      // Runtime absence — fall back to _source
      return doc -> {
        Object val = sourceLookup.extractValue(fieldName, null);
        if (val instanceof Number) {
          return ((Number) val).intValue();
        }
        // PoC limitation: _source may return List for multi-valued or String for numeric
        return val;
      };
    }
    final SortedNumericDocValues docValues = dv;
    return doc -> {
      if (docValues.advanceExact(doc)) {
        // PoC simplification: take the first value for multi-valued fields
        return (int) docValues.nextValue();
      }
      return null;
    };
  }

  private static FieldReader buildNumericDoubleReader(
      LeafReader leafReader, String fieldName, SourceLookup sourceLookup) throws IOException {
    SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(fieldName);
    if (dv == null) {
      // Runtime absence — fall back to _source
      return doc -> {
        Object val = sourceLookup.extractValue(fieldName, null);
        if (val instanceof Number) {
          return ((Number) val).doubleValue();
        }
        // PoC limitation: _source may return List for multi-valued or String for numeric
        return val;
      };
    }
    final SortedNumericDocValues docValues = dv;
    return doc -> {
      if (docValues.advanceExact(doc)) {
        // PoC simplification: take the first value for multi-valued fields
        return NumericUtils.sortableLongToDouble(docValues.nextValue());
      }
      return null;
    };
  }

  private static FieldReader buildNumericFloatReader(
      LeafReader leafReader, String fieldName, SourceLookup sourceLookup) throws IOException {
    SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(fieldName);
    if (dv == null) {
      // Runtime absence — fall back to _source
      return doc -> {
        Object val = sourceLookup.extractValue(fieldName, null);
        if (val instanceof Number) {
          return ((Number) val).floatValue();
        }
        // PoC limitation: _source may return List for multi-valued or String for numeric
        return val;
      };
    }
    final SortedNumericDocValues docValues = dv;
    return doc -> {
      if (docValues.advanceExact(doc)) {
        // PoC simplification: take the first value for multi-valued fields
        return NumericUtils.sortableIntToFloat((int) docValues.nextValue());
      }
      return null;
    };
  }

  private static FieldReader buildBooleanReader(
      LeafReader leafReader, String fieldName, SourceLookup sourceLookup) throws IOException {
    SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(fieldName);
    if (dv == null) {
      // Runtime absence — fall back to _source
      return doc -> sourceLookup.extractValue(fieldName, null);
    }
    final SortedNumericDocValues docValues = dv;
    return doc -> {
      if (docValues.advanceExact(doc)) {
        // PoC simplification: take the first value for multi-valued fields
        return docValues.nextValue() != 0;
      }
      return null;
    };
  }

  /**
   * Navigates the _source document along a dotted field path to extract a scalar leaf value. For
   * nested/object fields that are arrays of objects (e.g. "address" is nested and the path is
   * "address.city"), SourceLookup.extractValue returns a flattened List of all leaf values. For
   * Calcite filters comparing to a scalar, a List never matches equality. This method unwraps
   * single-element lists to enable scalar comparison in the common case.
   */
  @SuppressWarnings("unchecked")
  private static Object extractNestedValue(SourceLookup sourceLookup, String fieldName) {
    Object value = sourceLookup.extractValue(fieldName, null);
    // If extractValue returned a single-element List, unwrap to scalar for filter comparisons
    if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      if (list.size() == 1) {
        return list.get(0);
      }
      // Multi-value: return as-is (projection will show the list; filter equality won't match)
    }
    return value;
  }
}
