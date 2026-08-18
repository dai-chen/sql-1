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
import org.apache.lucene.util.NumericUtils;
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
    switch (typeName) {
      case "keyword":
        return buildKeywordReader(leafReader, fieldName, sourceLookup);
      case "long":
      case "date":
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
}
