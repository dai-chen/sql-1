/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.mapper.Uid;
import org.opensearch.search.lookup.SourceLookup;

/**
 * Per-leaf reader that materializes one or more {@code Object[]} rows per matching document. For
 * each field it tries doc_values first (keyed by OpenSearch type name); when doc_values are absent
 * at runtime it falls back to {@code _source} via {@link SourceLookup}.
 *
 * <p>When fields under a nested path are requested, the reader expands one parent document into
 * multiple rows — one per nested sub-document — with sibling alignment: fields under the SAME
 * nested path are read from the SAME sub-document within a given output row. Fields under DIFFERENT
 * nested paths produce a cross-product across those paths (deliberate documented limitation).
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
  private final String[] fieldNames;

  /**
   * Per-field nested path (null for non-nested). Same length as readers. Used for nested row
   * expansion in {@link #readRows(int)}.
   */
  private final String[] fieldNestedPaths;

  /** True when at least one field has a non-null nested path. */
  private final boolean hasNestedFields;

  private final SourceLookup sourceLookup;

  private ShardRowReader(
      FieldReader[] readers,
      String[] fieldNames,
      String[] fieldNestedPaths,
      boolean hasNestedFields,
      SourceLookup sourceLookup) {
    this.readers = readers;
    this.fieldCount = readers.length;
    this.fieldNames = fieldNames;
    this.fieldNestedPaths = fieldNestedPaths;
    this.hasNestedFields = hasNestedFields;
    this.sourceLookup = sourceLookup;
  }

  /**
   * Creates a ShardRowReader for the given leaf context and field descriptors with no nested path
   * information. All fields are treated as non-nested.
   */
  public static ShardRowReader create(
      LeafReaderContext ctx,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      SourceLookup sourceLookup)
      throws IOException {
    return create(ctx, fields, sourceLookup, Map.of());
  }

  /**
   * Creates a ShardRowReader for the given leaf context and field descriptors.
   *
   * @param ctx the leaf reader context
   * @param fields ordered list of field descriptors
   * @param sourceLookup SourceLookup positioned to this leaf (caller must call
   *     setSegmentAndDocument before reading)
   * @param nestedPaths per-field nested path map (field name → nested parent path, null if not
   *     nested)
   * @return a new ShardRowReader instance
   * @throws IOException if an I/O error occurs accessing doc_values
   */
  public static ShardRowReader create(
      LeafReaderContext ctx,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      SourceLookup sourceLookup,
      Map<String, String> nestedPaths)
      throws IOException {
    LeafReader leafReader = ctx.reader();
    int size = fields.size();
    FieldReader[] readers = new FieldReader[size];
    String[] names = new String[size];
    String[] nestedPathArray = new String[size];
    boolean hasNested = false;

    for (int i = 0; i < size; i++) {
      CalciteExecAggregationBuilder.FieldDescriptor fd = fields.get(i);
      String fieldName = fd.getName();
      String typeName = fd.getType().toLowerCase(Locale.ROOT);
      names[i] = fieldName;
      String nestedPath = nestedPaths.get(fieldName);
      nestedPathArray[i] = nestedPath;
      if (nestedPath != null) {
        hasNested = true;
        // Nested fields are read from _source during row expansion, not via doc_values.
        // The parent Lucene document has no doc_values for nested leaves.
        readers[i] = doc -> null;
      } else {
        readers[i] = buildReader(leafReader, fieldName, typeName, sourceLookup);
      }
    }
    return new ShardRowReader(readers, names, nestedPathArray, hasNested, sourceLookup);
  }

  /**
   * Reads one row for the given doc id from doc_values and/or _source. Returns a single Object[]
   * with one slot per requested field. Does not perform nested sub-document expansion — nested-path
   * fields remain null (as set by the no-op reader assigned at construction time). The caller must
   * have already called sourceLookup.setSegmentAndDocument(ctx, doc) before calling this method.
   */
  public Object[] readRow(int doc) throws IOException {
    Object[] row = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      row[i] = readers[i].read(doc);
    }
    return row;
  }

  /**
   * Reads one or more rows for the given doc id, expanding nested sub-documents into multiple rows
   * with sibling alignment. The caller must have already called
   * sourceLookup.setSegmentAndDocument(ctx, doc) before calling this method.
   *
   * <p>Fields under the same nested path are aligned: they come from the same sub-document in each
   * output row. Fields under different nested paths produce a cross-product (deliberate documented
   * limitation). Non-nested fields are repeated in every output row.
   */
  @SuppressWarnings("unchecked")
  public List<Object[]> readRows(int doc) throws IOException {
    if (!hasNestedFields) {
      List<Object[]> single = new ArrayList<>(1);
      single.add(readRow(doc));
      return single;
    }

    // 1. Read non-nested field values from doc_values/source
    Object[] baseRow = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      if (fieldNestedPaths[i] == null) {
        baseRow[i] = readers[i].read(doc);
      }
    }

    // 2. Group field indices by nested path for sibling alignment
    Map<String, List<Integer>> pathToFieldIndices = new LinkedHashMap<>();
    for (int i = 0; i < fieldCount; i++) {
      if (fieldNestedPaths[i] != null) {
        pathToFieldIndices.computeIfAbsent(fieldNestedPaths[i], k -> new ArrayList<>()).add(i);
      }
    }

    // 3. For each distinct nested path, read sub-document array from _source and extract
    //    per-sub-document field values with sibling alignment.
    Map<String, Object> source = sourceLookup.source();
    List<Object[]> result = new ArrayList<>();
    result.add(baseRow.clone());

    for (Map.Entry<String, List<Integer>> entry : pathToFieldIndices.entrySet()) {
      String nestedPath = entry.getKey();
      List<Integer> fieldIndices = entry.getValue();

      // Navigate _source to the nested array
      Object nestedValue = navigateSource(source, nestedPath);
      List<Map<String, Object>> subDocs;
      if (nestedValue instanceof List) {
        subDocs = (List<Map<String, Object>>) nestedValue;
      } else if (nestedValue instanceof Map) {
        subDocs = List.of((Map<String, Object>) nestedValue);
      } else if (nestedValue == null) {
        // Legitimately sparse parent document: the nested array is absent from _source.
        // Emit one row with null nested values; a downstream filter on nested fields drops it.
        subDocs = List.of(Map.of());
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Nested path '%s' for field '%s' has unexpected _source shape: %s",
                nestedPath, fieldNames[fieldIndices.get(0)], nestedValue.getClass().getName()));
      }

      // Cross-product: for each existing partial row × each sub-document, produce a new row
      List<Object[]> expanded = new ArrayList<>(result.size() * subDocs.size());
      for (Object[] existingRow : result) {
        for (Map<String, Object> subDoc : subDocs) {
          Object[] newRow = existingRow.clone();
          for (int fieldIdx : fieldIndices) {
            // Extract the leaf value relative to the nested path
            String leafName = fieldNames[fieldIdx].substring(nestedPath.length() + 1);
            newRow[fieldIdx] = navigateSource(subDoc, leafName);
          }
          expanded.add(newRow);
        }
      }
      result = expanded;
    }

    return result;
  }

  /**
   * Navigates a nested Map structure following a dotted path. Returns the value at the leaf, or
   * null if any intermediate segment is absent.
   */
  @SuppressWarnings("unchecked")
  static Object navigateSource(Map<String, Object> source, String dottedPath) {
    if (source == null) {
      return null;
    }
    String[] segments = dottedPath.split("\\.");
    Object current = source;
    for (String segment : segments) {
      if (current instanceof Map) {
        current = ((Map<String, Object>) current).get(segment);
      } else {
        return null;
      }
    }
    return current;
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
