/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.opensearch.stage.CalciteExecAggregationBuilder.FieldDescriptor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class ShardRowReaderTest {

  @Test
  void keyword_field_reads_from_sorted_set_doc_values() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    SortedSetDocValues dv = mock(SortedSetDocValues.class);
    when(leafReader.getSortedSetDocValues("city")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextOrd()).thenReturn(5L);
    when(dv.lookupOrd(5L)).thenReturn(new BytesRef("Seattle"));

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("city", "keyword"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(String.class, row[0].getClass());
    assertEquals("Seattle", row[0]);
  }

  @Test
  void long_field_reads_from_sorted_numeric_doc_values() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("balance")).thenReturn(dv);
    when(dv.advanceExact(3)).thenReturn(true);
    when(dv.nextValue()).thenReturn(39225L);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("balance", "long"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(3);

    assertEquals(Long.class, row[0].getClass());
    assertEquals(39225L, row[0]);
  }

  @Test
  void integer_field_reads_from_sorted_numeric_doc_values() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("age")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextValue()).thenReturn(32L);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("age", "integer"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Integer.class, row[0].getClass());
    assertEquals(Integer.valueOf(32), row[0]);
  }

  @Test
  void double_field_decodes_via_numeric_utils() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    long encoded = NumericUtils.doubleToSortableLong(3.14);
    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("rate")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextValue()).thenReturn(encoded);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("rate", "double"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Double.class, row[0].getClass());
    assertEquals(3.14, (Double) row[0], 0.0001);
  }

  @Test
  void float_field_decodes_via_numeric_utils() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    // OpenSearch stores float doc_values as floatToSortableInt widened to long
    long encoded = (long) NumericUtils.floatToSortableInt(2.5f);
    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("score")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextValue()).thenReturn(encoded);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("score", "float"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Float.class, row[0].getClass());
    assertEquals(2.5f, (Float) row[0], 0.0001f);
  }

  @Test
  void boolean_field_returns_true_for_nonzero() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("male")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextValue()).thenReturn(1L);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("male", "boolean"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Boolean.class, row[0].getClass());
    assertEquals(true, row[0]);
  }

  @Test
  void boolean_field_returns_false_for_zero() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("male")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextValue()).thenReturn(0L);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("male", "boolean"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(false, row[0]);
  }

  @Test
  void date_field_reads_epoch_millis() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    long epochMillis = 1508716800000L;
    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("birthdate")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextValue()).thenReturn(epochMillis);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("birthdate", "date"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    // Date fields return Long epoch millis — Calcite's TIMESTAMP type uses Long internally.
    // PPL temporal UDFs receive the Long; ExprValueUtils.fromObjectValue(Long, TIMESTAMP)
    // converts it to ExprTimestampValue via Instant.ofEpochMilli.
    assertEquals(Long.class, row[0].getClass());
    assertEquals(epochMillis, row[0]);
  }

  @Test
  void text_field_always_reads_from_source() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);
    when(sourceLookup.extractValue("address", null)).thenReturn("880 Holmes Lane");

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("address", "text"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals("880 Holmes Lane", row[0]);
  }

  @Test
  void keyword_field_falls_back_to_source_when_doc_values_absent() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    // doc_values are null for this field (runtime absence)
    when(leafReader.getSortedSetDocValues("gender")).thenReturn(null);
    SourceLookup sourceLookup = mock(SourceLookup.class);
    when(sourceLookup.extractValue("gender", null)).thenReturn("M");

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("gender", "keyword"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals("M", row[0]);
  }

  @Test
  void absent_doc_value_produces_null_not_exception() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("balance")).thenReturn(dv);
    // advanceExact returns false — value is absent for this doc
    when(dv.advanceExact(5)).thenReturn(false);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("balance", "long"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(5);

    assertNull(row[0]);
  }

  @Test
  void unknown_type_throws_illegal_argument_exception() {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("some_field", "geo_shape"));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> ShardRowReader.create(ctx, fields, sourceLookup));

    assertEquals(
        "Unrecognized OpenSearch field type 'geo_shape' for field 'some_field'", ex.getMessage());
  }

  @Test
  void multiple_fields_produce_correct_row_with_mixed_types() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    // keyword via doc_values
    SortedSetDocValues cityDv = mock(SortedSetDocValues.class);
    when(leafReader.getSortedSetDocValues("city")).thenReturn(cityDv);
    when(cityDv.advanceExact(0)).thenReturn(true);
    when(cityDv.nextOrd()).thenReturn(1L);
    when(cityDv.lookupOrd(1L)).thenReturn(new BytesRef("Brogan"));

    // long via doc_values
    SortedNumericDocValues balDv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("balance")).thenReturn(balDv);
    when(balDv.advanceExact(0)).thenReturn(true);
    when(balDv.nextValue()).thenReturn(39225L);

    // text via _source
    when(sourceLookup.extractValue("address", null)).thenReturn("880 Holmes Lane");

    List<FieldDescriptor> fields =
        List.of(
            new FieldDescriptor("city", "keyword"),
            new FieldDescriptor("balance", "long"),
            new FieldDescriptor("address", "text"));

    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals("Brogan", row[0]);
    assertEquals(39225L, row[1]);
    assertEquals("880 Holmes Lane", row[2]);
  }

  @Test
  void numeric_long_falls_back_to_source_when_doc_values_absent() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    when(leafReader.getSortedNumericDocValues("balance")).thenReturn(null);
    SourceLookup sourceLookup = mock(SourceLookup.class);
    when(sourceLookup.extractValue("balance", null)).thenReturn(39225);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("balance", "long"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Long.class, row[0].getClass());
    assertEquals(39225L, row[0]);
  }

  @Test
  void integer_field_falls_back_to_source_with_correct_type() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    when(leafReader.getSortedNumericDocValues("age")).thenReturn(null);
    SourceLookup sourceLookup = mock(SourceLookup.class);
    when(sourceLookup.extractValue("age", null)).thenReturn(32);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("age", "integer"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Integer.class, row[0].getClass());
    assertEquals(Integer.valueOf(32), row[0]);
  }

  @Test
  void float_field_falls_back_to_source_with_correct_type() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    when(leafReader.getSortedNumericDocValues("score")).thenReturn(null);
    SourceLookup sourceLookup = mock(SourceLookup.class);
    when(sourceLookup.extractValue("score", null)).thenReturn(2.5);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("score", "float"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Float.class, row[0].getClass());
    assertEquals(2.5f, (Float) row[0], 0.0001f);
  }

  @Test
  void double_field_falls_back_to_source_with_correct_type() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    when(leafReader.getSortedNumericDocValues("rate")).thenReturn(null);
    SourceLookup sourceLookup = mock(SourceLookup.class);
    when(sourceLookup.extractValue("rate", null)).thenReturn(3.14);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("rate", "double"));
    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup);
    Object[] row = reader.readRow(0);

    assertEquals(Double.class, row[0].getClass());
    assertEquals(3.14, (Double) row[0], 0.0001);
  }

  @Test
  void io_exception_from_doc_values_propagates() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    when(leafReader.getSortedNumericDocValues("balance")).thenThrow(new IOException("disk error"));
    SourceLookup sourceLookup = mock(SourceLookup.class);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("balance", "long"));

    assertThrows(IOException.class, () -> ShardRowReader.create(ctx, fields, sourceLookup));
  }

  @Test
  void io_exception_from_sorted_set_doc_values_propagates() throws IOException {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    when(leafReader.getSortedSetDocValues("city")).thenThrow(new IOException("segment corrupted"));
    SourceLookup sourceLookup = mock(SourceLookup.class);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("city", "keyword"));

    assertThrows(IOException.class, () -> ShardRowReader.create(ctx, fields, sourceLookup));
  }

  // ===================== NESTED FIELD EXPANSION TESTS =====================

  @Test
  void nested_sibling_alignment_two_fields_same_path() throws IOException {
    // Two fields under the same nested path "address": address.city and address.state.
    // A document with 3 sub-documents should produce 3 output rows, each pairing city+state
    // from the SAME sub-document (sibling alignment, NOT cross-product).
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    // _source for the parent document with 3 nested sub-docs under "address"
    Map<String, Object> source =
        Map.of(
            "name",
            "abbas",
            "address",
            List.of(
                Map.of("city", "New york city", "state", "NY"),
                Map.of("city", "bellevue", "state", "WA"),
                Map.of("city", "seattle", "state", "WA")));
    when(sourceLookup.source()).thenReturn(source);

    // name is a non-nested field read via _source (text type)
    when(sourceLookup.extractValue("name", null)).thenReturn("abbas");

    List<FieldDescriptor> fields =
        List.of(
            new FieldDescriptor("name", "text"),
            new FieldDescriptor("address.city", "text"),
            new FieldDescriptor("address.state", "text"));

    java.util.HashMap<String, String> paths = new java.util.HashMap<>();
    paths.put("name", null);
    paths.put("address.city", "address");
    paths.put("address.state", "address");

    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup, paths);
    List<Object[]> rows = reader.readRows(0);

    // Should produce 3 rows with sibling alignment
    assertEquals(3, rows.size());
    // Row 0: name repeated, city/state from sub-doc 0
    assertEquals("abbas", rows.get(0)[0]);
    assertEquals("New york city", rows.get(0)[1]);
    assertEquals("NY", rows.get(0)[2]);
    // Row 1: name repeated, city/state from sub-doc 1
    assertEquals("abbas", rows.get(1)[0]);
    assertEquals("bellevue", rows.get(1)[1]);
    assertEquals("WA", rows.get(1)[2]);
    // Row 2: name repeated, city/state from sub-doc 2
    assertEquals("abbas", rows.get(2)[0]);
    assertEquals("seattle", rows.get(2)[1]);
    assertEquals("WA", rows.get(2)[2]);
  }

  @Test
  void non_nested_fields_only_produces_single_row() throws IOException {
    // When no nested paths are present, readRows returns exactly one row per doc.
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
    when(leafReader.getSortedNumericDocValues("age")).thenReturn(dv);
    when(dv.advanceExact(0)).thenReturn(true);
    when(dv.nextValue()).thenReturn(25L);

    when(sourceLookup.extractValue("name", null)).thenReturn("alice");

    List<FieldDescriptor> fields =
        List.of(new FieldDescriptor("name", "text"), new FieldDescriptor("age", "long"));

    // All null nested paths
    java.util.HashMap<String, String> paths = new java.util.HashMap<>();
    paths.put("name", null);
    paths.put("age", null);

    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup, paths);
    List<Object[]> rows = reader.readRows(0);

    assertEquals(1, rows.size());
    assertEquals("alice", rows.get(0)[0]);
    assertEquals(25L, rows.get(0)[1]);
  }

  @Test
  void nested_single_sub_document_produces_single_row() throws IOException {
    // A nested field with only one sub-document should produce exactly one row.
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    Map<String, Object> source =
        Map.of("address", List.of(Map.of("city", "houston", "state", "TX")));
    when(sourceLookup.source()).thenReturn(source);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("address.city", "text"));

    java.util.HashMap<String, String> paths = new java.util.HashMap<>();
    paths.put("address.city", "address");

    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup, paths);
    List<Object[]> rows = reader.readRows(0);

    assertEquals(1, rows.size());
    assertEquals("houston", rows.get(0)[0]);
  }

  @Test
  void nested_path_absent_from_source_produces_one_row_with_null_nested_values()
      throws IOException {
    // Case (a): nested array is genuinely absent from _source (sparse parent document).
    // Should produce exactly one row with null for the nested fields.
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    // _source has no "address" key at all
    Map<String, Object> source = Map.of("name", "sparse_doc");
    when(sourceLookup.source()).thenReturn(source);
    when(sourceLookup.extractValue("name", null)).thenReturn("sparse_doc");

    List<FieldDescriptor> fields =
        List.of(new FieldDescriptor("name", "text"), new FieldDescriptor("address.city", "text"));

    java.util.HashMap<String, String> paths = new java.util.HashMap<>();
    paths.put("name", null);
    paths.put("address.city", "address");

    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup, paths);
    List<Object[]> rows = reader.readRows(0);

    assertEquals(1, rows.size());
    assertEquals("sparse_doc", rows.get(0)[0]);
    assertNull(rows.get(0)[1]);
  }

  @Test
  void nested_path_with_unexpected_shape_throws() throws IOException {
    // Case (b): nested path value is present but has an unexpected shape (e.g. a String where
    // a List or Map was expected). Must throw with a message naming the path and the type.
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    LeafReader leafReader = mock(LeafReader.class);
    when(ctx.reader()).thenReturn(leafReader);
    SourceLookup sourceLookup = mock(SourceLookup.class);

    // _source has "address" as a plain String instead of a List/Map of sub-documents
    Map<String, Object> source = Map.of("address", "not_a_nested_structure");
    when(sourceLookup.source()).thenReturn(source);

    List<FieldDescriptor> fields = List.of(new FieldDescriptor("address.city", "text"));

    java.util.HashMap<String, String> paths = new java.util.HashMap<>();
    paths.put("address.city", "address");

    ShardRowReader reader = ShardRowReader.create(ctx, fields, sourceLookup, paths);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> reader.readRows(0));

    // Message must contain the nested path, the field name, and the actual type
    assertEquals(
        "Nested path 'address' for field 'address.city' has unexpected _source shape:"
            + " java.lang.String",
        ex.getMessage());
  }
}
