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
}
