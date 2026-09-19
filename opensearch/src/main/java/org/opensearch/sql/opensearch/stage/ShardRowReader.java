/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.data.model.ExprTimestampValue;

/** Reads individual shard-aggregation inputs from doc values, falling back to {@code _source}. */
final class ShardRowReader {

  private static final int MAX_DATE_CACHE_ENTRIES = 131_072;

  @FunctionalInterface
  interface FieldReader {
    Object read(int doc) throws IOException;
  }

  private final FieldReader[] readers;

  private ShardRowReader(FieldReader[] readers) {
    this.readers = readers;
  }

  static ShardRowReader create(
      LeafReaderContext context,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      BitSet requiredFields,
      SourceLookup source)
      throws IOException {
    FieldReader[] readers = new FieldReader[fields.size()];
    for (int fieldIndex = requiredFields.nextSetBit(0);
        fieldIndex >= 0;
        fieldIndex = requiredFields.nextSetBit(fieldIndex + 1)) {
      CalciteExecAggregationBuilder.FieldDescriptor field = fields.get(fieldIndex);
      readers[fieldIndex] =
          reader(context, field.name(), field.mappingType().toLowerCase(Locale.ROOT), source);
    }
    return new ShardRowReader(readers);
  }

  Object readField(int fieldIndex, int doc) throws IOException {
    FieldReader reader = readers[fieldIndex];
    if (reader == null) {
      throw new IllegalArgumentException(
          "Field " + fieldIndex + " is not required by the fragment");
    }
    return reader.read(doc);
  }

  private static FieldReader reader(
      LeafReaderContext context, String field, String type, SourceLookup source)
      throws IOException {
    return switch (type) {
      case "keyword" -> keywordReader(context, field, source);
      case "long" -> longReader(context, field, source);
      case "date" -> dateReader(context, field, source);
      case "byte" -> byteReader(context, field, source);
      case "short" -> shortReader(context, field, source);
      case "integer" -> integerReader(context, field, source);
      case "double" -> doubleReader(context, field, source);
      case "float" -> floatReader(context, field, source);
      case "half_float" -> sourceNumberReader(context, field, source, Number::floatValue);
      case "scaled_float" -> sourceNumberReader(context, field, source, Number::doubleValue);
      case "boolean" -> booleanReader(context, field, source);
      case "text", "object", "nested" -> sourceReader(context, field, source);
      default -> sourceReader(context, field, source);
    };
  }

  private static FieldReader keywordReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedSetDocValues values = leaf.getSortedSetDocValues(field);
    if (values == null) {
      return sourceReader(context, field, source);
    }
    String[] decoded =
        values.getValueCount() <= Integer.MAX_VALUE
            ? new String[(int) values.getValueCount()]
            : null;
    return doc -> {
      if (!values.advanceExact(doc)) {
        return null;
      }
      long ordinal = values.nextOrd();
      if (decoded == null) {
        return values.lookupOrd(ordinal).utf8ToString();
      }
      int index = (int) ordinal;
      String value = decoded[index];
      if (value == null) {
        value = values.lookupOrd(ordinal).utf8ToString();
        decoded[index] = value;
      }
      return value;
    };
  }

  private static FieldReader longReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      FieldReader fallback = sourceReader(context, field, source);
      return doc -> number(fallback.read(doc), Number::longValue);
    }
    return doc -> values.advanceExact(doc) ? values.nextValue() : null;
  }

  private static FieldReader dateReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      FieldReader fallback = sourceReader(context, field, source);
      return doc -> timestamp(fallback.read(doc));
    }
    Map<Long, Object> decoded = new HashMap<>();
    return doc -> {
      if (!values.advanceExact(doc)) {
        return null;
      }
      long epochMillis = values.nextValue();
      Object value = decoded.get(epochMillis);
      if (value == null) {
        value = timestamp(epochMillis);
        if (decoded.size() < MAX_DATE_CACHE_ENTRIES) {
          decoded.put(epochMillis, value);
        }
      }
      return value;
    };
  }

  private static FieldReader integerReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      FieldReader fallback = sourceReader(context, field, source);
      return doc -> number(fallback.read(doc), Number::intValue);
    }
    return doc -> values.advanceExact(doc) ? (int) values.nextValue() : null;
  }

  private static FieldReader byteReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      return sourceNumberReader(context, field, source, Number::byteValue);
    }
    return doc -> values.advanceExact(doc) ? (byte) values.nextValue() : null;
  }

  private static FieldReader shortReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      return sourceNumberReader(context, field, source, Number::shortValue);
    }
    return doc -> values.advanceExact(doc) ? (short) values.nextValue() : null;
  }

  private static FieldReader doubleReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      FieldReader fallback = sourceReader(context, field, source);
      return doc -> number(fallback.read(doc), Number::doubleValue);
    }
    return doc ->
        values.advanceExact(doc) ? NumericUtils.sortableLongToDouble(values.nextValue()) : null;
  }

  private static FieldReader floatReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      FieldReader fallback = sourceReader(context, field, source);
      return doc -> number(fallback.read(doc), Number::floatValue);
    }
    return doc ->
        values.advanceExact(doc) ? NumericUtils.sortableIntToFloat((int) values.nextValue()) : null;
  }

  private static FieldReader booleanReader(
      LeafReaderContext context, String field, SourceLookup source) throws IOException {
    LeafReader leaf = context.reader();
    SortedNumericDocValues values = leaf.getSortedNumericDocValues(field);
    if (values == null) {
      FieldReader fallback = sourceReader(context, field, source);
      return doc -> booleanValue(fallback.read(doc));
    }
    return doc -> values.advanceExact(doc) ? values.nextValue() != 0 : null;
  }

  private static FieldReader sourceReader(
      LeafReaderContext context, String field, SourceLookup source) {
    return doc -> {
      source.setSegmentAndDocument(context, doc);
      return source.extractValue(field, null);
    };
  }

  private static FieldReader sourceNumberReader(
      LeafReaderContext context,
      String field,
      SourceLookup source,
      java.util.function.Function<Number, Object> conversion) {
    FieldReader fallback = sourceReader(context, field, source);
    return doc -> number(fallback.read(doc), conversion);
  }

  static Object number(Object value, java.util.function.Function<Number, Object> conversion) {
    if (value instanceof Number number) {
      return conversion.apply(number);
    }
    if (value instanceof String string) {
      if (string.isBlank()) {
        return null;
      }
      try {
        return conversion.apply(new BigDecimal(string));
      } catch (NumberFormatException ignored) {
        return value;
      }
    }
    return value;
  }

  static Object timestamp(Object value) {
    if (value instanceof Number number) {
      return new ExprTimestampValue(Instant.ofEpochMilli(number.longValue())).value();
    }
    return value;
  }

  private static Object booleanValue(Object value) {
    if (value instanceof Number number) {
      return number.doubleValue() != 0;
    }
    if (value instanceof String string) {
      if (string.equalsIgnoreCase("true") || string.equals("1")) {
        return true;
      }
      if (string.equalsIgnoreCase("false") || string.equals("0")) {
        return false;
      }
    }
    return value;
  }
}
