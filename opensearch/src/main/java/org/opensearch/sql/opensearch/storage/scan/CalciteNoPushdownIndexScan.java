/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.opensearch.planner.rules.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;

/**
 * A variant of {@link CalciteLogicalIndexScan} that registers only non-pushdown rules plus a custom
 * enumerable conversion rule that converts date string values to epoch values. Used by the ANSI SQL
 * schema to let Calcite handle all planning with standard operators.
 */
public class CalciteNoPushdownIndexScan extends CalciteLogicalIndexScan {

  public CalciteNoPushdownIndexScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchIndex osIndex) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        ImmutableList.of(),
        table,
        osIndex,
        table.getRowType(),
        new PushDownContext(osIndex));
  }

  @Override
  public void register(RelOptPlanner planner) {
    // Register non-pushdown rules but replace INDEX_SCAN_RULE with our date-converting variant
    for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_NON_PUSHDOWN_RULES) {
      planner.addRule(rule);
    }
    // Remove the default EnumerableIndexScanRule that would create a non-date-converting scan
    planner.removeRule(OpenSearchIndexRules.OPEN_SEARCH_NON_PUSHDOWN_RULES.getFirst());
    planner.addRule(DateConvertingEnumerableScanRule.CONFIG.toRule());
    // Skip ALL pushdown rules — they NPE because the schema excludes metadata fields.
  }

  /** Enumerable scan that converts date string values to epoch values for standard SQL types. */
  public static class DateConvertingEnumerableScan extends AbstractCalciteIndexScan
      implements Scannable, EnumerableRel {

    private final int[] dateFieldIndices;
    private final int[] timestampFieldIndices;
    private final int[] timeFieldIndices;

    public DateConvertingEnumerableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        OpenSearchIndex osIndex,
        RelDataType schema,
        PushDownContext pushDownContext) {
      super(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
      // Pre-compute which field indices are date/timestamp/time
      List<RelDataTypeField> fields = schema.getFieldList();
      dateFieldIndices =
          java.util.stream.IntStream.range(0, fields.size())
              .filter(i -> fields.get(i).getType().getSqlTypeName() == SqlTypeName.DATE)
              .toArray();
      timestampFieldIndices =
          java.util.stream.IntStream.range(0, fields.size())
              .filter(i -> fields.get(i).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP)
              .toArray();
      timeFieldIndices =
          java.util.stream.IntStream.range(0, fields.size())
              .filter(i -> fields.get(i).getType().getSqlTypeName() == SqlTypeName.TIME)
              .toArray();
    }

    @Override
    protected AbstractCalciteIndexScan buildScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        OpenSearchIndex osIndex,
        RelDataType schema,
        PushDownContext pushDownContext) {
      return new DateConvertingEnumerableScan(
          cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
    }

    @Override
    public DateConvertingEnumerableScan copy() {
      return new DateConvertingEnumerableScan(
          getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      PhysType physType =
          PhysTypeImpl.of(
              implementor.getTypeFactory(),
              OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), getRowType()),
              pref.preferArray());
      Expression scanOperator =
          implementor.stash(this, DateConvertingEnumerableScan.class);
      return implementor.result(
          physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
      return new AbstractEnumerable<>() {
        @Override
        public Enumerator<Object> enumerator() {
          var requestBuilder = pushDownContext.createRequestBuilder();
          Enumerator<Object> base =
              new OpenSearchIndexEnumerator(
                  osIndex.getClient(),
                  getRowType().getFieldNames(),
                  requestBuilder.getMaxResponseSize(),
                  requestBuilder.getMaxResultWindow(),
                  osIndex.getQueryBucketSize(),
                  osIndex.buildRequest(requestBuilder),
                  osIndex.createOpenSearchResourceMonitor());
          return new DateConvertingEnumerator(
              base,
              getRowType().getFieldCount(),
              dateFieldIndices,
              timestampFieldIndices,
              timeFieldIndices);
        }
      };
    }

    @Override
    public void register(RelOptPlanner planner) {
      // No additional rules needed for the enumerable scan
    }
  }

  /** Enumerator wrapper that converts date string values to epoch integers/longs. */
  static class DateConvertingEnumerator implements Enumerator<Object> {
    private final Enumerator<Object> delegate;
    private final int fieldCount;
    private final int[] dateIndices;
    private final int[] timestampIndices;
    private final int[] timeIndices;

    DateConvertingEnumerator(
        Enumerator<Object> delegate,
        int fieldCount,
        int[] dateIndices,
        int[] timestampIndices,
        int[] timeIndices) {
      this.delegate = delegate;
      this.fieldCount = fieldCount;
      this.dateIndices = dateIndices;
      this.timestampIndices = timestampIndices;
      this.timeIndices = timeIndices;
    }

    @Override
    public Object current() {
      Object row = delegate.current();
      if (fieldCount == 1) {
        // Single column — scalar value
        return convertValue(row, 0);
      }
      Object[] arr = (Object[]) row;
      for (int idx : dateIndices) {
        if (idx < arr.length && arr[idx] instanceof String s) {
          arr[idx] = dateStringToEpochDays(s);
        }
      }
      for (int idx : timestampIndices) {
        if (idx < arr.length && arr[idx] instanceof String s) {
          arr[idx] = timestampStringToEpochMillis(s);
        }
      }
      for (int idx : timeIndices) {
        if (idx < arr.length && arr[idx] instanceof String s) {
          arr[idx] = timeStringToMillisOfDay(s);
        }
      }
      return arr;
    }

    private Object convertValue(Object val, int idx) {
      if (val instanceof String s) {
        for (int di : dateIndices) {
          if (di == idx) return dateStringToEpochDays(s);
        }
        for (int ti : timestampIndices) {
          if (ti == idx) return timestampStringToEpochMillis(s);
        }
        for (int ti : timeIndices) {
          if (ti == idx) return timeStringToMillisOfDay(s);
        }
      }
      return val;
    }

    static int dateStringToEpochDays(String s) {
      try {
        return (int) LocalDate.parse(s.substring(0, Math.min(s.length(), 10))).toEpochDay();
      } catch (DateTimeParseException e) {
        try {
          return (int)
              (Instant.parse(s).atZone(ZoneOffset.UTC).toLocalDate().toEpochDay());
        } catch (DateTimeParseException e2) {
          return (int) (Long.parseLong(s) / 86400000L);
        }
      }
    }

    static long timestampStringToEpochMillis(String s) {
      try {
        return Instant.parse(s).toEpochMilli();
      } catch (DateTimeParseException e) {
        try {
          TemporalAccessor ta =
              new DateTimeFormatterBuilder()
                  .appendPattern("yyyy-MM-dd['T'][ ]HH:mm:ss")
                  .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                  .toFormatter()
                  .parseBest(
                      s,
                      Instant::from,
                      java.time.LocalDateTime::from);
          if (ta instanceof Instant inst) return inst.toEpochMilli();
          return ((java.time.LocalDateTime) ta).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException e2) {
          try {
            // Date-only string like "1990-05-15"
            return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
          } catch (DateTimeParseException e3) {
            return Long.parseLong(s);
          }
        }
      }
    }

    static int timeStringToMillisOfDay(String s) {
      try {
        return (int) (LocalTime.parse(s).toNanoOfDay() / 1_000_000);
      } catch (DateTimeParseException e) {
        return Integer.parseInt(s);
      }
    }

    @Override
    public boolean moveNext() {
      return delegate.moveNext();
    }

    @Override
    public void reset() {
      delegate.reset();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  /** Rule that converts CalciteNoPushdownIndexScan to DateConvertingEnumerableScan. */
  public static class DateConvertingEnumerableScanRule extends ConverterRule {
    static final Config CONFIG =
        Config.INSTANCE
            .as(Config.class)
            .withConversion(
                CalciteNoPushdownIndexScan.class,
                s -> s.getOsIndex() != null,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                "DateConvertingEnumerableScanRule")
            .withRuleFactory(DateConvertingEnumerableScanRule::new);

    protected DateConvertingEnumerableScanRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final CalciteNoPushdownIndexScan scan = (CalciteNoPushdownIndexScan) rel;
      return new DateConvertingEnumerableScan(
          scan.getCluster(),
          scan.getTraitSet().plus(EnumerableConvention.INSTANCE),
          scan.getHints(),
          scan.getTable(),
          scan.getOsIndex(),
          scan.getSchema(),
          scan.getPushDownContext());
    }
  }
}
