/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * Single-value (non-bucket) aggregator for the staged Calcite execution. US-004: materializes one
 * row per matching document using per-type doc_values readers with a _source fallback, and reports
 * rowsCollected/rowsEmitted via {@link InternalCalciteExec}.
 */
public class CalciteExecAggregator extends MetricsAggregator {

  private final String plan;
  private final List<CalciteExecAggregationBuilder.FieldDescriptor> fields;
  private final CombineDescriptor combine;
  private final int rowBudget;
  private final String forcingOperator;
  private final String indexName;

  /**
   * US-013: optional early termination limit. Non-null only when limit pushdown (rule 4) fires AND
   * the fragment is cardinality-preserving. When set, the shard may stop collecting documents at
   * this count — safe because no Filter/Aggregate/Window can reduce output below N.
   */
  private final Integer earlyTerminationLimit;

  /** US-013: true once earlyTerminationLimit has been reached and collection should stop. */
  private boolean terminated;

  /**
   * Per-field nested path: field name → nested parent path, null for non-nested fields. Resolved
   * once from the shard's mapping in the constructor and passed into ShardRowReader per leaf.
   */
  private final Map<String, String> nestedPaths;

  /** True when at least one requested field lives under a nested path. */
  private final boolean hasNestedFields;

  /** Accumulated rows — one or more Object[] per matching document, in collect() order. */
  private final List<Object[]> rows = new ArrayList<>();

  /** Count of rows collected (post-expansion for nested; US-009 enforces rowBudget). */
  private long rowsCollected;

  /**
   * Circuit breaker: accumulated bytes not yet charged. Flushed to the request breaker every {@link
   * #BREAKER_PROBE_CADENCE} rows, mirroring OpenSearch's MultiBucketConsumer pattern.
   */
  private long unchargedBytes;

  /** How often to flush accumulated bytes to the circuit breaker (every N rows). */
  private static final int BREAKER_PROBE_CADENCE = 1024;

  public CalciteExecAggregator(
      String name,
      String plan,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      CombineDescriptor combine,
      int rowBudget,
      String forcingOperator,
      Integer earlyTerminationLimit,
      SearchContext searchContext,
      Aggregator parent,
      Map<String, Object> metadata)
      throws IOException {
    super(name, searchContext, parent, metadata);
    this.plan = plan;
    this.fields = fields;
    this.combine = combine;
    this.rowBudget = rowBudget;
    this.forcingOperator = forcingOperator;
    this.earlyTerminationLimit = earlyTerminationLimit;
    this.terminated = false;
    this.indexName = searchContext.indexShard().shardId().getIndexName();
    this.nestedPaths = resolveNestedPaths(fields, searchContext);
    this.hasNestedFields = nestedPaths.values().stream().anyMatch(java.util.Objects::nonNull);
  }

  /**
   * Resolves the nested parent path for each field by walking the field's dotted parent segments
   * and checking the shard's mapping for a nested ObjectMapper.
   */
  private static Map<String, String> resolveNestedPaths(
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields, SearchContext searchContext) {
    Map<String, String> paths = new LinkedHashMap<>();
    for (CalciteExecAggregationBuilder.FieldDescriptor fd : fields) {
      paths.put(fd.getName(), findNestedPath(fd.getName(), searchContext));
    }
    return paths;
  }

  /**
   * Walks the dotted parent segments of a field name and returns the innermost nested path, or null
   * if the field is not under any nested object.
   */
  private static String findNestedPath(String fieldName, SearchContext searchContext) {
    // Walk from deepest parent to shallowest, return the innermost nested
    String current = fieldName;
    String nestedPath = null;
    int lastDot = current.lastIndexOf('.');
    while (lastDot > 0) {
      String parent = current.substring(0, lastDot);
      org.opensearch.index.mapper.ObjectMapper om =
          searchContext.getQueryShardContext().getObjectMapper(parent);
      if (om != null && om.nested().isNested()) {
        nestedPath = parent;
        // Walking from the leaf upward, the first nested ObjectMapper we hit IS the innermost
        // (closest to the leaf) nested path — no need to continue.
        break;
      }
      lastDot = parent.lastIndexOf('.');
      current = parent;
    }
    return nestedPath;
  }

  @Override
  protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub)
      throws IOException {
    // US-013: if we already terminated, throw immediately before building any ShardRowReader.
    // This second guard is load-bearing: CollectionTerminatedException is caught per-leaf by
    // ContextIndexSearcher#searchLeaf() and the leaf loop otherwise continues, so the exception
    // ALONE does not stop a shard — this guard prevents subsequent leaves from being opened.
    if (terminated) {
      throw new CollectionTerminatedException();
    }

    SourceLookup sourceLookup =
        context.getQueryShardContext().lookup().getLeafSearchLookup(ctx).source();
    ShardRowReader rowReader = ShardRowReader.create(ctx, fields, sourceLookup, nestedPaths);

    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        // Position _source on the current document before reading
        sourceLookup.setSegmentAndDocument(ctx, doc);
        if (hasNestedFields) {
          List<Object[]> expanded = rowReader.readRows(doc);
          rows.addAll(expanded);
          rowsCollected += expanded.size();
          for (Object[] row : expanded) {
            chargeRowBytes(row);
          }
        } else {
          Object[] row = rowReader.readRow(doc);
          rows.add(row);
          rowsCollected++;
          chargeRowBytes(row);
        }
        // US-013: check early termination BEFORE the rowBudget check. A legitimately bounded
        // query must never fail the safety budget — checking early termination first is deliberate.
        if (earlyTerminationLimit != null && rowsCollected >= earlyTerminationLimit) {
          flushBreakerBytes();
          terminated = true;
          throw new CollectionTerminatedException();
        }
        // US-009: enforce rowBudget as a HARD exception. Do NOT use
        // CollectionTerminatedException here — that is per-leaf only, caught by
        // ContextIndexSearcher#searchLeaf(), and would silently truncate rather than fail
        // the query. CollectionTerminatedException is reserved for US-013's legitimately
        // pushable limit (rule 4) which is a different semantic. ↓ This throw enforces it.
        if (rowsCollected > rowBudget) {
          // Flush any remaining uncharged bytes before throwing so the breaker state is accurate
          flushBreakerBytes();
          throw new RowBudgetExceededException(rowsCollected, rowBudget, forcingOperator);
        }
      }
    };
  }

  /**
   * Accumulates the estimated byte cost of a buffered row and flushes to the circuit breaker every
   * {@link #BREAKER_PROBE_CADENCE} rows.
   */
  private void chargeRowBytes(Object[] row) {
    unchargedBytes += estimateRowBytes(row);
    if (rowsCollected % BREAKER_PROBE_CADENCE == 0) {
      flushBreakerBytes();
    }
  }

  /** Flushes accumulated uncharged bytes to the request circuit breaker. */
  private void flushBreakerBytes() {
    if (unchargedBytes > 0) {
      addRequestCircuitBreakerBytes(unchargedBytes);
      unchargedBytes = 0;
    }
  }

  /**
   * Estimates the heap footprint of a single buffered row (Object[]). This is an ESTIMATE, not a
   * precise measurement — it approximates Object overhead and String character storage. Used to
   * charge the request circuit breaker so the cluster sees the query's memory footprint.
   *
   * <p>Sizing heuristic: array header (16 bytes) + per-element estimate based on type.
   */
  static long estimateRowBytes(Object[] row) {
    // Object[] header: 16 bytes (mark word + klass pointer + length)
    long bytes = 16L;
    for (Object val : row) {
      if (val == null) {
        // null reference slot in the array
        continue;
      } else if (val instanceof String s) {
        // String: object header (16) + char[] header (16) + 2 bytes per char + hash field (4)
        // Approximation: 40 + 2 * length
        bytes += 40L + 2L * s.length();
      } else if (val instanceof Long || val instanceof Double) {
        // Boxed 64-bit: object header (16)
        bytes += 16L;
      } else if (val instanceof Integer || val instanceof Float) {
        // Boxed 32-bit: object header (16)
        bytes += 16L;
      } else if (val instanceof Boolean) {
        // Boxed boolean: object header (16)
        bytes += 16L;
      } else {
        // Unknown type: conservative estimate
        bytes += 16L;
      }
    }
    return bytes;
  }

  @Override
  public InternalAggregation buildAggregation(long owningBucketOrd) {
    // Flush any remaining uncharged bytes before executing the fragment
    flushBreakerBytes();

    // Compile the fragment to a Bindable and execute it, wrapped in the Calcite classloader
    // helper to ensure Janino can resolve plugin classes (CALCITE-3745 workaround).
    List<List<Object>> outputRows =
        CalciteClassLoaderHelper.withCalciteClassLoader(
            () -> executeFragment(plan, fields, rows, indexName), CalciteExecAggregator.class);

    long rowsEmitted = outputRows.size();
    return new InternalCalciteExec(
        name, combine, rowsCollected, rowsEmitted, outputRows, metadata());
  }

  /**
   * Deserializes the fragment, converts it to EnumerableConvention via a two-phase approach (HEP
   * rewrite for window expansion, then VolcanoPlanner with enumerable rules), compiles to a Janino
   * Bindable, and drains the result over the buffered shard rows.
   */
  static List<List<Object>> executeFragment(
      String base64Plan,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      List<Object[]> bufferedRows,
      String indexName) {

    // 1. Deserialize the logical RelNode from the base64 plan
    RelNode logicalRel = RelFragmentCodec.deserialize(base64Plan, indexName, fields);

    // 2-3. Optimize to EnumerableConvention (HEP rewrite, then Volcano)
    RelNode bestPlan = optimizeFragment(logicalRel);

    // 4. Compile the EnumerableRel to a Bindable using Janino.
    //    SparkHandler is null — that is precisely the code path that triggers Janino compilation
    //    rather than Spark execution (EnumerableInterpretable JavaDoc).
    @SuppressWarnings("unchecked")
    Bindable<Object[]> bindable =
        (Bindable<Object[]>)
            EnumerableInterpretable.toBindable(
                ImmutableMap.of(), null, (EnumerableRel) bestPlan, EnumerableRel.Prefer.ARRAY);

    // 5. Bind with a DataContext carrying the buffered rows under the stash key that
    //    ShardRowSourceTable.scan() reads. The DataContext also needs the root schema because
    //    EnumerableTableScan's generated code navigates root.getRootSchema() to locate the table.
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    // Register the table with the row type matching the deserialized schema (built from fields).
    // EnumerableTableScan's generated code finds it here and calls scan(DataContext).
    RelDataTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
    RelDataTypeFactory.Builder rtBuilder = typeFactory.builder();
    for (CalciteExecAggregationBuilder.FieldDescriptor fd : fields) {
      SqlTypeName sqlType = RelFragmentCodec.osTypeToSqlType(fd.getType());
      rtBuilder.add(
          fd.getName(),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true));
    }
    RelDataType tableRowType = rtBuilder.build();
    osSchema.add(indexName, new RelFragmentCodec.ShardRowSourceTable(tableRowType));

    // Build a DataContext that exposes the schema and the stash slot
    DataContext dataContext =
        new DataContext() {
          @Override
          public SchemaPlus getRootSchema() {
            return rootSchema;
          }

          @Override
          public JavaTypeFactory getTypeFactory() {
            return OpenSearchTypeFactory.TYPE_FACTORY;
          }

          @Override
          public QueryProvider getQueryProvider() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object get(String name) {
            if (RelFragmentCodec.SHARD_ROWS_STASH_KEY.equals(name)) {
              return bufferedRows;
            }
            // Temporal functions (MONTH, YEAR, etc.) require UTC_TIMESTAMP in the DataContext.
            // Calcite reads it via DataContext.Variable.UTC_TIMESTAMP.get(dataContext).
            if (Variable.UTC_TIMESTAMP.camelName.equals(name)) {
              return System.currentTimeMillis();
            }
            return null;
          }
        };

    // 6. Drain the Enumerable into a List<List<Object>> for the InternalCalciteExec wire format.
    //    Note: when the plan output has a single column, Calcite's ARRAY mode may return the
    //    scalar value directly rather than an Object[]. Handle both cases.
    Enumerable<Object[]> enumerable = bindable.bind(dataContext);
    int outputColumnCount = bestPlan.getRowType().getFieldCount();
    List<List<Object>> result = new ArrayList<>();
    try (var enumerator = enumerable.enumerator()) {
      while (enumerator.moveNext()) {
        Object current = enumerator.current();
        if (outputColumnCount == 1 && !(current instanceof Object[])) {
          // Single-column result returned as a scalar
          result.add(List.of(current));
        } else {
          Object[] row = (Object[]) current;
          result.add(Arrays.asList(row.clone()));
        }
      }
    }
    return result;
  }

  /**
   * Converts a deserialized logical fragment to EnumerableConvention in two phases. Package-private
   * so tests can assert the physical shape (e.g. that an ordered Sort with a fetch becomes an
   * {@code EnumerableLimitSort} rather than a full sort followed by a separate limit).
   *
   * <p>Phase 1 is a HEP pass that expands windows (ProjectToWindowRule converts a RexOver inside a
   * LogicalProject into a LogicalWindow the Volcano phase can handle) and merges Filter/Project
   * into Calc (EnumerableCalc supports ARRAY mode; separate EnumerableProject/EnumerableFilter do
   * not).
   *
   * <p>Phase 2 runs the VolcanoPlanner with the standard enumerable rules. ConventionTraitDef and
   * RelCollationTraitDef are already registered by {@link RelFragmentCodec#deserialize}.
   */
  static RelNode optimizeFragment(RelNode logicalRel) {
    HepProgramBuilder hepBuilder = new HepProgramBuilder();
    hepBuilder.addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
    hepBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
    hepBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    hepBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    HepPlanner hepPlanner = new HepPlanner(hepBuilder.build());
    hepPlanner.setRoot(logicalRel);
    RelNode expandedRel = hepPlanner.findBestExp();

    RelOptCluster cluster = expandedRel.getCluster();
    RelOptPlanner planner = cluster.getPlanner();
    for (var rule : EnumerableRules.rules()) {
      planner.addRule(rule);
    }
    // US-014: fuse an ordered Sort carrying a fetch into a single EnumerableLimitSort instead of
    // an EnumerableLimit stacked on a full EnumerableSort. NOT part of EnumerableRules.rules() in
    // Calcite 1.42.0 — registered explicitly, and asserted by FragmentExecutionTest. Note this is
    // a plan-shape improvement only: linq4j 1.42.0 contains no PriorityQueue anywhere, so
    // EnumerableLimitSort still sorts its whole input and then takes n.
    planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

    RelTraitSet desiredTraits = expandedRel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    planner.setRoot(planner.changeTraits(expandedRel, desiredTraits));
    return planner.findBestExp();
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalCalciteExec(name, combine, 0L, 0L, List.of(), metadata());
  }
}
