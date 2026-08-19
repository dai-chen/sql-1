/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
  private final String indexName;

  /** Accumulated rows — one Object[] per matching document, in collect() order. */
  private final List<Object[]> rows = new ArrayList<>();

  /** Count of rows collected (equals rows.size() — US-009 will enforce rowBudget). */
  private long rowsCollected;

  public CalciteExecAggregator(
      String name,
      String plan,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      CombineDescriptor combine,
      int rowBudget,
      SearchContext searchContext,
      Aggregator parent,
      Map<String, Object> metadata)
      throws IOException {
    super(name, searchContext, parent, metadata);
    this.plan = plan;
    this.fields = fields;
    this.combine = combine;
    this.rowBudget = rowBudget;
    this.indexName = searchContext.indexShard().shardId().getIndexName();
  }

  @Override
  protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub)
      throws IOException {
    SourceLookup sourceLookup =
        context.getQueryShardContext().lookup().getLeafSearchLookup(ctx).source();
    ShardRowReader rowReader = ShardRowReader.create(ctx, fields, sourceLookup);

    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        // Position _source on the current document before reading
        sourceLookup.setSegmentAndDocument(ctx, doc);
        Object[] row = rowReader.readRow(doc);
        rows.add(row);
        rowsCollected++;
        // TODO US-009: enforce rowBudget
      }
    };
  }

  @Override
  public InternalAggregation buildAggregation(long owningBucketOrd) {
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

    // 2. Phase 1: HEP pass to expand windows (ProjectToWindowRule converts RexOver in a
    //    LogicalProject into a LogicalWindow node that the Volcano phase can handle) and
    //    merge Filter/Project into Calc (EnumerableCalc supports ARRAY mode; separate
    //    EnumerableProject/EnumerableFilter do not).
    HepProgramBuilder hepBuilder = new HepProgramBuilder();
    hepBuilder.addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
    hepBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
    hepBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    hepBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    HepPlanner hepPlanner = new HepPlanner(hepBuilder.build());
    hepPlanner.setRoot(logicalRel);
    RelNode expandedRel = hepPlanner.findBestExp();

    // 3. Phase 2: VolcanoPlanner converts the expanded logical plan to EnumerableConvention.
    //    ConventionTraitDef and RelCollationTraitDef are already registered by
    //    RelFragmentCodec.deserialize(). Add the standard enumerable implementation rules.
    RelOptCluster cluster = expandedRel.getCluster();
    RelOptPlanner planner = cluster.getPlanner();
    for (var rule : EnumerableRules.rules()) {
      planner.addRule(rule);
    }

    // Request conversion to EnumerableConvention
    RelTraitSet desiredTraits = expandedRel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    planner.setRoot(planner.changeTraits(expandedRel, desiredTraits));
    RelNode bestPlan = planner.findBestExp();

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

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalCalciteExec(name, combine, 0L, 0L, List.of(), metadata());
  }
}
