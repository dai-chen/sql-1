/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * Executes a coordinator-side RelNode tree over gathered rows. This is the tier-2 executor in the
 * staged execution model: after {@code InternalCalciteExec.reduce()} combines shard results
 * (tier-1), this class compiles and runs the {@code coordinatorTree} through Calcite's Enumerable
 * convention over the gathered rows.
 *
 * <p>The coordinator tree is POST-OPTIMIZE: it holds Enumerable physical nodes everywhere except
 * its leaf, which is a {@code LogicalTableScan} (Convention.NONE) injected by {@link StagePlanner}.
 * A targeted Volcano pass converts only that leaf to {@code EnumerableTableScan}; all other nodes
 * pass through unchanged.
 *
 * <p>The entire compile+bind+drain block runs inside {@link
 * CalciteClassLoaderHelper#withCalciteClassLoader} to ensure Janino can resolve plugin classes
 * (CALCITE-3745 workaround).
 */
public final class CoordinatorTreeExecutor {

  private CoordinatorTreeExecutor() {}

  /**
   * Compiles and executes the coordinator tree over gathered rows.
   *
   * @param coordinatorTree the post-split coordinator RelNode (Enumerable nodes + LogicalTableScan
   *     leaf)
   * @param gatheredRows the combined rows from InternalCalciteExec.reduce(), as
   *     List&lt;Object[]&gt;
   * @param rowType the row type of the gathered-rows table (= cut node row type)
   * @return result rows as List&lt;Object[]&gt; in plan output order
   */
  public static List<Object[]> execute(
      RelNode coordinatorTree, List<Object[]> gatheredRows, RelDataType rowType) {
    return CalciteClassLoaderHelper.withCalciteClassLoader(
        () -> executeInternal(coordinatorTree, gatheredRows, rowType),
        CoordinatorTreeExecutor.class);
  }

  private static List<Object[]> executeInternal(
      RelNode coordinatorTree, List<Object[]> gatheredRows, RelDataType rowType) {
    // 1. HEP pass: extract windows, then merge Project/Filter into Calc so that the Enumerable
    //    convention can handle them. EnumerableProject.implement() throws under Prefer.ARRAY, so
    //    standalone Projects must become Calcs. This is safe even on post-optimize trees where
    //    nodes are already EnumerableCalc — HEP rules only fire on matching Logical nodes and skip
    //    Enumerable ones.
    //
    //    PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW MUST COME FIRST. The placement floor classifies a
    //    Project holding a RexOver as NEEDS_GATHER, so window functions land here — `top N f`
    //    arrives as Project(ROW_NUMBER) + Filter(rn <= k) + Project. Without this rule the Calc
    //    rules below merge those into ONE LogicalCalc carrying the RexOver, and no rule can convert
    //    such a Calc to EnumerableConvention: Volcano then fails with CannotPlanException("Missing
    //    conversion is LogicalCalc[convention: NONE -> ENUMERABLE]"). Same ordering requirement as
    //    the shard path in CalciteExecAggregator.optimizeFragment; regression test is
    //    CoordinatorTreeExecutorTest.execute_window_rank_filter_keeps_only_the_top_ranked_row.
    HepProgramBuilder hepBuilder = new HepProgramBuilder();
    hepBuilder.addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
    hepBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
    hepBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    hepBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    HepPlanner hepPlanner = new HepPlanner(hepBuilder.build());
    hepPlanner.setRoot(coordinatorTree);
    RelNode prepared = hepPlanner.findBestExp();

    // 2. Volcano pass: convert all remaining Logical nodes to EnumerableConvention.
    RelOptPlanner planner = prepared.getCluster().getPlanner();
    for (var rule : EnumerableRules.rules()) {
      planner.addRule(rule);
    }
    RelTraitSet desired = prepared.getTraitSet().replace(EnumerableConvention.INSTANCE);
    planner.setRoot(planner.changeTraits(prepared, desired));
    RelNode executablePlan = planner.findBestExp();

    // 2. Compile the EnumerableRel to a Bindable via Janino.
    //    SparkHandler is null — that is precisely the code path that Janino-compiles.
    @SuppressWarnings("unchecked")
    Bindable<Object[]> bindable =
        (Bindable<Object[]>)
            EnumerableInterpretable.toBindable(
                ImmutableMap.of(),
                null,
                (EnumerableRel) executablePlan,
                EnumerableRel.Prefer.ARRAY);

    // 3. Build a DataContext with the root schema (so EnumerableTableScan's generated code
    //    can navigate root.getRootSchema()) and the gathered rows in the stash slot.
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    // Register the gathered-rows table under the name used by StagePlanner.buildGatheredRowsScan
    osSchema.add(
        "_gathered_rows",
        new RelFragmentCodec.ShardRowSourceTable(rowType, StagePlanner.GATHERED_ROWS_STASH_KEY));

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
            if (StagePlanner.GATHERED_ROWS_STASH_KEY.equals(name)) {
              return gatheredRows;
            }
            return null;
          }
        };

    // 4. Drain the Enumerable into a List<Object[]>.
    //    IMPORTANT: when the output row type has a single column, Calcite's Prefer.ARRAY
    //    returns a bare scalar instead of an Object[]. Guard against that.
    Enumerable<Object[]> enumerable = bindable.bind(dataContext);
    int outputColumnCount = executablePlan.getRowType().getFieldCount();
    List<Object[]> result = new ArrayList<>();
    try (var enumerator = enumerable.enumerator()) {
      while (enumerator.moveNext()) {
        Object current = enumerator.current();
        if (outputColumnCount == 1 && !(current instanceof Object[])) {
          // Single-column result returned as a bare scalar by Prefer.ARRAY
          result.add(new Object[] {current});
        } else {
          result.add(((Object[]) current).clone());
        }
      }
    }
    return result;
  }
}
