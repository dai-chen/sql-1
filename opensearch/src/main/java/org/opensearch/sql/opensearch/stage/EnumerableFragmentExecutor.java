/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.expression.function.udf.SpanFunction;

/** Compiles and executes a process-local Calcite fragment with Enumerable. */
final class EnumerableFragmentExecutor {

  private static final int MAX_COMPILED_FRAGMENT_CACHE_ENTRIES = 256;
  private static final HepProgram LOGICAL_PREPARATION =
      new HepProgramBuilder()
          .addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
          .addRuleInstance(CoreRules.FILTER_TO_CALC)
          .addRuleInstance(CoreRules.PROJECT_TO_CALC)
          .addRuleInstance(CoreRules.CALC_MERGE)
          .build();
  private static final HepProgram EXECUTABLE_PREPARATION =
      new HepProgramBuilder()
          .addRuleInstance(EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE)
          .addRuleInstance(EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE)
          .build();
  private static final Cache<CacheKey, CompiledPlan> COMPILED_FRAGMENT_CACHE =
      CacheBuilder.newBuilder().maximumSize(MAX_COMPILED_FRAGMENT_CACHE_ENTRIES).build();

  private EnumerableFragmentExecutor() {}

  static List<Object[]> execute(
      RelNode tree, SchemaPlus rootSchema, Map<String, Object> dataContextValues) {
    return SpanFunction.withTimestampCache(
        () ->
            CalciteClassLoaderHelper.withCalciteClassLoader(
                () -> executeInternal(tree, rootSchema, dataContextValues),
                EnumerableFragmentExecutor.class));
  }

  static List<Object[]> executeCached(
      CacheKey cacheKey,
      RelNode tree,
      SchemaPlus rootSchema,
      Map<String, Object> dataContextValues) {
    return SpanFunction.withTimestampCache(
        () ->
            CalciteClassLoaderHelper.withCalciteClassLoader(
                () -> executeCompiled(cachedCompile(cacheKey, tree), rootSchema, dataContextValues),
                EnumerableFragmentExecutor.class));
  }

  private static List<Object[]> executeInternal(
      RelNode tree, SchemaPlus rootSchema, Map<String, Object> dataContextValues) {
    return executeCompiled(compile(tree), rootSchema, dataContextValues);
  }

  private static CompiledPlan cachedCompile(CacheKey cacheKey, RelNode tree) {
    try {
      return COMPILED_FRAGMENT_CACHE.get(cacheKey, () -> compile(tree));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("Unable to compile Calcite fragment", cause);
    }
  }

  private static CompiledPlan compile(RelNode tree) {
    HepPlanner hepPlanner = new HepPlanner(LOGICAL_PREPARATION);
    hepPlanner.setRoot(tree);
    RelNode prepared = hepPlanner.findBestExp();

    RelOptPlanner planner = prepared.getCluster().getPlanner();
    for (var rule : EnumerableRules.rules()) {
      planner.addRule(rule);
    }
    RelTraitSet desired = prepared.getTraitSet().replace(EnumerableConvention.INSTANCE);
    planner.setRoot(planner.changeTraits(prepared, desired));
    RelNode executablePlan = planner.findBestExp();

    HepPlanner executablePlanner = new HepPlanner(EXECUTABLE_PREPARATION);
    executablePlanner.setRoot(executablePlan);
    executablePlan = executablePlanner.findBestExp();

    @SuppressWarnings("unchecked")
    Bindable<Object[]> bindable =
        (Bindable<Object[]>)
            EnumerableInterpretable.toBindable(
                ImmutableMap.of(),
                null,
                (EnumerableRel) executablePlan,
                EnumerableRel.Prefer.ARRAY);
    return new CompiledPlan(bindable, executablePlan.getRowType().getFieldCount());
  }

  private static List<Object[]> executeCompiled(
      CompiledPlan plan, SchemaPlus rootSchema, Map<String, Object> dataContextValues) {
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
          public org.apache.calcite.linq4j.QueryProvider getQueryProvider() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object get(String name) {
            return dataContextValues.get(name);
          }
        };

    Enumerable<Object[]> enumerable = plan.bindable().bind(dataContext);
    List<Object[]> result = new ArrayList<>();
    try (var enumerator = enumerable.enumerator()) {
      while (enumerator.moveNext()) {
        Object current = enumerator.current();
        if (plan.outputColumnCount() == 1 && !(current instanceof Object[])) {
          result.add(new Object[] {current});
        } else {
          result.add(((Object[]) current).clone());
        }
      }
    }
    return result;
  }

  record CacheKey(String fragmentJson, String inputRowTypeJson) {}

  private record CompiledPlan(Bindable<Object[]> bindable, int outputColumnCount) {}
}
