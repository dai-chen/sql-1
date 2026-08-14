/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CalciteExecAggregator extends MetricsAggregator {

  private static final Logger log = LogManager.getLogger(CalciteExecAggregator.class);

  private final List<String> fields;
  private final boolean probe;
  private final String plan;
  private final List<String> schema;
  private final List<Object[]> collectedRows = new ArrayList<>();
  private volatile String probeResult;
  private volatile boolean probeExecuted = false;

  CalciteExecAggregator(
      String name,
      List<String> fields,
      boolean probe,
      String plan,
      List<String> schema,
      SearchContext context,
      Aggregator parent,
      Map<String, Object> metadata
  ) throws IOException {
    super(name, context, parent, metadata);
    this.fields = fields;
    this.probe = probe;
    this.plan = plan;
    this.schema = schema;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
    // Detect field types from the leaf reader's field infos and load appropriate doc values
    NumericDocValues[] numericDvs = new NumericDocValues[fields.size()];
    org.apache.lucene.index.SortedNumericDocValues[] sortedNumericDvs = new org.apache.lucene.index.SortedNumericDocValues[fields.size()];
    SortedDocValues[] sortedDvs = new SortedDocValues[fields.size()];
    org.apache.lucene.index.SortedSetDocValues[] sortedSetDvs = new org.apache.lucene.index.SortedSetDocValues[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      var fieldInfo = ctx.reader().getFieldInfos().fieldInfo(fields.get(i));
      if (fieldInfo != null && fieldInfo.getDocValuesType() == org.apache.lucene.index.DocValuesType.SORTED_NUMERIC) {
        sortedNumericDvs[i] = DocValues.getSortedNumeric(ctx.reader(), fields.get(i));
      } else if (fieldInfo != null && fieldInfo.getDocValuesType() == org.apache.lucene.index.DocValuesType.NUMERIC) {
        numericDvs[i] = DocValues.getNumeric(ctx.reader(), fields.get(i));
      } else if (fieldInfo != null && fieldInfo.getDocValuesType() == org.apache.lucene.index.DocValuesType.SORTED) {
        sortedDvs[i] = DocValues.getSorted(ctx.reader(), fields.get(i));
      } else if (fieldInfo != null && fieldInfo.getDocValuesType() == org.apache.lucene.index.DocValuesType.SORTED_SET) {
        sortedSetDvs[i] = DocValues.getSortedSet(ctx.reader(), fields.get(i));
      }
    }

    // Run Janino probe once
    if (probe && !probeExecuted) {
      probeExecuted = true;
      try {
        String result = CalciteClassLoaderHelper.withCalciteClassLoader(() -> {
          // Generate and compile a trivial expression through Calcite's Janino path,
          // same pattern as CalciteScriptEngine.compile()
          String code =
              "public Object[] apply(Object root0) {\n"
              + "  return new Object[] { Integer.valueOf(40 + 2) };\n"
              + "}\n";
          org.apache.calcite.rex.RexExecutable executable =
              new org.apache.calcite.rex.RexExecutable(code, "calcite_exec probe");
          org.apache.calcite.linq4j.function.Function1 fn = executable.getFunction();
          Object[] result1 = (Object[]) fn.apply(null);
          if (result1 != null && result1.length == 1 && Integer.valueOf(42).equals(result1[0])) {
            return "JANINO_COMPILE_OK";
          }
          return "JANINO_COMPILE_WRONG_RESULT: " + java.util.Arrays.toString(result1);
        }, CalciteExecAggregator.class);
        probeResult = result;
      } catch (Exception e) {
        probeResult = "JANINO_COMPILE_FAILED: " + e.getMessage();
      }
    }

    return new LeafBucketCollectorBase(sub, null) {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        Object[] row = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          if (numericDvs[i] != null && numericDvs[i].advanceExact(doc)) {
            row[i] = numericDvs[i].longValue();
          } else if (sortedNumericDvs[i] != null && sortedNumericDvs[i].advanceExact(doc)) {
            if (sortedNumericDvs[i].docValueCount() > 0) {
              row[i] = sortedNumericDvs[i].nextValue();
            }
          } else if (sortedDvs[i] != null && sortedDvs[i].advanceExact(doc)) {
            row[i] = sortedDvs[i].lookupOrd(sortedDvs[i].ordValue()).utf8ToString();
          } else if (sortedSetDvs[i] != null && sortedSetDvs[i].advanceExact(doc)) {
            // Read the first value from the sorted set (keyword fields in OpenSearch)
            row[i] = sortedSetDvs[i].lookupOrd(sortedSetDvs[i].nextOrd()).utf8ToString();
          } else {
            row[i] = null;
          }
        }
        collectedRows.add(row);
      }
    };
  }

  @Override
  public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
    if (plan != null && !plan.isEmpty()) {
      log.debug("CalciteExecAggregator: executing serialized plan on shard, "
          + "inputRows={}, planLength={}", collectedRows.size(), plan.length());
      List<Object[]> outputRows = executePlan(collectedRows);
      log.debug("CalciteExecAggregator: shard plan execution complete, outputRows={}",
          outputRows.size());
      return new InternalCalciteExec(name, outputRows, probeResult, metadata());
    }
    return new InternalCalciteExec(name, new ArrayList<>(collectedRows), probeResult, metadata());
  }

  private List<Object[]> executePlan(List<Object[]> inputRows) {
    return CalciteClassLoaderHelper.withCalciteClassLoader(() -> {
      // Parse the schema param: pairs of "name:TYPE"
      List<String> fieldNames = new ArrayList<>();
      List<SqlTypeName> fieldTypes = new ArrayList<>();
      for (String entry : schema) {
        int colon = entry.indexOf(':');
        fieldNames.add(entry.substring(0, colon));
        fieldTypes.add(SqlTypeName.valueOf(entry.substring(colon + 1)));
      }

      SchemaPlus rootSchema = FragmentDeserializer.buildSchema(fieldNames, fieldTypes);
      RelNode rel = FragmentDeserializer.deserialize(plan, fieldNames, fieldTypes, rootSchema);

      // Convert to Bindable convention (handles ScannableTable via BindableTableScan)
      Bindable<Object[]> bindable = convertToBindable(rel);

      DataContext dataContext = new ShardDataContext(inputRows, rootSchema);
      Enumerable<Object[]> result = bindable.bind(dataContext);

      List<Object[]> output = new ArrayList<>();
      try (Enumerator<Object[]> enumerator = result.enumerator()) {
        while (enumerator.moveNext()) {
          output.add(enumerator.current().clone());
        }
      }
      return output;
    }, CalciteExecAggregator.class);
  }

  // Convert logical plan to Bindable via the Volcano planner
  @SuppressWarnings("unchecked")
  private static Bindable<Object[]> convertToBindable(RelNode rel) {
    org.apache.calcite.plan.RelOptPlanner planner = rel.getCluster().getPlanner();
    org.apache.calcite.plan.RelTraitSet desiredTraits = rel.getCluster().traitSet()
        .replace(org.apache.calcite.interpreter.BindableConvention.INSTANCE);
    rel = planner.changeTraits(rel, desiredTraits);
    planner.setRoot(rel);
    RelNode best = planner.findBestExp();
    if (best instanceof Bindable) {
      return (Bindable<Object[]>) best;
    }
    throw new IllegalStateException("Failed to convert plan to Bindable: " + best.getClass());
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalCalciteExec(name, List.of(), probeResult, metadata());
  }
}
