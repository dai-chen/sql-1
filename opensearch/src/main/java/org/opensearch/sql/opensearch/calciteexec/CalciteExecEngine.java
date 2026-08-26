/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.Dedup;
import org.opensearch.sql.calcite.plan.rule.PPLDedupConvertRule;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Execution engine that routes window-family PPL plans (dedup, eventstats/streamstats) to the
 * calcite_exec custom aggregation for distributed shard-level execution. Phase 1 PoC: shard
 * executes a serialized Calcite plan fragment; coordinator executes the reduce tree via Calcite
 * Enumerable+Janino. No hand-written operator logic.
 */
public class CalciteExecEngine implements ExecutionEngine {
  private static final Logger log = LogManager.getLogger(CalciteExecEngine.class);

  /**
   * Controls whether this engine is active. Enabled by default on this PoC branch. Will be gated by
   * a cluster setting in production.
   */
  public static volatile boolean enabled = true;

  private final OpenSearchClient client;

  public CalciteExecEngine(OpenSearchClient client) {
    this.client = client;
  }

  @Override
  public boolean canVectorize(RelNode plan) {
    if (!enabled) {
      return false;
    }
    PlanInspection inspection = inspectPlan(plan);
    if (inspection.scanCount != 1) {
      return false;
    }
    if (inspection.hasJoin || inspection.hasUnion) {
      return false;
    }
    if (!inspection.hasDedup && !inspection.hasWindow) {
      return false;
    }
    // Additional safety: must have a recognized OpenSearch scan
    if (PlanSplitter.findScan(plan) == null) {
      return false;
    }
    return true;
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    client.schedule(
        () -> {
          try {
            log.info("CalciteExecEngine: routing plan to calcite_exec aggregation");
            log.debug("CalciteExecEngine: input plan:\n{}", plan.explain());

            // Split the plan into shard fragment + reduce spec
            PlanSplitter.SplitResult split = PlanSplitter.split(plan);
            List<String> fieldNames = split.shardFieldNames();
            List<SqlTypeName> fieldTypes = split.shardFieldTypes();
            log.debug(
                "CalciteExecEngine: scan fieldNames={}, reduceSpec={}",
                fieldNames,
                split.reduceSpec());

            AbstractCalciteIndexScan scan = PlanSplitter.findScan(plan);
            String indexName = scan.getOsIndex().getIndexName().toString();

            // Serialize the shard fragment to Base64 JSON
            String base64Plan = serializeFragment(split.shardFragment());

            // Build schema param: "name:TYPE" pairs
            List<String> schemaParam = new ArrayList<>();
            for (int i = 0; i < fieldNames.size(); i++) {
              schemaParam.add(fieldNames.get(i) + ":" + fieldTypes.get(i).getName());
            }

            // Build search request with serialized plan
            CalciteExecAggregationBuilder aggBuilder =
                new CalciteExecAggregationBuilder("calcite_stage");
            aggBuilder.setFields(fieldNames);
            aggBuilder.setProbe(false);
            aggBuilder.setPlan(base64Plan);
            aggBuilder.setSchema(schemaParam);

            SearchSourceBuilder sourceBuilder =
                new SearchSourceBuilder()
                    .query(QueryBuilders.matchAllQuery())
                    .size(0)
                    .aggregation(aggBuilder);

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(sourceBuilder);

            log.info(
                "CalciteExecEngine: sending serialized plan to shard for index [{}], fields={}",
                indexName,
                fieldNames);

            SearchResponse searchResponse =
                client
                    .getNodeClient()
                    .orElseThrow(
                        () -> new IllegalStateException("CalciteExecEngine requires NodeClient"))
                    .search(searchRequest)
                    .actionGet();

            InternalCalciteExec aggResult = searchResponse.getAggregations().get("calcite_stage");
            List<Object[]> shardRows = aggResult.getRows();

            log.info(
                "CalciteExecEngine: received {} rows from shard plan execution", shardRows.size());

            // Execute the reduce tree on coordinator via Calcite Enumerable+Janino
            List<Object[]> finalRows =
                executeReduceTree(split.reduceSpec(), shardRows, fieldNames, fieldTypes);

            // Build response using the output row type from reduce execution
            RelDataType outputType = getOutputRowType(plan);
            QueryResponse response = buildResponse(finalRows, outputType);
            listener.onResponse(response);

          } catch (Exception e) {
            log.error("CalciteExecEngine execution failed", e);
            listener.onFailure(e);
          }
        });
  }

  private String serializeFragment(RelNode fragment) {
    RelJsonWriter jsonWriter = new RelJsonWriter();
    fragment.explain(jsonWriter);
    String json = jsonWriter.asString();
    return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Executes the reduce tree on the coordinator using Calcite Enumerable+Janino compilation. Builds
   * a fresh plan tree in a new VolcanoPlanner with all required rules, then converts to Bindable
   * and executes with shard rows injected via DataContext.
   */
  private List<Object[]> executeReduceTree(
      PlanSplitter.ReduceSpec reduceSpec,
      List<Object[]> shardRows,
      List<String> fieldNames,
      List<SqlTypeName> fieldTypes) {

    return CalciteClassLoaderHelper.withCalciteClassLoader(
        () -> {
          SchemaPlus rootSchema = FragmentDeserializer.buildSchema(fieldNames, fieldTypes);

          // Create a VolcanoPlanner with ConventionTraitDef + all required rules
          VolcanoPlanner planner = new VolcanoPlanner();
          planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
          for (RelOptRule rule : Bindables.RULES) {
            planner.addRule(rule);
          }
          // Enumerable rules: TableScan, Window, Calc (NOT Filter/Project which are stubs)
          planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          // Merge Filter/Project into LogicalCalc before Enumerable conversion
          planner.addRule(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_CALC);
          planner.addRule(org.apache.calcite.rel.rules.CoreRules.FILTER_TO_CALC);
          planner.addRule(org.apache.calcite.rel.rules.CoreRules.FILTER_CALC_MERGE);
          planner.addRule(org.apache.calcite.rel.rules.CoreRules.PROJECT_CALC_MERGE);
          planner.addRule(org.apache.calcite.rel.rules.CoreRules.CALC_MERGE);
          // Expand Project with OVER expressions into LogicalWindow + LogicalProject
          planner.addRule(
              org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

          JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
          RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

          // Build the reduce tree from scratch using RelBuilder with our planner's cluster
          CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
          CalciteCatalogReader catalogReader =
              new CalciteCatalogReader(
                  calciteSchema,
                  calciteSchema.path(null),
                  typeFactory,
                  new CalciteConnectionConfigImpl(new Properties()));

          RelBuilder builder =
              RelBuilder.create(
                  Frameworks.newConfigBuilder().defaultSchema(rootSchema).build(), cluster);

          // We need a RelBuilder that uses our cluster. The simplest way: push a scan,
          // then build the reduce operations on top.
          builder.scan(FragmentDeserializer.TABLE_NAME);

          // Build the reduce operations based on the spec
          if (reduceSpec instanceof PlanSplitter.DedupReduce dedupSpec) {
            log.info(
                "CalciteExecEngine reduce: dedupFields={}, allowedDup={}, keepEmpty={},"
                    + " outputProj={}",
                dedupSpec.dedupFieldIndices(),
                dedupSpec.allowedDuplication(),
                dedupSpec.keepEmpty(),
                dedupSpec.outputProjection());
            List<RexNode> dedupFields = new ArrayList<>();
            for (int idx : dedupSpec.dedupFieldIndices()) {
              dedupFields.add(builder.field(idx));
            }
            if (dedupSpec.keepEmpty()) {
              PPLDedupConvertRule.buildDedupOrNull(
                  builder, dedupFields, dedupSpec.allowedDuplication(), dedupSpec.inputCollation());
            } else {
              PPLDedupConvertRule.buildDedupNotNull(
                  builder, dedupFields, dedupSpec.allowedDuplication(), dedupSpec.inputCollation());
            }
            if (dedupSpec.outputProjection() != null) {
              List<RexNode> projExprs = new ArrayList<>();
              for (int idx : dedupSpec.outputProjection()) {
                projExprs.add(builder.field(idx));
              }
              builder.project(projExprs);
            }
          } else if (reduceSpec instanceof PlanSplitter.WindowReduce windowSpec) {
            Window window = windowSpec.window();
            for (Window.Group group : window.groups) {
              for (Window.RexWinAggCall aggCall : group.aggCalls) {
                List<RexNode> partitionKeys = new ArrayList<>();
                for (int key : group.keys) {
                  partitionKeys.add(builder.field(key));
                }
                List<RexNode> operands = new ArrayList<>();
                for (RexNode operand : aggCall.getOperands()) {
                  if (operand instanceof RexInputRef ref) {
                    operands.add(builder.field(ref.getIndex()));
                  }
                }
                RexNode overExpr =
                    builder
                        .aggregateCall((SqlAggFunction) aggCall.getOperator(), operands)
                        .over()
                        .partitionBy(partitionKeys)
                        .toRex();
                builder.projectPlus(overExpr);
              }
            }
            if (windowSpec.outputProjection() != null) {
              List<RexNode> projExprs = new ArrayList<>();
              for (int idx : windowSpec.outputProjection()) {
                projExprs.add(builder.field(idx));
              }
              builder.project(projExprs);
            }
          }

          RelNode reduceTree = builder.build();

          // Convert to EnumerableConvention (Window requires Enumerable+Janino, not Interpreter)
          RelTraitSet desiredTraits = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
          RelNode converted = planner.changeTraits(reduceTree, desiredTraits);
          planner.setRoot(converted);
          RelNode best = planner.findBestExp();

          // Compile to Bindable via EnumerableInterpretable.toBindable
          Bindable<Object[]> bindable;
          if (best instanceof Bindable) {
            bindable = (Bindable<Object[]>) best;
          } else if (best instanceof EnumerableRel enumerableRel) {
            bindable =
                (Bindable<Object[]>)
                    EnumerableInterpretable.toBindable(
                        Map.of(), null, enumerableRel, EnumerableRel.Prefer.ARRAY);
          } else {
            throw new IllegalStateException(
                "Failed to convert reduce tree to executable form: " + best.getClass());
          }

          // Execute with the shard rows injected via DataContext
          DataContext dataContext = new ShardDataContext(shardRows, rootSchema);
          Enumerable<Object[]> result = bindable.bind(dataContext);

          List<Object[]> output = new ArrayList<>();
          @SuppressWarnings("unchecked")
          Enumerable<Object> rawResult = (Enumerable<Object>) (Enumerable<?>) result;
          try (Enumerator<Object> enumerator = rawResult.enumerator()) {
            while (enumerator.moveNext()) {
              Object current = enumerator.current();
              // Calcite may return scalar values for single-column results
              if (current instanceof Object[] arr) {
                output.add(arr.clone());
              } else {
                output.add(new Object[] {current});
              }
            }
          }
          return output;
        },
        CalciteExecEngine.class);
  }

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException(
            "CalciteExecEngine does not support PhysicalPlan execution"));
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException(
            "CalciteExecEngine does not support PhysicalPlan execution"));
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException(
            "CalciteExecEngine does not support PhysicalPlan explain"));
  }

  private RelDataType getOutputRowType(RelNode plan) {
    return plan.getRowType();
  }

  private QueryResponse buildResponse(List<Object[]> rows, RelDataType rowType) {
    List<RelDataTypeField> outputFields = rowType.getFieldList();

    List<ExprValue> exprRows = new ArrayList<>();
    for (Object[] row : rows) {
      Map<String, ExprValue> tuple = new LinkedHashMap<>();
      for (int i = 0; i < outputFields.size(); i++) {
        Object value = i < row.length ? row[i] : null;
        ExprValue exprValue = ExprValueUtils.fromObjectValue(value);
        tuple.put(outputFields.get(i).getName(), exprValue);
      }
      exprRows.add(ExprTupleValue.fromExprValueMap(tuple));
    }

    List<Schema.Column> columns = new ArrayList<>();
    for (RelDataTypeField field : outputFields) {
      ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
      columns.add(new Schema.Column(field.getName(), null, exprType));
    }

    return new QueryResponse(new Schema(columns), exprRows, null);
  }

  private static PlanInspection inspectPlan(RelNode plan) {
    PlanInspection result = new PlanInspection();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
          result.scanCount++;
        }
        if (node instanceof Dedup) {
          result.hasDedup = true;
        }
        if (node instanceof Window) {
          result.hasWindow = true;
        }
        if (node instanceof Join) {
          result.hasJoin = true;
        }
        if (node instanceof Union) {
          result.hasUnion = true;
        }
        super.visit(node, ordinal, parent);
      }
    }.go(plan);
    return result;
  }

  private static class PlanInspection {
    int scanCount = 0;
    boolean hasDedup = false;
    boolean hasWindow = false;
    boolean hasJoin = false;
    boolean hasUnion = false;
  }
}
