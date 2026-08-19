/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.calcite.avatica.util.StructImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Point;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.TimewrapPivot;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.ResourceLimitExceededException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.functions.DistinctCountApproxAggFunction;
import org.opensearch.sql.opensearch.functions.GeoIpFunction;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.stage.CalciteExecAggregationBuilder;
import org.opensearch.sql.opensearch.stage.CoordinatorTreeExecutor;
import org.opensearch.sql.opensearch.stage.InternalCalciteExec;
import org.opensearch.sql.opensearch.stage.RelFragmentCodec;
import org.opensearch.sql.opensearch.stage.StagePlan;
import org.opensearch.sql.opensearch.stage.StagePlanner;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch execution engine implementation. */
public class OpenSearchExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(OpenSearchExecutionEngine.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final OpenSearchClient client;

  private final ExecutionProtector executionProtector;
  private final PlanSerializer planSerializer;

  public OpenSearchExecutionEngine(
      OpenSearchClient client,
      ExecutionProtector executionProtector,
      PlanSerializer planSerializer) {
    this.client = client;
    this.executionProtector = executionProtector;
    this.planSerializer = planSerializer;
    registerOpenSearchFunctions();
  }

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    execute(physicalPlan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan physicalPlan,
      ExecutionContext context,
      ResponseListener<QueryResponse> listener) {
    PhysicalPlan plan = executionProtector.protect(physicalPlan);
    client.schedule(
        () -> {
          try {
            List<ExprValue> result = new ArrayList<>();

            context.getSplit().ifPresent(plan::add);
            plan.open();

            Integer querySizeLimit = context.getQuerySizeLimit();
            while (plan.hasNext() && (querySizeLimit == null || result.size() < querySizeLimit)) {
              result.add(plan.next());
            }

            QueryResponse response =
                new QueryResponse(
                    physicalPlan.schema(), result, planSerializer.convertToCursor(plan));
            listener.onResponse(response);
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            Explain openSearchExplain =
                new Explain() {
                  @Override
                  public ExplainResponseNode visitTableScan(
                      TableScanOperator node, Object context) {
                    return explain(
                        node,
                        context,
                        explainNode -> {
                          explainNode.setDescription(Map.of("request", node.explain()));
                        });
                  }
                };

            listener.onResponse(openSearchExplain.apply(plan));
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  private Hook.Closeable getPhysicalPlanInHook(
      AtomicReference<String> physical, SqlExplainLevel level) {
    return Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
        obj -> {
          RelRoot relRoot = (RelRoot) obj;
          physical.set(RelOptUtil.toString(relRoot.rel, level));
        });
  }

  private Hook.Closeable getCodegenInHook(AtomicReference<String> codegen) {
    return Hook.JAVA_PLAN.addThread(
        obj -> {
          codegen.set((String) obj);
        });
  }

  /**
   * Parse sourceBuilder JSON strings within the physical plan tree to objects. This finds any
   * sourceBuilder fields (which are serialized as JSON strings by RelJsonWriter) and parses them to
   * JSON objects for easier client consumption.
   */
  @SuppressWarnings("unchecked")
  private void parseSourceBuilderInPhysicalTree(Object physicalTree) {
    try {
      if (!(physicalTree instanceof Map)) {
        return;
      }
      Map<String, Object> tree = (Map<String, Object>) physicalTree;
      Object relsObj = tree.get("rels");
      if (!(relsObj instanceof List)) {
        return;
      }

      List<Object> rels = (List<Object>) relsObj;
      for (Object relObj : rels) {
        if (!(relObj instanceof Map)) {
          continue;
        }
        Map<String, Object> rel = (Map<String, Object>) relObj;

        // Parse sourceBuilder if it exists as a JSON string
        Object sourceBuilderObj = rel.get("sourceBuilder");
        if (sourceBuilderObj instanceof String) {
          try {
            String sourceBuilderJson = (String) sourceBuilderObj;
            Object parsed = objectMapper.readValue(sourceBuilderJson, Object.class);
            rel.put("sourceBuilder", parsed);
          } catch (Exception e) {
            logger.debug("Failed to parse sourceBuilder JSON: {}", e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to parse sourceBuilder in physical tree: " + e.getMessage());
    }
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    explain(rel, mode, null, context, listener);
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainMode mode,
      Format format,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            if (format == Format.JSON_TREE) {
              // Use RelJsonWriter for structured JSON tree output
              try {
                RelJsonWriter logicalWriter = new RelJsonWriter();
                rel.explain(logicalWriter);
                String logicalJson = logicalWriter.asString();

                AtomicReference<String> physicalJson = new AtomicReference<>();
                AtomicReference<Exception> physicalError = new AtomicReference<>();
                SqlExplainLevel level =
                    mode == ExplainMode.COST
                        ? SqlExplainLevel.ALL_ATTRIBUTES
                        : SqlExplainLevel.EXPPLAN_ATTRIBUTES;

                try (Hook.Closeable closeable =
                    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
                        obj -> {
                          try {
                            RelRoot relRoot = (RelRoot) obj;
                            RelJsonWriter physicalWriter = new RelJsonWriter();
                            relRoot.rel.explain(physicalWriter);
                            physicalJson.set(physicalWriter.asString());
                          } catch (Exception e) {
                            physicalError.set(e);
                          }
                        })) {
                  // triggers the hook
                  OpenSearchRelRunners.run(context, CalciteToolsHelper.optimize(rel, context));
                }

                if (physicalError.get() != null) {
                  throw physicalError.get();
                }

                // Parse JSON strings to objects for structured output
                Object logicalTree = objectMapper.readValue(logicalJson, Object.class);
                Object physicalTree = objectMapper.readValue(physicalJson.get(), Object.class);

                // Parse sourceBuilder JSON if present in physical plan
                parseSourceBuilderInPhysicalTree(physicalTree);

                ExplainResponseNodeV2 response =
                    new ExplainResponseNodeV2(logicalJson, physicalJson.get(), null);
                response.setLogicalTree(logicalTree);
                response.setPhysicalTree(physicalTree);

                listener.onResponse(new ExplainResponse(response));
              } catch (Exception e) {
                // RelJsonWriter can't handle some custom types (e.g., SystemLimitType enum)
                listener.onFailure(
                    new UnsupportedOperationException(
                        "Cannot serialize plan to json_tree format: " + e.getMessage(), e));
                return;
              }
            } else {
              // Original string format for json/yaml
              if (mode == ExplainMode.SIMPLE) {
                String logical = RelOptUtil.toString(rel, SqlExplainLevel.NO_ATTRIBUTES);
                listener.onResponse(
                    new ExplainResponse(new ExplainResponseNodeV2(logical, null, null)));
              } else {
                SqlExplainLevel level =
                    mode == ExplainMode.COST
                        ? SqlExplainLevel.ALL_ATTRIBUTES
                        : SqlExplainLevel.EXPPLAN_ATTRIBUTES;
                String logical = RelOptUtil.toString(rel, level);
                AtomicReference<String> physical = new AtomicReference<>();
                AtomicReference<String> javaCode = new AtomicReference<>();
                try (Hook.Closeable closeable = getPhysicalPlanInHook(physical, level)) {
                  if (mode == ExplainMode.EXTENDED) {
                    getCodegenInHook(javaCode);
                    CalcitePlanContext.skipEncoding.set(true);
                  }
                  // triggers the hook
                  OpenSearchRelRunners.run(context, CalciteToolsHelper.optimize(rel, context));
                }
                listener.onResponse(
                    new ExplainResponse(
                        new ExplainResponseNodeV2(logical, physical.get(), javaCode.get())));
              }
            }
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            CalcitePlanContext.skipEncoding.remove();
          }
        });
  }

  @Override
  public void execute(
      RelNode rel, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    client.schedule(
        () -> {
          try {
            // Staged execution gate: activate when the plan is stageable AND pushdown is disabled.
            // This is a configuration flip — the PoC posture is pushdown.enabled=false.
            StagePlan stagePlan = StagePlanner.split(rel);
            if (stagePlan.staged() && isPushdownDisabled(stagePlan)) {
              executeStagedPlan(stagePlan, context, listener);
              return;
            }
          } catch (Exception e) {
            listener.onFailure(e);
            return;
          }

          try (PreparedStatement statement = OpenSearchRelRunners.run(context, rel)) {
            ProfileMetric metric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
            long execTime = System.nanoTime();
            ResultSet result = statement.executeQuery();
            QueryResponse response =
                buildResultSet(result, rel.getRowType(), context.sysLimit.querySizeLimit());
            metric.add(System.nanoTime() - execTime);
            listener.onResponse(response);

          } catch (SQLException e) {
            if (isPitContextLimitReached(e)) {
              // reason (title) comes from the wrapped cause's message; keep it short and put the
              // explanation and remedy in details.
              ResourceLimitExceededException pitException =
                  new ResourceLimitExceededException(
                      "Too many open Point-In-Time (PIT) contexts on this node.", e);
              throw ErrorReport.wrap(pitException)
                  .code(ErrorCode.RESOURCE_LIMIT_EXCEEDED)
                  .details(
                      "This query opened a Point-In-Time (PIT) context on each shard and reached"
                          + " the limit set by [search.max_open_pit_context]. Increase that"
                          + " setting.")
                  .build();
            }
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Substring of the error OpenSearch's {@code SearchService} raises when a node has no free PIT
   * contexts. The engine opens a PIT (one reader context per shard) to page over a query it cannot
   * push down -- e.g. a {@code stats} that groups by a text field with no {@code keyword} sub-field
   * -- and a busy node exhausts its per-node budget. The raw failure is an opaque internal message,
   * so it is replaced with an actionable one when this marker appears anywhere in the cause chain.
   */
  private static final String PIT_CONTEXT_LIMIT_MARKER = "too many Point In Time contexts";

  /** Package-private for testing. Walks the cause chain guarding against self-referential loops. */
  static boolean isPitContextLimitReached(Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && message.contains(PIT_CONTEXT_LIMIT_MARKER)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gate condition for staged execution: returns true when pushdown is disabled. Reads the setting
   * from the scan's index settings, which is the same path CalciteLogicalIndexScan uses at line
   * 153.
   */
  private static boolean isPushdownDisabled(StagePlan stagePlan) {
    AbstractCalciteIndexScan scan = stagePlan.shardScan();
    Settings settings = scan.getOsIndex().getSettings();
    return !((Boolean) settings.getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED));
  }

  /**
   * Executes a staged plan: builds a size-0 search request carrying the {@code calcite_exec}
   * aggregation, retrieves the coordinator-side {@link InternalCalciteExec} from the response,
   * feeds the gathered rows through the coordinator tree, and converts the result to a {@link
   * QueryResponse}.
   */
  private void executeStagedPlan(
      StagePlan stagePlan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    try {
      ProfileMetric metric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
      long execTime = System.nanoTime();

      // 1. Build the search request
      SearchRequest searchRequest = buildStagedSearchRequest(stagePlan);

      // 2. Execute the search
      SearchResponse searchResponse =
          client
              .getNodeClient()
              .orElseThrow(
                  () -> new IllegalStateException("Staged execution requires a NodeClient"))
              .search(searchRequest)
              .actionGet();

      // 3. Extract InternalCalciteExec from the response aggregations.
      //    The aggregation name matches what we set in buildStagedSearchRequest.
      InternalAggregation rawAgg = searchResponse.getAggregations().get("calcite_stage");
      if (!(rawAgg instanceof InternalCalciteExec calciteExec)) {
        throw new IllegalStateException(
            "Expected InternalCalciteExec aggregation in staged response, got: "
                + (rawAgg == null ? "null" : rawAgg.getClass().getName()));
      }

      // 4. Convert gathered rows from List<List<Object>> to List<Object[]> for the DataContext
      // stash
      List<Object[]> gatheredRows = new ArrayList<>(calciteExec.getRows().size());
      for (List<Object> row : calciteExec.getRows()) {
        gatheredRows.add(row.toArray());
      }

      // 5. Execute the coordinator tree over gathered rows (tier-2)
      RelNode coordinatorTree = stagePlan.coordinatorTree();
      // The row type of the gathered-rows table is the cut node's output type, which is the
      // shardFragment's output row type. Under the generic floor, the fragment may contain a
      // Project or Filter above the scan, narrowing the row type relative to the scan.
      RelDataType gatheredRowType = stagePlan.shardFragment().getRowType();
      List<Object[]> outputRows =
          CoordinatorTreeExecutor.execute(coordinatorTree, gatheredRows, gatheredRowType);

      // 6. Convert output to QueryResponse
      RelDataType outputType = coordinatorTree.getRowType();
      Integer querySizeLimit = context.sysLimit.querySizeLimit();

      // Pre-compute ExprType per output field ONCE outside the row loop — temporal types
      // (TIMESTAMP, DATE, TIME) need the two-arg fromObjectValue to convert epoch-millis Longs
      // into formatted ExprTimestamp/Date/TimeValues matching the JDBC contract.
      List<RelDataTypeField> outputFields = outputType.getFieldList();
      int fieldCount = outputFields.size();
      ExprType[] exprTypes = new ExprType[fieldCount];
      for (int i = 0; i < fieldCount; i++) {
        exprTypes[i] =
            OpenSearchTypeFactory.convertRelDataTypeToExprType(outputFields.get(i).getType());
      }

      List<ExprValue> values = new ArrayList<>();
      for (Object[] row : outputRows) {
        if (querySizeLimit != null && values.size() >= querySizeLimit) {
          break;
        }
        Map<String, ExprValue> tuple = new LinkedHashMap<>();
        for (int i = 0; i < fieldCount; i++) {
          String fieldName = outputFields.get(i).getName();
          Object value = i < row.length ? row[i] : null;
          tuple.put(fieldName, ExprValueUtils.fromObjectValue(value, exprTypes[i]));
        }
        values.add(ExprTupleValue.fromExprValueMap(tuple));
      }

      // Build schema columns from outputType (reuses the pre-computed exprTypes)
      List<Column> columns = new ArrayList<>();
      for (int i = 0; i < fieldCount; i++) {
        columns.add(new Column(outputFields.get(i).getName(), null, exprTypes[i]));
      }
      Schema schema = new Schema(columns);

      metric.add(System.nanoTime() - execTime);
      listener.onResponse(new QueryResponse(schema, values, null));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Builds a staged search request: size=0, allowPartialSearchResults=false, the PredicateAnalyzer
   * query clause from the scan's PushDownContext, and exactly one {@code calcite_exec} aggregation.
   * No PIT, scroll, or search_after is ever created on this path.
   *
   * <p>Package-private for unit testing from {@code StagedRequestShapeTest}.
   */
  public static SearchRequest buildStagedSearchRequest(StagePlan stagePlan) {
    AbstractCalciteIndexScan scan = stagePlan.shardScan();
    String indexName = scan.getOsIndex().getIndexName().toString();

    // Extract relevance predicates from the shard fragment BEFORE serialization.
    // Relevance functions (query_string, match, match_phrase, etc.) cannot be Janino-compiled —
    // they require the inverted index and must always be in the DSL query clause, regardless of
    // the pushdown.enabled setting. Per design doc: "Push a predicate only if it consults an
    // index structure" and relevance functions "cannot be evaluated from doc_values at all".
    RelNode fragment = stagePlan.shardFragment();
    RelevanceExtractionResult extraction = extractRelevancePredicates(fragment, scan);
    fragment = extraction.cleanedFragment;

    // Serialize the (possibly cleaned) shard fragment
    String serializedPlan = RelFragmentCodec.serialize(fragment);

    // Build field descriptors from the scan's index field types and the fragment's row type.
    // Use the flattened type map so dotted paths like "address.city" resolve correctly.
    RelDataType scanRowType = scan.getRowType();
    Map<String, OpenSearchDataType> fieldTypes =
        OpenSearchDataType.traverseAndFlatten(scan.getOsIndex().getFieldOpenSearchTypes());
    List<CalciteExecAggregationBuilder.FieldDescriptor> fieldDescriptors = new ArrayList<>();
    for (RelDataTypeField field : scanRowType.getFieldList()) {
      // Look up the OpenSearch mapping type for this field
      OpenSearchDataType osType = fieldTypes.get(field.getName());
      String typeName = osType != null ? osType.getMappingType().toString() : "keyword";
      fieldDescriptors.add(
          new CalciteExecAggregationBuilder.FieldDescriptor(field.getName(), typeName));
    }

    // Build the calcite_exec aggregation
    CalciteExecAggregationBuilder aggBuilder =
        new CalciteExecAggregationBuilder("calcite_stage")
            .plan(serializedPlan)
            .fields(fieldDescriptors)
            .combine(stagePlan.combine())
            .forcingOperator(stagePlan.forcingOperator());

    // Build the SearchSourceBuilder: size=0, the query clause, and the aggregation
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.size(0);
    sourceBuilder.aggregation(aggBuilder);

    // Apply the pushed-down query clause from the PushDownContext (PredicateAnalyzer output).
    // The scan's PushDownContext creates a request builder that carries the query clause.
    org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder reqBuilder =
        scan.getPushDownContext().createRequestBuilder();
    QueryBuilder queryClause = reqBuilder.getSourceBuilder().query();
    // Combine with any relevance predicates extracted from the fragment
    if (extraction.relevanceQuery != null) {
      if (queryClause != null) {
        queryClause =
            org.opensearch.index.query.QueryBuilders.boolQuery()
                .filter(queryClause)
                .filter(extraction.relevanceQuery);
      } else {
        queryClause = extraction.relevanceQuery;
      }
    }
    if (queryClause != null) {
      sourceBuilder.query(queryClause);
    }

    // Build the SearchRequest: no PIT, no scroll, no search_after
    SearchRequest searchRequest =
        new SearchRequest()
            .indices(scan.getOsIndex().getIndexName().getIndexNames())
            .source(sourceBuilder);
    // Design invariant 7: every staged request sets allowPartialSearchResults(false)
    searchRequest.allowPartialSearchResults(false);

    return searchRequest;
  }

  /**
   * Names of relevance functions that cannot be Janino-compiled and MUST be pushed to the DSL query
   * clause regardless of pushdown settings. These consult the inverted index and have no doc_values
   * or _source representation.
   */
  private static final Set<String> RELEVANCE_FUNCTION_NAMES =
      Set.of(
          "query_string",
          "simple_query_string",
          "match",
          "match_phrase",
          "match_bool_prefix",
          "match_phrase_prefix",
          "multi_match");

  /** Result of extracting relevance predicates from a shard fragment. */
  private static class RelevanceExtractionResult {
    final RelNode cleanedFragment;
    final QueryBuilder relevanceQuery;

    RelevanceExtractionResult(RelNode cleanedFragment, QueryBuilder relevanceQuery) {
      this.cleanedFragment = cleanedFragment;
      this.relevanceQuery = relevanceQuery;
    }
  }

  /**
   * Walks the shard fragment looking for {@code LogicalFilter} nodes containing relevance function
   * calls. Extracts those predicates via {@link PredicateAnalyzer} and returns the cleaned fragment
   * (with the relevance filter removed) plus the query clause.
   *
   * <p>Only handles the simple case of a top-level Filter directly above the table scan with a
   * condition that IS a relevance call (possibly in a conjunction). Complex filter shapes pass
   * through unchanged — if the shard fragment fails to compile, it surfaces as a loud error per
   * design invariant.
   */
  private static RelevanceExtractionResult extractRelevancePredicates(
      RelNode fragment, AbstractCalciteIndexScan scan) {
    if (!(fragment instanceof org.apache.calcite.rel.logical.LogicalFilter)) {
      // Not a filter at the top — check if it's a Calc with a condition containing relevance
      if (fragment instanceof org.apache.calcite.rel.logical.LogicalCalc) {
        return extractRelevanceFromCalc(
            (org.apache.calcite.rel.logical.LogicalCalc) fragment, scan);
      }
      // Check if there's a Project over a Filter containing relevance
      if (fragment instanceof org.apache.calcite.rel.logical.LogicalProject) {
        org.apache.calcite.rel.logical.LogicalProject project =
            (org.apache.calcite.rel.logical.LogicalProject) fragment;
        if (project.getInput() instanceof org.apache.calcite.rel.logical.LogicalFilter) {
          RelevanceExtractionResult innerResult =
              extractRelevancePredicates(project.getInput(), scan);
          if (innerResult.relevanceQuery != null) {
            // Rebuild the project over the cleaned inner fragment
            RelNode newProject =
                project.copy(project.getTraitSet(), List.of(innerResult.cleanedFragment));
            return new RelevanceExtractionResult(newProject, innerResult.relevanceQuery);
          }
        }
      }
      return new RelevanceExtractionResult(fragment, null);
    }

    org.apache.calcite.rel.logical.LogicalFilter filter =
        (org.apache.calcite.rel.logical.LogicalFilter) fragment;
    RexNode condition = filter.getCondition();

    // Check if the condition contains a relevance function call
    if (!containsRelevanceFunction(condition)) {
      return new RelevanceExtractionResult(fragment, null);
    }

    // Try to convert the relevance predicate to a QueryBuilder via PredicateAnalyzer
    try {
      List<String> schema = scan.getRowType().getFieldNames();
      Map<String, ExprType> fieldTypes = buildExprTypeMap(scan);
      QueryBuilder qb =
          PredicateAnalyzer.analyze(
              condition, schema, fieldTypes, scan.getRowType(), scan.getCluster());
      // Success: remove the entire filter from the fragment (the DSL handles it)
      return new RelevanceExtractionResult(filter.getInput(), qb);
    } catch (Exception e) {
      // If PredicateAnalyzer can't handle it (e.g. mixed relevance + scalar), try to
      // decompose conjunctions and extract only the relevance parts.
      return extractRelevanceFromConjunction(filter, scan);
    }
  }

  /**
   * For a LogicalCalc, check if its condition program contains relevance functions. If so, attempt
   * to extract and convert them.
   */
  private static RelevanceExtractionResult extractRelevanceFromCalc(
      org.apache.calcite.rel.logical.LogicalCalc calc, AbstractCalciteIndexScan scan) {
    org.apache.calcite.rex.RexProgram program = calc.getProgram();
    if (program.getCondition() == null) {
      return new RelevanceExtractionResult(calc, null);
    }
    // Expand the condition to a RexNode
    RexNode condition = program.expandLocalRef(program.getCondition());
    if (!containsRelevanceFunction(condition)) {
      return new RelevanceExtractionResult(calc, null);
    }

    // Try to push the relevance predicate entirely
    try {
      List<String> schema = scan.getRowType().getFieldNames();
      Map<String, ExprType> fieldTypes = buildExprTypeMap(scan);
      QueryBuilder qb =
          PredicateAnalyzer.analyze(
              condition, schema, fieldTypes, scan.getRowType(), scan.getCluster());
      // Remove the condition from the Calc (keep only the projection)
      org.apache.calcite.rex.RexProgram newProgram =
          org.apache.calcite.rex.RexProgram.create(
              program.getInputRowType(),
              program.getProjectList().stream().map(program::expandLocalRef).toList(),
              null, // no condition
              program.getOutputRowType(),
              calc.getCluster().getRexBuilder());
      RelNode newCalc =
          org.apache.calcite.rel.logical.LogicalCalc.create(calc.getInput(), newProgram);
      return new RelevanceExtractionResult(newCalc, qb);
    } catch (Exception e) {
      // Can't extract — leave as-is, will fail loudly at compile time
      return new RelevanceExtractionResult(calc, null);
    }
  }

  /**
   * Decomposes a conjunction (AND) to extract relevance predicates while leaving scalar predicates
   * in the fragment.
   */
  private static RelevanceExtractionResult extractRelevanceFromConjunction(
      org.apache.calcite.rel.logical.LogicalFilter filter, AbstractCalciteIndexScan scan) {
    RexNode condition = filter.getCondition();
    List<RexNode> conjuncts = org.apache.calcite.rex.RexUtil.flattenAnd(List.of(condition));

    List<RexNode> relevanceParts = new ArrayList<>();
    List<RexNode> scalarParts = new ArrayList<>();

    for (RexNode conjunct : conjuncts) {
      if (containsRelevanceFunction(conjunct)) {
        relevanceParts.add(conjunct);
      } else {
        scalarParts.add(conjunct);
      }
    }

    if (relevanceParts.isEmpty()) {
      return new RelevanceExtractionResult(filter, null);
    }

    // Convert relevance parts to QueryBuilder
    QueryBuilder relevanceQb = null;
    try {
      RexNode relevanceCondition =
          org.apache.calcite.rex.RexUtil.composeConjunction(
              filter.getCluster().getRexBuilder(), relevanceParts);
      List<String> schema = scan.getRowType().getFieldNames();
      Map<String, ExprType> fieldTypes = buildExprTypeMap(scan);
      relevanceQb =
          PredicateAnalyzer.analyze(
              relevanceCondition, schema, fieldTypes, scan.getRowType(), scan.getCluster());
    } catch (Exception e) {
      // Can't convert — leave unchanged, will fail loudly
      return new RelevanceExtractionResult(filter, null);
    }

    // Rebuild the filter with only scalar parts (or remove it entirely)
    RelNode cleanedFragment;
    if (scalarParts.isEmpty()) {
      cleanedFragment = filter.getInput();
    } else {
      RexNode scalarCondition =
          org.apache.calcite.rex.RexUtil.composeConjunction(
              filter.getCluster().getRexBuilder(), scalarParts);
      cleanedFragment = filter.copy(filter.getTraitSet(), filter.getInput(), scalarCondition);
    }

    return new RelevanceExtractionResult(cleanedFragment, relevanceQb);
  }

  /** Builds a field name → ExprType map from the scan's OpenSearch field types. */
  private static Map<String, ExprType> buildExprTypeMap(AbstractCalciteIndexScan scan) {
    Map<String, OpenSearchDataType> osTypes =
        OpenSearchDataType.traverseAndFlatten(scan.getOsIndex().getFieldOpenSearchTypes());
    Map<String, ExprType> result = new HashMap<>();
    for (Map.Entry<String, OpenSearchDataType> entry : osTypes.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getExprType());
    }
    return result;
  }

  /** Checks recursively if a RexNode contains a relevance function call. */
  private static boolean containsRelevanceFunction(RexNode node) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      String opName = call.getOperator().getName().toLowerCase(Locale.ROOT);
      if (RELEVANCE_FUNCTION_NAMES.contains(opName)) {
        return true;
      }
      // Check operands recursively (for AND/OR containing relevance)
      for (RexNode operand : call.getOperands()) {
        if (containsRelevanceFunction(operand)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Process values recursively, handling geo points, nested maps, structs and arrays. When a {@link
   * RelDataType} is provided, struct values (StructImpl) are converted to Maps keyed by field
   * names, preserving field-name information in the JSON output.
   *
   * @param value The raw value from the JDBC result set
   * @param type The Calcite type metadata for this value, or null if unavailable
   */
  @SuppressWarnings("unchecked")
  private static Object processValue(Object value, RelDataType type) throws SQLException {
    if (value == null) {
      return null;
    }
    if (value instanceof Point point) {
      return new OpenSearchExprGeoPointValue(point.getY(), point.getX());
    }
    if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      Map<String, Object> convertedMap = new HashMap<>();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        convertedMap.put(entry.getKey(), processValue(entry.getValue(), null));
      }
      return convertedMap;
    }
    if (value instanceof StructImpl structImpl) {
      Object[] attrs = structImpl.getAttributes();
      if (type != null && type.getSqlTypeName() == SqlTypeName.ROW) {
        List<RelDataTypeField> fields = type.getFieldList();
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < fields.size() && i < attrs.length; i++) {
          map.put(fields.get(i).getName(), processValue(attrs[i], fields.get(i).getType()));
        }
        return map;
      }
      return Arrays.asList(attrs);
    }
    if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      RelDataType componentType =
          (type != null && type.getComponentType() != null) ? type.getComponentType() : null;
      List<Object> convertedList = new ArrayList<>();
      for (Object item : list) {
        convertedList.add(processValue(item, componentType));
      }
      return convertedList;
    }
    // For other types, return as-is
    return value;
  }

  private QueryResponse buildResultSet(
      ResultSet resultSet, RelDataType rowTypes, Integer querySizeLimit) throws SQLException {
    // Get the ResultSet metadata to know about columns
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<RelDataType> fieldTypes =
        rowTypes.getFieldList().stream().map(RelDataTypeField::getType).toList();
    List<ExprValue> values = new ArrayList<>();
    // Iterate through the ResultSet
    while (resultSet.next() && (querySizeLimit == null || values.size() < querySizeLimit)) {
      Map<String, ExprValue> row = new LinkedHashMap<String, ExprValue>();
      // Loop through each column
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        Object value = resultSet.getObject(columnName);
        Object converted = processValue(value, fieldTypes.get(i - 1));
        ExprValue exprValue = ExprValueUtils.fromObjectValue(converted);
        row.put(columnName, exprValue);
      }
      values.add(ExprTupleValue.fromExprValueMap(row));
    }

    List<Column> columns = new ArrayList<>(metaData.getColumnCount());
    for (int i = 1; i <= columnCount; ++i) {
      String columnName = metaData.getColumnName(i);
      RelDataType fieldType = fieldTypes.get(i - 1);
      // TODO: Correct this after fixing issue github.com/opensearch-project/sql/issues/3751
      //  The element type of struct and array is currently set to ANY.
      //  We set them using the runtime type as a workaround.
      ExprType exprType;
      if (fieldType.getSqlTypeName() == SqlTypeName.ANY) {
        if (!values.isEmpty()) {
          exprType = values.getFirst().tupleValue().get(columnName).type();
        } else {
          // Using UNDEFINED instead of UNKNOWN to avoid throwing exception
          exprType = ExprCoreType.UNDEFINED;
        }
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);
      }
      columns.add(new Column(columnName, null, exprType));
    }
    // Timewrap post-processing: pivot unpivoted rows into period columns. The pivot is shared with
    // the analytics route (AnalyticsExecutionEngine) so both engines produce identical output.
    if (TimewrapPivot.isTimewrap()) {
      try {
        TimewrapPivot.Result pivoted =
            TimewrapPivot.pivot(
                columns,
                values,
                CalcitePlanContext.timewrapUnitName.get(),
                CalcitePlanContext.timewrapSeries.get());
        columns = pivoted.columns();
        values = pivoted.values();
      } finally {
        CalcitePlanContext.clearTimewrapSignals();
      }
    }

    Schema schema = new Schema(columns);
    QueryResponse response = new QueryResponse(schema, values, null);
    return response;
  }

  /** Registers opensearch-dependent functions */
  private void registerOpenSearchFunctions() {
    Optional<NodeClient> nodeClient = client.getNodeClient();
    if (nodeClient.isPresent()) {
      SqlUserDefinedFunction geoIpFunction =
          new GeoIpFunction(nodeClient.get()).toUDF(BuiltinFunctionName.GEOIP.name());
      PPLFuncImpTable.INSTANCE.registerExternalOperator(BuiltinFunctionName.GEOIP, geoIpFunction);
      OperatorTable.addOperator(BuiltinFunctionName.GEOIP.name(), geoIpFunction);
    } else {
      logger.info(
          "Function [GEOIP] not registered: incompatible client type {}",
          client.getClass().getName());
    }

    SqlUserDefinedAggFunction approxDistinctCountFunction =
        UserDefinedFunctionUtils.createUserDefinedAggFunction(
            DistinctCountApproxAggFunction.class,
            BuiltinFunctionName.DISTINCT_COUNT_APPROX.name(),
            ReturnTypes.BIGINT_FORCE_NULLABLE,
            null);
    PPLFuncImpTable.INSTANCE.registerExternalAggOperator(
        BuiltinFunctionName.DISTINCT_COUNT_APPROX, approxDistinctCountFunction);
    OperatorTable.addOperator(
        BuiltinFunctionName.DISTINCT_COUNT_APPROX.name(), approxDistinctCountFunction);

    // Note: GraphLookup is now implemented as a custom RelNode (LogicalGraphLookup)
    // instead of a UDF, so no registration is needed here.
  }

  /**
   * Dynamic SqlOperatorTable that allows adding operators after initialization. Similar to
   * PPLBuiltinOperator.instance() or SqlStdOperatorTable.instance().
   */
  public static class OperatorTable extends ListSqlOperatorTable {
    private static final Supplier<OperatorTable> INSTANCE =
        Suppliers.memoize(() -> (OperatorTable) new OperatorTable().init());
    // Use map instead of list to avoid duplicated elements if the class is initialized multiple
    // times
    private static final Map<String, SqlOperator> operators = new ConcurrentHashMap<>();

    public static SqlOperatorTable instance() {
      return INSTANCE.get();
    }

    private ListSqlOperatorTable init() {
      setOperators(buildIndex(operators.values()));
      return this;
    }

    public static synchronized void addOperator(String name, SqlOperator operator) {
      operators.put(name, operator);
    }
  }
}
