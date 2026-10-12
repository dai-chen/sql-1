/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import com.google.common.base.Suppliers;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.index.shard.DocsStats;
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
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.functions.DistinctCountApproxAggFunction;
import org.opensearch.sql.opensearch.functions.GeoIpFunction;
import org.opensearch.sql.opensearch.stage.CalciteStageExecutor;
import org.opensearch.sql.opensearch.stage.StagePlan;
import org.opensearch.sql.opensearch.stage.StagePlanner;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.transport.client.node.NodeClient;
import tools.jackson.databind.ObjectMapper;

/** OpenSearch execution engine implementation. */
public class OpenSearchExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(OpenSearchExecutionEngine.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final OpenSearchClient client;
  private final CalciteStageExecutor stageExecutor;
  private final ExecutionProtector executionProtector;
  private final PlanSerializer planSerializer;
  private final Settings settings;

  public OpenSearchExecutionEngine(
      OpenSearchClient client,
      ExecutionProtector executionProtector,
      PlanSerializer planSerializer) {
    this(client, executionProtector, planSerializer, null);
  }

  public OpenSearchExecutionEngine(
      OpenSearchClient client,
      ExecutionProtector executionProtector,
      PlanSerializer planSerializer,
      Settings settings) {
    this.client = client;
    this.executionProtector = executionProtector;
    this.planSerializer = planSerializer;
    this.settings = settings;
    this.stageExecutor = new CalciteStageExecutor(client);
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
            if (format != Format.JSON_TREE && mode != ExplainMode.SIMPLE) {
              StagePlan stagePlan = splitIfStaged(rel);
              if (stagePlan != null) {
                SqlExplainLevel level =
                    mode == ExplainMode.COST
                        ? SqlExplainLevel.ALL_ATTRIBUTES
                        : SqlExplainLevel.EXPPLAN_ATTRIBUTES;
                listener.onResponse(
                    new ExplainResponse(
                        new ExplainResponseNodeV2(
                            RelOptUtil.toString(rel, level),
                            CalciteStageExecutor.formatPhysicalPlan(stagePlan, level),
                            null)));
                return;
              }
            }
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
            StagePlan stagePlan = splitIfStaged(rel);
            if (stagePlan != null) {
              executeStagedPlan(stagePlan, context, listener);
              return;
            }
          } catch (Exception e) {
            listener.onFailure(e);
            return;
          }

          try (PreparedStatement statement = OpenSearchRelRunners.run(context, rel)) {
            QueryResponse response;
            try (ProfileScope executePhase = ProfileScope.open(MetricName.EXECUTE)) {
              ResultSet result = statement.executeQuery();
              response =
                  buildResultSet(result, rel.getRowType(), context.sysLimit.querySizeLimit());
            }
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

  private StagePlan splitIfStaged(RelNode rel) {
    boolean analyticsEnabled = booleanSetting(Settings.Key.CALCITE_ANALYTICS_ENABLED, false);
    if (!analyticsEnabled) {
      return null;
    }

    long minDocs = longSetting(Settings.Key.CALCITE_ANALYTICS_ROUTING_MIN_DOCS, 100_000L);
    StagePlan plan = StagePlanner.split(rel).orElse(null);
    if (plan == null || !plan.residualCoordinatorWork()) {
      logRouting("calcite", "LEGACY_FULLY_PUSHDOWN", "FULLY_PUSHDOWN", null, minDocs);
      return null;
    }
    if (!plan.allShardFragmentsReduce()) {
      logRouting(
          "calcite", "LEGACY_NO_SHARD_REDUCTION", "RESIDUAL_COORDINATOR_WORK", null, minDocs);
      return null;
    }

    Long primaryDocs = primaryDocumentCount(plan);
    if (primaryDocs == null) {
      logRouting("calcite", "LEGACY_UNKNOWN_INPUT", "RESIDUAL_COORDINATOR_WORK", null, minDocs);
      return null;
    }
    if (primaryDocs < minDocs) {
      logRouting(
          "calcite", "LEGACY_SMALL_INPUT", "RESIDUAL_COORDINATOR_WORK", primaryDocs, minDocs);
      return null;
    }

    logRouting(
        "analytics",
        "ANALYTICS_COORDINATOR_WORK",
        "RESIDUAL_COORDINATOR_WORK",
        primaryDocs,
        minDocs);
    return plan;
  }

  private boolean booleanSetting(Settings.Key key, boolean defaultValue) {
    Object value = settingValue(key);
    return value instanceof Boolean bool ? bool : defaultValue;
  }

  private long longSetting(Settings.Key key, long defaultValue) {
    Object value = settingValue(key);
    return value instanceof Number number ? number.longValue() : defaultValue;
  }

  private Object settingValue(Settings.Key key) {
    if (settings != null) {
      return settings.getSettingValue(key);
    }
    return null;
  }

  private Long primaryDocumentCount(StagePlan plan) {
    try {
      Set<String> indices = new LinkedHashSet<>();
      for (StagePlan.GatherExchange exchange : plan.exchanges()) {
        indices.addAll(
            List.of(exchange.source().scan().getOsIndex().getIndexName().getIndexNames()));
      }
      if (indices.isEmpty()) {
        return null;
      }

      IndicesStatsRequest request = new IndicesStatsRequest();
      request.indices(indices.toArray(String[]::new));
      request.clear();
      request.docs(true);
      IndicesStatsResponse response =
          client.getNodeClient().orElseThrow().admin().indices().stats(request).actionGet();
      if (response.getFailedShards() != 0) {
        return null;
      }
      long total = 0;
      int countedIndices = 0;
      for (IndexStats indexStats : response.getIndices().values()) {
        CommonStats primaries = indexStats.getPrimaries();
        DocsStats docs = primaries == null ? null : primaries.getDocs();
        if (docs != null) {
          total = Math.addExact(total, docs.getCount());
          countedIndices++;
        }
      }
      return countedIndices == 0 ? null : total;
    } catch (Exception e) {
      logger.debug("Unable to fetch primary document count for calcite_exec routing", e);
      return null;
    }
  }

  private static void logRouting(
      String route, String reason, String shape, Long primaryDocs, long minDocs) {
    logger.info(
        "Engine routing: route={}, reason={}, legacy_shape={}, primary_docs={}, min_docs={}",
        route,
        reason,
        shape,
        primaryDocs == null ? "unknown" : primaryDocs,
        minDocs);
  }

  private void executeStagedPlan(
      StagePlan stagePlan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    try {
      ProfileMetric metric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
      long execTime = System.nanoTime();
      long currentTimeNanos = Hook.CURRENT_TIME.get(-1L);
      if (currentTimeNanos < 0) {
        throw new IllegalStateException("Staged execution requires a query start time");
      }
      RelNode coordinatorTree = stagePlan.coordinatorTree();
      List<Object[]> outputRows = stageExecutor.execute(stagePlan, currentTimeNanos);

      RelDataType outputType = coordinatorTree.getRowType();
      Integer querySizeLimit = context.sysLimit.querySizeLimit();

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
          tuple.put(fieldName, stagedExprValue(value, exprTypes[i]));
        }
        values.add(ExprTupleValue.fromExprValueMap(tuple));
      }

      List<Column> columns = new ArrayList<>();
      for (int i = 0; i < fieldCount; i++) {
        columns.add(new Column(outputFields.get(i).getName(), null, exprTypes[i]));
      }
      metric.add(System.nanoTime() - execTime);
      QueryResponse response = buildQueryResponse(columns, values);
      response.setEngine("analytics");
      listener.onResponse(response);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private static ExprValue stagedExprValue(Object value, ExprType type) {
    if (value == null) {
      return ExprValueUtils.LITERAL_NULL;
    }
    Long epochMillis = epochMillis(value);
    if (epochMillis != null) {
      if (type == ExprCoreType.TIMESTAMP) {
        return ExprValueUtils.timestampValue(java.time.Instant.ofEpochMilli(epochMillis));
      }
      if (type == ExprCoreType.DATE) {
        return ExprValueUtils.dateValue(
            java.time.Instant.ofEpochMilli(epochMillis)
                .atZone(java.time.ZoneOffset.UTC)
                .toLocalDate());
      }
      if (type == ExprCoreType.TIME) {
        return ExprValueUtils.timeValue(
            java.time.Instant.ofEpochMilli(epochMillis)
                .atZone(java.time.ZoneOffset.UTC)
                .toLocalTime());
      }
    }
    return ExprValueUtils.fromObjectValue(value, type);
  }

  private static Long epochMillis(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String string) {
      try {
        return Long.parseLong(string);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
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
    return buildQueryResponse(columns, values);
  }

  private static QueryResponse buildQueryResponse(List<Column> columns, List<ExprValue> values) {
    var warnings = CalcitePlanContext.drainWarnings();
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

    QueryResponse response = new QueryResponse(new Schema(columns), values, null);
    response.setWarnings(warnings);
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
