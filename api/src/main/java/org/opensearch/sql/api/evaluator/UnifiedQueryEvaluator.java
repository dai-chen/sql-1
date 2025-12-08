/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.evaluator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryType;

/**
 * {@code UnifiedQueryEvaluator} provides a high-level API for evaluating PPL queries against any
 * catalog, returning results in an engine-agnostic row model format. It serves as a reference
 * runtime implementation built on Apache Calcite, enabling CLI tools, embedded query engines, and
 * conformance test suites to execute PPL queries without directly coupling to OpenSearch or Calcite
 * internals.
 *
 * <p>The evaluator follows the same design patterns as {@link UnifiedQueryPlanner} and {@link
 * org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler}, providing a consistent fluent builder
 * API and integrating seamlessly with the Unified Query API layer.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
 *     .language(QueryType.PPL)
 *     .catalog("my_catalog", schema)
 *     .defaultNamespace("my_catalog")
 *     .build();
 *
 * UnifiedQueryEvaluator evaluator = UnifiedQueryEvaluator.builder()
 *     .planner(planner)
 *     .build();
 *
 * ExecutionEngine.QueryResponse response = evaluator.evaluate("source my_table | fields name, age");
 * }</pre>
 */
@Builder
public class UnifiedQueryEvaluator {

  /** The planner instance used for parsing and planning queries. */
  private final UnifiedQueryPlanner planner;

  /**
   * Evaluates a PPL query string and returns results in an engine-agnostic format.
   *
   * <p>The evaluation process:
   *
   * <ol>
   *   <li>Parses the query string using the configured planner
   *   <li>Plans the query into a Calcite logical plan (RelNode)
   *   <li>Executes the plan using Calcite's execution engine
   *   <li>Converts results to the engine-agnostic QueryResponse format
   * </ol>
   *
   * @param query the raw query string in PPL syntax
   * @return a QueryResponse containing schema and result rows
   * @throws org.opensearch.sql.common.antlr.SyntaxCheckException if the query has syntax errors
   * @throws IllegalStateException if semantic or runtime errors occur during execution
   */
  public ExecutionEngine.QueryResponse evaluate(String query) {
    try {
      // Step 1: Parse and plan the query to get RelNode
      RelNode relNode = planner.plan(query);

      // Step 2: Create CalcitePlanContext for execution
      // Use a large query size limit since this is a reference implementation
      CalcitePlanContext context =
          CalcitePlanContext.create(
              planner.getConfig(), new SysLimit(10000, 10000, 10000), QueryType.PPL);

      // Step 3: Execute the RelNode using Calcite's execution engine
      try (PreparedStatement statement =
          CalciteToolsHelper.OpenSearchRelRunners.run(context, relNode)) {
        ResultSet resultSet = statement.executeQuery();

        // Step 4: Convert ResultSet to QueryResponse
        return buildQueryResponse(resultSet, relNode.getRowType());
      }
    } catch (SyntaxCheckException e) {
      // Re-throw syntax errors without wrapping
      throw e;
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to execute query: " + e.getMessage(), e);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to evaluate query", e);
    }
  }

  /**
   * Builds a QueryResponse from a JDBC ResultSet and Calcite row type information. This method
   * follows the same pattern as OpenSearchExecutionEngine.buildResultSet.
   *
   * @param resultSet the JDBC result set from query execution
   * @param rowType the Calcite row type containing field metadata
   * @return a QueryResponse with schema and result rows
   * @throws SQLException if result set access fails
   */
  private ExecutionEngine.QueryResponse buildQueryResponse(ResultSet resultSet, RelDataType rowType)
      throws SQLException {

    // Get metadata from ResultSet
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<RelDataType> fieldTypes =
        rowType.getFieldList().stream().map(RelDataTypeField::getType).toList();

    // Convert all rows to ExprValue list
    List<ExprValue> values = new ArrayList<>();
    while (resultSet.next()) {
      Map<String, ExprValue> row = new LinkedHashMap<>();

      // Loop through each column and convert to ExprValue
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        int sqlType = metaData.getColumnType(i);
        RelDataType fieldType = fieldTypes.get(i - 1);

        // Use the same conversion logic as OpenSearchExecutionEngine
        ExprValue exprValue =
            convertJdbcValueToExprValue(resultSet, i, sqlType, fieldType, columnName);
        row.put(columnName, exprValue);
      }
      values.add(ExprTupleValue.fromExprValueMap(row));
    }

    // Build schema from metadata
    List<ExecutionEngine.Schema.Column> columns = new ArrayList<>(columnCount);
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      RelDataType fieldType = fieldTypes.get(i - 1);

      // Convert Calcite type to ExprType
      ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);

      // Column alias is null for reference implementation
      columns.add(new ExecutionEngine.Schema.Column(columnName, null, exprType));
    }

    ExecutionEngine.Schema schema = new ExecutionEngine.Schema(columns);

    // Return QueryResponse with null cursor (not needed for reference implementation)
    return new ExecutionEngine.QueryResponse(schema, values, null);
  }

  /**
   * Converts a JDBC value to an ExprValue. This is a simplified version that delegates to the
   * existing conversion utilities where possible.
   *
   * @param resultSet the result set
   * @param columnIndex the column index (1-based)
   * @param sqlType the JDBC SQL type
   * @param fieldType the Calcite field type
   * @param columnName the column name
   * @return the corresponding ExprValue
   * @throws SQLException if result set access fails
   */
  private ExprValue convertJdbcValueToExprValue(
      ResultSet resultSet, int columnIndex, int sqlType, RelDataType fieldType, String columnName)
      throws SQLException {

    Object value = resultSet.getObject(columnIndex);
    if (value == null) {
      return org.opensearch.sql.data.model.ExprNullValue.of();
    }

    // Get the ExprType for this field
    ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);

    // Use OpenSearchTypeFactory to convert the value
    return OpenSearchTypeFactory.getExprValueByExprType(exprType, value);
  }
}
