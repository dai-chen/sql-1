/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.routing;

import java.util.Map;
import java.util.Set;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.opensearch.analytics.plan.BoundaryScan;
import org.opensearch.analytics.plan.UnsupportedFunctionValidator;
import org.opensearch.analytics.schema.ParquetTable;
import org.opensearch.analytics.spi.ParquetEngineCapabilities;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

public class ParquetQueryPlanner {

  private ParquetQueryPlanner() {}

  private static AbstractSchema createParquetSchema() {
    return new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return Map.of("parquet_index", new ParquetTable());
      }
    };
  }

  public static RelNode plan(String pplQuery) throws Exception {
    AbstractSchema schema = createParquetSchema();
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("parquet", schema)
            .defaultNamespace("parquet")
            .build();
    try {
      RelNode relNode = new UnifiedQueryPlanner(context).plan(pplQuery);
      UnsupportedFunctionValidator.validate(relNode, new ParquetEngineCapabilities());
      return optimize(replaceScanWithBoundary(relNode));
    } finally {
      context.close();
    }
  }

  private static final Set<String> SEARCH_FUNCTIONS =
      Set.of(
          "match",
          "match_phrase",
          "match_bool_prefix",
          "match_phrase_prefix",
          "multi_match",
          "simple_query_string",
          "query_string");

  private static SqlOperatorTable createSearchFunctionTable() {
    ListSqlOperatorTable table = new ListSqlOperatorTable();
    SEARCH_FUNCTIONS.forEach(
        name ->
            table.add(
                new SqlFunction(
                    name,
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    null,
                    null,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION)));
    return table;
  }

  public static RelNode planSql(String sqlQuery) throws Exception {
    AbstractSchema schema = createParquetSchema();
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    SchemaPlus parquetSchema = rootSchema.add("parquet", schema);

    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
            .defaultSchema(parquetSchema)
            .operatorTable(
                SqlOperatorTables.chain(
                    createSearchFunctionTable(), SqlStdOperatorTable.instance()))
            .build();
    Planner planner = Frameworks.getPlanner(config);
    try {
      SqlNode parsed;
      try {
        parsed = planner.parse(sqlQuery);
      } catch (SqlParseException e) {
        String lower = sqlQuery.toLowerCase();
        for (String fn : SEARCH_FUNCTIONS) {
          if (lower.contains(fn + "(")) {
            throw new UnsupportedOperationException(
                "Function '" + fn + "' is not supported for Parquet indices");
          }
        }
        throw e;
      }
      SqlNode validated = planner.validate(parsed);
      RelRoot relRoot = planner.rel(validated);
      RelNode relNode = relRoot.rel;
      UnsupportedFunctionValidator.validate(relNode, new ParquetEngineCapabilities());
      return optimize(replaceScanWithBoundary(relNode));
    } finally {
      planner.close();
    }
  }

  public static String formatExplain(RelNode relNode) {
    String plan = RelOptUtil.toString(relNode);
    return "{\"Parquet\":{\"plan\":\""
        + plan.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
        + "\"}}";
  }

  private static RelNode replaceScanWithBoundary(RelNode relNode) {
    return relNode.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(TableScan scan) {
            return BoundaryScan.create(
                scan.getCluster(), scan.getTable(), new ParquetEngineCapabilities());
          }
        });
  }

  private static RelNode optimize(RelNode relNode) {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.FilterAbsorptionRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.ProjectAbsorptionRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.SortAbsorptionRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.AggregateAbsorptionRule.Config.DEFAULT.toRule());
    HepPlanner planner = new HepPlanner(builder.build());
    planner.setRoot(relNode);
    return planner.findBestExp();
  }
}
