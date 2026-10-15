/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.List;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlVisitor;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * ClickHouse dialect extension.
 *
 * <ul>
 *   <li>operators: ClickHouse functions ({@link ClickHouseOperators});
 *   <li>post-parse: {@link ClickHouseQueryGuard}, DQL-only checks that run before the validator
 *       resolves any table against the OpenSearch schema;
 *   <li>post-analysis: {@link ClickHouseFunctionExpansionRule} lowers ClickHouse functions to
 *       operators the Analytics Engine executes, then {@link ClickHousePlanValidator} checks the
 *       final logical plan (tables, operators, relational nodes).
 * </ul>
 */
public class ClickHouseExtension implements LanguageSpec.LanguageExtension {

  @Override
  public SqlOperatorTable operators() {
    return ClickHouseOperators.table();
  }

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    return List.of(
        new ClickHouseQueryGuard(),
        new ClickHouseCountRule(),
        new ClickHouseAggregateRule(),
        new ClickHouseFunctionAliasRule());
  }

  @Override
  public List<RelShuttle> postAnalysisRules() {
    return List.of(new ClickHouseFunctionExpansionRule(), new ClickHousePlanValidator());
  }
}
