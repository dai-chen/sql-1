/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.executor.QueryType;

/** Planner-level (no cluster) tests for the dialect=clickhouse Calcite-native path. */
public class ClickHouseDialectPlannerTest extends UnifiedQueryTestBase {

  private final AtomicInteger tableLookups = new AtomicInteger();

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Before
  @Override
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            tableLookups.incrementAndGet();
            return Map.of(
                "logs",
                SimpleTable.builder()
                    .col("id", INTEGER)
                    .col("svc", VARCHAR)
                    .col("msg", VARCHAR)
                    .col("ts", TIMESTAMP)
                    .build());
          }
        };
    context = contextBuilder().build();
    planner = new UnifiedQueryPlanner(context);
  }

  @Override
  protected UnifiedQueryContext.Builder contextBuilder() {
    return UnifiedQueryContext.builder()
        .language(QueryType.SQL)
        .dialect(ClickHouseSqlSpec.DIALECT)
        .catalog("opensearch", testSchema)
        .defaultNamespace("opensearch");
  }

  private String plan(String sql) {
    RelNode rel = planner.plan(sql);
    return RelOptUtil.toString(rel);
  }

  private String rejection(String sql) {
    return assertThrows(Exception.class, () -> planner.plan(sql)).getMessage();
  }

  @Test
  public void prewhereIsMergedIntoWhere() {
    String p = plan("SELECT id FROM logs PREWHERE svc = 'api' WHERE id > 3");
    assertThat(p, containsString("AND(=($1, 'api'), >($0, 3))"));
  }

  @Test
  public void prewhereAlone() {
    assertThat(plan("SELECT id FROM logs PREWHERE id > 3"), containsString(">($0, 3)"));
  }

  @Test
  public void existingFunctionAlias() {
    String p = plan("SELECT lengthUTF8(msg) AS n FROM logs");
    assertThat(p, containsString("CHAR_LENGTH($2)"));
    assertThat(p, not(containsString("lengthUTF8")));
  }

  @Test
  public void newFunctionIsExpanded() {
    String p = plan("SELECT toYYYYMM(ts) AS ym FROM logs");
    assertThat(p, containsString("EXTRACT(FLAG(YEAR), $3)"));
    assertThat(p, containsString("EXTRACT(FLAG(MONTH), $3)"));
  }

  @Test
  public void concatOverridePropagatesNull() {
    String p = plan("SELECT concat(svc, msg) AS c FROM logs");
    assertThat(p, containsString("CASE(OR(IS NULL($1), IS NULL($2)), null:VARCHAR"));
    assertThat(p, containsString("||($1, $2)"));
  }

  @Test
  public void zeroArgCountMeansCountStar() {
    assertThat(plan("SELECT count() AS c FROM logs"), containsString("COUNT()"));
  }

  @Test
  public void aggregateCombinators() {
    assertThat(plan("SELECT countIf(id > 1) AS c FROM logs"), containsString("COUNT($0)"));
    assertThat(
        plan("SELECT sumIf(id, svc = 'a') AS s FROM logs"),
        containsString("CASE(=($1, 'a'), $0, null:INTEGER)"));
    assertThat(plan("SELECT sumDistinct(id) AS s FROM logs"), containsString("SUM(DISTINCT $0)"));
    assertThat(plan("SELECT uniqExact(svc) AS u FROM logs"), containsString("COUNT(DISTINCT $0)"));
    assertThat(plan("SELECT argMax(svc, id) AS a FROM logs"), containsString("ARG_MAX($0, $1)"));
    assertThat(
        plan("SELECT maxOrNull(id) AS m FROM logs"),
        containsString("CASE(=($0, 0), null:INTEGER, $1)"));
    assertThat(plan("SELECT corr(id, id) AS r FROM logs"), containsString("COVAR_POP"));
  }

  @Test
  public void functionNamesAreCaseSensitive() {
    assertThat(rejection("SELECT LENGTHUTF8(msg) FROM logs"), containsString("LENGTHUTF8"));
  }

  @Test
  public void distributableShapesPlan() {
    assertThat(
        plan("SELECT svc, count(*) AS c FROM logs GROUP BY svc ORDER BY c DESC LIMIT 2"),
        containsString("LogicalAggregate"));
  }

  @Test
  public void rejectsWrites() {
    assertThat(rejection("INSERT INTO logs (id) VALUES (1)"), containsString("Only query"));
    assertThat(rejection("DELETE FROM logs WHERE id = 1"), containsString("Only query"));
    assertThat(rejection("UPDATE logs SET id = 2"), containsString("Only query"));
  }

  @Test
  public void rejectsUnsupportedStatements() {
    // DDL, COPY and USE are not in the grammar at all.
    rejection("CREATE TABLE x (a INT)");
    rejection("COPY logs TO '/tmp/x'");
    rejection("USE other");
    assertThat(rejection("ALTER SESSION SET x = 1"), containsString("Only query"));
    assertThat(rejection("EXPLAIN PLAN FOR SELECT id FROM logs"), containsString("Only query"));
  }

  @Test
  public void rejectsMultipleStatements() {
    rejection("SELECT id FROM logs; SELECT id FROM logs");
  }

  @Test
  public void rejectsTableFunctionsAndRemoteOrSystemTables() {
    assertThat(rejection("SELECT * FROM TABLE(ramp(3))"), containsString("not allowed"));
    assertThat(rejection("SELECT * FROM `.kibana`"), containsString("system or hidden"));
    assertThat(rejection("SELECT * FROM `remote:logs`"), containsString("remote"));
    assertThat(rejection("SELECT * FROM `logs*`"), containsString("pattern"));
    assertThat(rejection("SELECT * FROM metadata.tables"), containsString("catalog"));
  }

  @Test
  public void guardRunsBeforeProviderResolution() {
    tableLookups.set(0);
    rejection("SELECT * FROM `.kibana`");
    rejection("SELECT * FROM logs JOIN `remote:logs` r ON true");
    assertEquals("no table was resolved for rejected queries", 0, tableLookups.get());
  }

  @Test
  public void rejectsUnapprovedFunctions() {
    assertThat(rejection("SELECT CURRENT_USER FROM logs"), containsString("not approved"));
    assertThat(rejection("SELECT RAND() FROM logs"), containsString("not approved"));
  }

  @Test
  public void unknownTableIsAValidationError() {
    assertThat(rejection("SELECT * FROM missing_index"), containsString("missing_index"));
  }

  @Test
  public void defaultSqlStillUsesV2Grammar() throws Exception {
    try (UnifiedQueryContext v2 =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("opensearch", testSchema)
            .defaultNamespace("opensearch")
            .build()) {
      UnifiedQueryPlanner v2Planner = new UnifiedQueryPlanner(v2);
      assertThrows(Exception.class, () -> v2Planner.plan("SELECT id FROM logs PREWHERE id > 3"));
    }
  }
}
