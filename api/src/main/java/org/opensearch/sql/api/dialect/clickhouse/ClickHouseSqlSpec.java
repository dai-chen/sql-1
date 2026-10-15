/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.opensearch.sql.api.dialect.clickhouse.parser.ClickHouseSqlParserImpl;
import org.opensearch.sql.api.spec.LanguageSpec;
import org.opensearch.sql.api.spec.datetime.DatetimeExtension;

/**
 * ClickHouse SQL dialect (PoC), selected only by an explicit {@code dialect=clickhouse} request.
 *
 * <ul>
 *   <li>Parser: {@link ClickHouseSqlParserImpl}, generated from the Calcite template pinned to the
 *       Analytics Engine runtime, extended with {@code PREWHERE}.
 *   <li>Lexical rules: backtick quoting and case-sensitive, case-preserving identifiers, as in
 *       ClickHouse.
 *   <li>Extensions: ClickHouse functions, the query guard, plan validation and OpenSearch datetime
 *       normalization.
 * </ul>
 */
public class ClickHouseSqlSpec implements LanguageSpec {

  public static final String DIALECT = "clickhouse";

  /**
   * LENIENT, plus "liberal" so the validator accepts LEFT SEMI / LEFT ANTI JOIN. Full BABEL is not
   * used: it wraps grouping-set keys in ANY_VALUE, which the Analytics Engine does not support.
   */
  private static final SqlConformance CONFORMANCE =
      new SqlDelegatingConformance(SqlConformanceEnum.LENIENT) {
        @Override
        public boolean isLiberal() {
          return true;
        }

        // ClickHouse strings are variable-length: CASE/UNION of literals must not pad to CHAR(n).
        @Override
        public boolean shouldConvertRaggedUnionTypesToVarying() {
          return true;
        }
      };

  private final List<LanguageExtension> extensions =
      List.of(new ClickHouseExtension(), new DatetimeExtension());

  public static ClickHouseSqlSpec create() {
    return new ClickHouseSqlSpec();
  }

  @Override
  public SqlParser.Config parserConfig() {
    return SqlParser.config()
        .withParserFactory(ClickHouseSqlParserImpl.FACTORY)
        .withQuoting(Quoting.BACK_TICK)
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuotedCasing(Casing.UNCHANGED)
        .withCaseSensitive(true)
        .withConformance(SqlConformanceEnum.LENIENT);
  }

  @Override
  public SqlValidator.Config validatorConfig() {
    return SqlValidator.Config.DEFAULT.withConformance(CONFORMANCE);
  }

  @Override
  public List<LanguageExtension> extensions() {
    return extensions;
  }

  /**
   * Standard operators, plus only the Calcite library operator tables that the ClickHouse alias
   * table targets (dialect-local; default SQL and PPL operator tables are unchanged), plus the
   * ClickHouse operators.
   */
  @Override
  public SqlOperatorTable operatorTable() {
    List<SqlOperatorTable> tables = new ArrayList<>();
    tables.add(SqlStdOperatorTable.instance());
    if (!ClickHouseFunctionAliasRule.LIBRARIES.isEmpty()) {
      tables.add(
          SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
              ClickHouseFunctionAliasRule.LIBRARIES));
    }
    tables.add(SqlOperatorTables.of(ClickHouseFunctionAliasRule.boundOperators()));
    extensions.forEach(ext -> tables.add(ext.operators()));
    return SqlOperatorTables.chain(tables);
  }
}
