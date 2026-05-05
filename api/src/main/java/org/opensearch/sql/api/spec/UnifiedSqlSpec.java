/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.opensearch.sql.api.spec.core.CoreExtension;
import org.opensearch.sql.api.spec.search.SearchExtension;

/**
 * SQL language specification. Configures Calcite's parser, validator, and composable extensions for
 * OpenSearch SQL compatibility.
 *
 * <p>Use {@link #extended()} for the default configuration with lenient syntax, hyphenated
 * identifiers, and search functions.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Accessors(fluent = true)
public class UnifiedSqlSpec implements LanguageSpec {

  /**
   * BABEL conformance with strict GROUP BY validation. Delegates all behavior to BABEL except
   * isNonStrictGroupBy which returns false — preventing the validator from wrapping non-grouped
   * expressions in ANY_VALUE and avoiding a NPE on CASE expressions in GROUP BY.
   */
  private static final SqlConformance BABEL_STRICT_GROUP_BY =
      new SqlDelegatingConformance(SqlConformanceEnum.BABEL) {
        @Override
        public boolean isNonStrictGroupBy() {
          return false;
        }
      };

  /** LENGTH — reuse Calcite's built-in (alias for CHAR_LENGTH). */
  private static final SqlFunction LENGTH = SqlLibraryOperators.LENGTH;

  /** REGEXP_REPLACE(string, pattern, replacement) — reuse Calcite's built-in. */
  private static final SqlFunction REGEXP_REPLACE = SqlLibraryOperators.REGEXP_REPLACE_3;

  /** DATE_TRUNC(unit_string, timestamp) — PostgreSQL/Spark convention (unit first). */
  private static final SqlFunction DATE_TRUNC =
      new SqlFunction(
          "DATE_TRUNC",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG1_NULLABLE,
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME),
          SqlFunctionCategory.TIMEDATE);

  /** Lexical rules: identifier quoting, character escaping, and special identifier support. */
  private final Lex lex;

  /** Parser implementation: controls keyword reservation and grammar extensions. */
  private final SqlParserImplFactory parserFactory;

  /** Validation rules: what SQL semantics the validator accepts (GROUP BY, LIMIT, coercion). */
  private final SqlConformanceEnum conformance;

  /** Composable extensions contributing operators and post-parse rewrite rules. */
  @Getter private final List<LanguageExtension> extensions;

  /**
   * Extended SQL spec: Babel parser, BIG_QUERY lex (hyphenated identifiers, backtick quoting),
   * BABEL conformance (lenient GROUP BY, LIMIT, optional FROM), and search functions.
   */
  public static UnifiedSqlSpec extended() {
    return new UnifiedSqlSpec(
        Lex.BIG_QUERY,
        SqlBabelParserImpl.FACTORY,
        SqlConformanceEnum.BABEL,
        List.of(new CoreExtension(), new SearchExtension()));
  }

  @Override
  public SqlParser.Config parserConfig() {
    return SqlParser.config()
        .withParserFactory(parserFactory)
        .withLex(lex)
        .withConformance(conformance);
  }

  @Override
  public SqlOperatorTable operatorTable() {
    List<SqlOperatorTable> tables = new ArrayList<>();
    tables.add(SqlStdOperatorTable.instance());
    tables.add(SqlOperatorTables.of(LENGTH, DATE_TRUNC, REGEXP_REPLACE));
    extensions().forEach(ext -> tables.add(ext.operators()));
    return SqlOperatorTables.chain(tables);
  }

  @Override
  public SqlValidator.Config validatorConfig() {
    return SqlValidator.Config.DEFAULT.withConformance(BABEL_STRICT_GROUP_BY);
  }
}
