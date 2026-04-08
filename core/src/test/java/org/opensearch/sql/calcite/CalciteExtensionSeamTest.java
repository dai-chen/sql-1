/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.extension.SearchExtension;
import org.opensearch.sql.calcite.preprocess.QueryPreprocessor;

/**
 * PoC test harness for Unified SQL by Extension.
 * Validates that Calcite's extension seams accept OpenSearch-specific SQL syntax.
 *
 * <p>Phase 1: Tests seams that don't require custom grammar (FMPP).
 * Phase 2 (TODO): Custom grammar for named params, array literals, Flint DDL.
 */
public class CalciteExtensionSeamTest {

  private static SearchExtension searchExtension;
  private static SqlParser.Config parserConfig;

  @BeforeAll
  static void setUp() {
    searchExtension = new SearchExtension();
    parserConfig = searchExtension.extendParserConfig(SqlParser.config());
  }

  private SqlNode parse(String sql) throws SqlParseException {
    String preprocessed = searchExtension.preprocessor().apply(sql);
    SqlParser parser = SqlParser.create(preprocessed, parserConfig);
    return parser.parseStmtList().get(0);
  }

  // === Seam 8: Query Pre-processor ===

  @Nested
  @DisplayName("Seam 8: Query Pre-processor")
  class PreprocessorTests {

    @Test
    @DisplayName("Cat 0: Auto-quote hyphenated table identifier")
    void autoQuoteHyphenatedIdentifier() {
      String input = "SELECT * FROM my-index";
      String result = QueryPreprocessor.process(input);
      assertEquals("SELECT * FROM `my-index`", result);
    }

    @Test
    @DisplayName("Cat 0: Auto-quote dot-separated table identifier")
    void autoQuoteDotSeparatedIdentifier() {
      String input = "SELECT * FROM logs.2024.01";
      String result = QueryPreprocessor.process(input);
      assertEquals("SELECT * FROM `logs.2024.01`", result);
    }

    @Test
    @DisplayName("Cat 0: Strip trailing semicolon")
    void stripTrailingSemicolon() {
      String input = "SELECT 1 ;";
      String result = QueryPreprocessor.process(input);
      assertEquals("SELECT 1", result);
    }

    @Test
    @DisplayName("Cat 0: Do not double-quote already backtick-quoted identifier")
    void noDoubleQuoting() {
      String input = "SELECT * FROM `my-index`";
      String result = QueryPreprocessor.process(input);
      assertEquals("SELECT * FROM `my-index`", result);
    }

    @Test
    @DisplayName("Cat 0: Hyphenated identifier parses after preprocessing")
    void hyphenatedIdentifierParses() {
      assertDoesNotThrow(() -> parse("SELECT * FROM my-index"));
    }
  }

  // === Seam 2: Parser Config (Babel + backtick quoting) ===

  @Nested
  @DisplayName("Seam 2: Parser Config")
  class ParserConfigTests {

    @Test
    @DisplayName("Cat 6: Backtick-quoted identifiers parse correctly")
    void backtickIdentifiers() throws SqlParseException {
      SqlNode node = parse("SELECT `field-name` FROM `my-index`");
      assertInstanceOf(SqlSelect.class, node);
    }

    @Test
    @DisplayName("Cat 10: match() is not reserved — parses as function call")
    void matchNotReserved() throws SqlParseException {
      SqlNode node = parse("SELECT * FROM my_index WHERE match(title, 'hello')");
      assertInstanceOf(SqlSelect.class, node);
      SqlSelect select = (SqlSelect) node;
      // WHERE clause should contain a function call to match()
      assertTrue(select.getWhere() != null);
      assertInstanceOf(SqlBasicCall.class, select.getWhere());
      SqlBasicCall call = (SqlBasicCall) select.getWhere();
      assertEquals("match", call.getOperator().getName());
    }
  }

  // === Seam 3: Conformance ===

  @Nested
  @DisplayName("Seam 3: Conformance")
  class ConformanceTests {

    @Test
    @DisplayName("LIMIT syntax accepted")
    void limitSyntax() {
      assertDoesNotThrow(() -> parse("SELECT * FROM my_index LIMIT 10"));
    }

    @Test
    @DisplayName("SELECT without FROM accepted")
    void selectWithoutFrom() {
      assertDoesNotThrow(() -> parse("SELECT 1"));
    }
  }

  // === Seam 4: Operator Table ===

  @Nested
  @DisplayName("Seam 4: Operator Table")
  class OperatorTableTests {

    @Test
    @DisplayName("Cat 1: NOW() parses as function call")
    void nowFunction() throws SqlParseException {
      SqlNode node = parse("SELECT NOW()");
      assertInstanceOf(SqlSelect.class, node);
    }

    @Test
    @DisplayName("Cat 1: CURDATE() parses as function call")
    void curdateFunction() throws SqlParseException {
      SqlNode node = parse("SELECT CURDATE()");
      assertInstanceOf(SqlSelect.class, node);
    }

    @Test
    @DisplayName("Cat 1+10: match() with positional args parses")
    void matchWithArgs() throws SqlParseException {
      SqlNode node = parse("SELECT * FROM my_index WHERE match(title, 'opensearch')");
      assertInstanceOf(SqlSelect.class, node);
    }
  }

  // === Phase 2: Flint DDL (Secondary Index Extension) ===

  @Nested
  @DisplayName("Part 2: Secondary Index Extension (Flint DDL)")
  class FlintDdlTests {

    @Test
    @DisplayName("CREATE SKIPPING INDEX parses to SqlCreateSkippingIndex")
    void createSkippingIndex() throws SqlParseException {
      SqlNode node = parse("CREATE SKIPPING INDEX ON my_table (col1 PARTITION, col2 VALUE_SET)");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlCreateSkippingIndex.class, node);
    }

    @Test
    @DisplayName("CREATE INDEX parses to SqlCreateCoveringIndex")
    void createCoveringIndex() throws SqlParseException {
      SqlNode node = parse("CREATE INDEX my_idx ON my_table (name, age)");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlCreateCoveringIndex.class, node);
    }

    @Test
    @DisplayName("CREATE MATERIALIZED VIEW parses to SqlCreateMaterializedView")
    void createMaterializedView() throws SqlParseException {
      SqlNode node = parse("CREATE MATERIALIZED VIEW my_mv AS SELECT * FROM my_table");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlCreateMaterializedView.class, node);
    }

    @Test
    @DisplayName("DROP SKIPPING INDEX parses to SqlDropFlintIndex")
    void dropSkippingIndex() throws SqlParseException {
      SqlNode node = parse("DROP SKIPPING INDEX ON my_table");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlDropFlintIndex.class, node);
    }

    @Test
    @DisplayName("DROP INDEX parses to SqlDropFlintIndex")
    void dropCoveringIndex() throws SqlParseException {
      SqlNode node = parse("DROP INDEX my_idx ON my_table");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlDropFlintIndex.class, node);
    }

    @Test
    @DisplayName("DROP MATERIALIZED VIEW parses to SqlDropFlintIndex")
    void dropMaterializedView() throws SqlParseException {
      SqlNode node = parse("DROP MATERIALIZED VIEW my_mv");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlDropFlintIndex.class, node);
    }

    @Test
    @DisplayName("REFRESH INDEX parses to SqlRefreshFlintIndex")
    void refreshIndex() throws SqlParseException {
      SqlNode node = parse("REFRESH INDEX my_idx ON my_table");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlRefreshFlintIndex.class, node);
    }

    @Test
    @DisplayName("SHOW FLINT INDEXES parses to SqlShowFlintIndexes")
    void showFlintIndexes() throws SqlParseException {
      SqlNode node = parse("SHOW FLINT INDEXES IN my_catalog.my_database");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlShowFlintIndexes.class, node);
    }

    @Test
    @DisplayName("RECOVER INDEX JOB parses to SqlRecoverIndexJob")
    void recoverIndexJob() throws SqlParseException {
      SqlNode node = parse("RECOVER INDEX JOB my_job_id");
      assertInstanceOf(
          org.opensearch.sql.calcite.parser.flint.SqlRecoverIndexJob.class, node);
    }
  }
}
