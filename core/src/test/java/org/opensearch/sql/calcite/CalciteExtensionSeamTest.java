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

  // === Phase 2 placeholders (require FMPP custom grammar) ===

  @Nested
  @DisplayName("Phase 2: Custom Grammar (TODO — requires FMPP)")
  class CustomGrammarTests {

    // TODO Cat 4: Named parameters — match(title, 'hello', boost=1.5)
    // TODO Cat 9: Array literals — multi_match([title, body], 'hello')
    // TODO Flint DDL — CREATE SKIPPING INDEX, CREATE INDEX, etc.
  }
}
