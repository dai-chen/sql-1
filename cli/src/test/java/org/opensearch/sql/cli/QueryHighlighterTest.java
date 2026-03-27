/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class QueryHighlighterTest {

  private static long styleOf(AttributedStyle style) {
    AttributedStringBuilder b = new AttributedStringBuilder();
    b.styled(style, "X");
    return b.toAttributedString().styleAt(0).getStyle();
  }

  private static final long BOLD_BLUE =
      styleOf(new AttributedStyle().bold().foreground(AttributedStyle.BLUE));
  private static final long GREEN =
      styleOf(new AttributedStyle().foreground(AttributedStyle.GREEN));
  private static final long MAGENTA =
      styleOf(new AttributedStyle().foreground(AttributedStyle.MAGENTA));
  private static final long CYAN = styleOf(new AttributedStyle().foreground(AttributedStyle.CYAN));
  private static final long DEFAULT_STYLE = styleOf(AttributedStyle.DEFAULT);

  private QueryHighlighter sqlHighlighter() {
    return new QueryHighlighter(Collections.emptyMap(), QueryType.SQL);
  }

  private QueryHighlighter pplHighlighter() {
    return new QueryHighlighter(Collections.emptyMap(), QueryType.PPL);
  }

  @Test
  public void testSqlKeywordHighlighting() {
    AttributedString result = sqlHighlighter().highlight(null, "SELECT * FROM t");
    assertEquals(BOLD_BLUE, result.styleAt(0).getStyle()); // S of SELECT
    assertEquals(BOLD_BLUE, result.styleAt(9).getStyle()); // F of FROM
  }

  @Test
  public void testPplKeywordHighlighting() {
    AttributedString result = pplHighlighter().highlight(null, "source where fields");
    assertEquals(BOLD_BLUE, result.styleAt(0).getStyle()); // s of source
    assertEquals(BOLD_BLUE, result.styleAt(7).getStyle()); // w of where
    assertEquals(BOLD_BLUE, result.styleAt(13).getStyle()); // f of fields
  }

  @Test
  public void testStringLiteralHighlighting() {
    AttributedString result = sqlHighlighter().highlight(null, "SELECT 'hello'");
    assertEquals(GREEN, result.styleAt(7).getStyle()); // opening quote
    assertEquals(GREEN, result.styleAt(12).getStyle()); // letter inside
  }

  @Test
  public void testNumberHighlighting() {
    AttributedString result = sqlHighlighter().highlight(null, "SELECT 42 3.14");
    assertEquals(MAGENTA, result.styleAt(7).getStyle()); // 4 of 42
    assertEquals(MAGENTA, result.styleAt(10).getStyle()); // 3 of 3.14
  }

  @Test
  public void testMetaCommandHighlighting() {
    AttributedString result = sqlHighlighter().highlight(null, ".help");
    assertEquals(CYAN, result.styleAt(0).getStyle());
  }

  @Test
  public void testLanguageSwitch() {
    QueryHighlighter h = sqlHighlighter();
    // SELECT is a SQL keyword
    AttributedString r1 = h.highlight(null, "SELECT");
    assertEquals(BOLD_BLUE, r1.styleAt(0).getStyle());

    h.updateLanguage(QueryType.PPL);
    // SELECT is not a PPL keyword
    AttributedString r2 = h.highlight(null, "SELECT");
    assertEquals(DEFAULT_STYLE, r2.styleAt(0).getStyle());

    // source is a PPL keyword
    AttributedString r3 = h.highlight(null, "source");
    assertEquals(BOLD_BLUE, r3.styleAt(0).getStyle());
  }

  @Test
  public void testOutputLengthMatchesInput() {
    QueryHighlighter h = sqlHighlighter();
    String[] inputs = {
      "",
      "SELECT * FROM t",
      ".help",
      "'hello world'",
      "42 3.14 -7",
      "SELECT name FROM t WHERE id = 'abc'",
      "  spaces  "
    };
    for (String input : inputs) {
      assertEquals(input.length(), h.highlight(null, input).length());
    }
  }
}
