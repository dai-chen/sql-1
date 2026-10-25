/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertFalse;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.Capability.LIKE_CASE_SENSITIVITY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.LikeQueryIT;
import org.opensearch.sql.util.RequiresCapability;

public class CalciteLikeQueryIT extends LikeQueryIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Override
  @Test
  public void test_convert_field_text_to_keyword() throws IOException {
    // US-006: LIKE is intentionally no longer pushed down (not on the index-accelerable list).
    // The superclass asserted that the explain plan contained "TextKeywordBody.keyword" (pushdown
    // rewrite to .keyword subfield). That rewrite no longer happens under Calcite.
    enabledOnlyWhenPushdownIsEnabled();
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE TextKeywordBody LIKE '*'";

    // Verify explain no longer shows a wildcard query pushdown for LIKE.
    String explain = explainQuery(query);
    assertFalse(
        "LIKE should no longer push down as a wildcard query", explain.contains("\"wildcard\""));

    // LIKE '*' uses SQL semantics: '*' is a literal character, not a wildcard.
    // No fixture rows contain a literal '*', so the result set should be empty.
    JSONObject result = executeJdbcRequest(query);
    verifyNumOfRows(result, 0);
  }

  @Test
  public void test_ilike_is_case_insensitive() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE ILike(KeywordBody, 'test Wildcard%') | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(
        result,
        rows("test wildcard"),
        rows("test wildcard in the end of the text%"),
        rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"));
  }

  @Test
  @RequiresCapability(
      value = LIKE_CASE_SENSITIVITY,
      note = "case-sensitive LIKE (legacy=false) expects 0 rows; AE LIKE is case-insensitive.")
  public void test_the_default_3rd_option() throws IOException {
    // only work in v3
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, 'test Wildcard%') | fields KeywordBody";
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "true",
        () -> {
          try {
            JSONObject result = executeQuery(query);
            verifyDataRows(
                result,
                rows("test wildcard"),
                rows("test wildcard in the end of the text%"),
                rows("test wildcard in % the middle of the text"),
                rows("test wildcard %% beside each other"),
                rows("test wildcard in the end of the text_"),
                rows("test wildcard in _ the middle of the text"),
                rows("test wildcard __ beside each other"));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          try {
            JSONObject result = executeQuery(query);
            verifyNumOfRows(result, 0);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
