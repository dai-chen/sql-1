/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DUPLICATION_NULLABLE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration test for the CalciteExecEngine distributed execution path. Verifies that
 * window-family PPL operators (dedup, eventstats) are routed through the calcite_exec aggregation
 * and produce correct results. Asserts against known-correct expected values.
 */
public class CalciteExecDistributedIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.DUPLICATION_NULLABLE);
    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testDedupViaDistributedExec() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name | fields name", TEST_INDEX_DUPLICATION_NULLABLE));

    verifyDataRows(actual, rows("A"), rows("B"), rows("C"), rows("D"), rows("E"));
  }

  @Test
  public void testEventstatsAvgByViaDistributedExec() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age by country"
                    + " | fields name, country, age, avg_age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("age", "int"),
        schema("avg_age", "double"));

    verifyDataRows(
        actual,
        rows("John", "Canada", 25, 22.5),
        rows("Jake", "USA", 70, 50.0),
        rows("Jane", "Canada", 20, 22.5),
        rows("Hello", "USA", 30, 50.0));
  }
}
