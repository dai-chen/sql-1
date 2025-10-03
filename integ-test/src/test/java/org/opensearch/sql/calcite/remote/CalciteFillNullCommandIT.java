/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.FillNullCommandIT;

public class CalciteFillNullCommandIT extends FillNullCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testFillNullValueSyntaxSpecificFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, num0 | fillnull value='missing' str2", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows("missing", -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows("missing", 0),
        rows("eight", null),
        rows("nine", 10),
        rows("ten", null),
        rows("eleven", null),
        rows("twelve", null),
        rows("missing", null),
        rows("fourteen", null),
        rows("fifteen", null),
        rows("sixteen", null),
        rows("missing", null));
  }

  @Test
  public void testFillNullValueSyntaxMultipleNumericFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields num0, num2 | fillnull value=-999 num0, num2", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows(12.3, 17.86),
        rows(-12.3, 16.73),
        rows(15.7, -999),
        rows(-15.7, 8.51),
        rows(3.5, 6.46),
        rows(-3.5, 8.98),
        rows(0, 11.69),
        rows(-999, 17.25),
        rows(10, -999),
        rows(-999, 11.5),
        rows(-999, 6.8),
        rows(-999, 3.79),
        rows(-999, -999),
        rows(-999, 13.04),
        rows(-999, -999),
        rows(-999, 10.98),
        rows(-999, 7.87));
  }

  @Test
  public void testFillNullValueSyntaxAllStringFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2 | fillnull value='<not found>'", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one"),
        rows("two"),
        rows("three"),
        rows("<not found>"),
        rows("five"),
        rows("six"),
        rows("<not found>"),
        rows("eight"),
        rows("nine"),
        rows("ten"),
        rows("eleven"),
        rows("twelve"),
        rows("<not found>"),
        rows("fourteen"),
        rows("fifteen"),
        rows("sixteen"),
        rows("<not found>"));
  }

  @Test
  public void testFillNullValueSyntaxNumeric() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, num0 | fillnull value=999 num0", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows(null, -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows(null, 0),
        rows("eight", 999),
        rows("nine", 10),
        rows("ten", 999),
        rows("eleven", 999),
        rows("twelve", 999),
        rows(null, 999),
        rows("fourteen", 999),
        rows("fifteen", 999),
        rows("sixteen", 999),
        rows(null, 999));
  }

  @Test
  public void testFillNullValueSyntaxWithSingleQuotes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, num0 | fillnull value='missing' str2", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows("missing", -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows("missing", 0),
        rows("eight", null),
        rows("nine", 10),
        rows("ten", null),
        rows("eleven", null),
        rows("twelve", null),
        rows("missing", null),
        rows("fourteen", null),
        rows("fifteen", null),
        rows("sixteen", null),
        rows("missing", null));
  }
}
