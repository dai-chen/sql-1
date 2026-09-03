/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.Capability.PERCENTILE_APPROXIMATE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;
import org.opensearch.sql.util.RequiresCapability;

public class WindowFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.BANK);
  }

  @Test
  public void testOrderByNullFirst() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT age, ROW_NUMBER() OVER(ORDER BY age DESC NULLS FIRST) "
                    + "FROM "
                    + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES,
                "jdbc"));

    verifyDataRows(
        response,
        rows(null, 1),
        rows(36, 2),
        rows(36, 3),
        rows(34, 4),
        rows(33, 5),
        rows(32, 6),
        rows(28, 7));
  }

  @Test
  public void testOrderByNullLast() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT age, ROW_NUMBER() OVER(ORDER BY age NULLS LAST) "
                    + "FROM "
                    + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES,
                "jdbc"));

    verifyDataRows(
        response,
        rows(28, 1),
        rows(32, 2),
        rows(33, 3),
        rows(34, 4),
        rows(36, 5),
        rows(36, 6),
        rows(null, 7));
  }

  @Test
  public void testDistinctCountOverNull() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT lastname, COUNT(DISTINCT gender) OVER() "
                    + "FROM "
                    + TestsConstants.TEST_INDEX_BANK,
                "jdbc"));
    verifyDataRows(
        response,
        rows("Duke Willmington", 2),
        rows("Bond", 2),
        rows("Bates", 2),
        rows("Adams", 2),
        rows("Ratliff", 2),
        rows("Ayala", 2),
        rows("Mcpherson", 2));
  }

  @Test
  public void testDistinctCountOver() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT lastname, COUNT(DISTINCT gender) OVER(ORDER BY lastname) "
                    + "FROM "
                    + TestsConstants.TEST_INDEX_BANK,
                "jdbc"));
    verifyDataRowsInOrder(
        response,
        rows("Adams", 1),
        rows("Ayala", 2),
        rows("Bates", 2),
        rows("Bond", 2),
        rows("Duke Willmington", 2),
        rows("Mcpherson", 2),
        rows("Ratliff", 2));
  }

  @Test
  public void testDistinctCountPartition() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT lastname, COUNT(DISTINCT gender) OVER(PARTITION BY gender ORDER BY"
                    + " lastname) FROM "
                    + TestsConstants.TEST_INDEX_BANK,
                "jdbc"));
    verifyDataRowsInOrder(
        response,
        rows("Ayala", 1),
        rows("Bates", 1),
        rows("Mcpherson", 1),
        rows("Adams", 1),
        rows("Bond", 1),
        rows("Duke Willmington", 1),
        rows("Ratliff", 1));
  }

  @Test
  @RequiresCapability(PERCENTILE_APPROXIMATE)
  public void testPercentileOverNull() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT lastname, percentile(balance, 50) OVER() "
                    + "FROM "
                    + TestsConstants.TEST_INDEX_BANK,
                "jdbc"));
    verifyDataRows(
        response,
        rows("Duke Willmington", 32838),
        rows("Bond", 32838),
        rows("Bates", 32838),
        rows("Adams", 32838),
        rows("Ratliff", 32838),
        rows("Ayala", 32838),
        rows("Mcpherson", 32838));
  }

  @Test
  @RequiresCapability(PERCENTILE_APPROXIMATE)
  public void testPercentileOver() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT lastname, percentile(balance, 50) OVER(ORDER BY lastname) "
                    + "FROM "
                    + TestsConstants.TEST_INDEX_BANK,
                "jdbc"));
    verifyDataRowsInOrder(
        response,
        rows("Adams", 4180),
        rows("Ayala", 40540),
        rows("Bates", 32838),
        rows("Bond", 32838),
        rows("Duke Willmington", 32838),
        rows("Mcpherson", 39225),
        rows("Ratliff", 32838));
  }

  @Test
  @RequiresCapability(PERCENTILE_APPROXIMATE)
  public void testPercentilePartition() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT lastname, percentile(balance, 50) OVER(PARTITION BY gender ORDER BY"
                    + " lastname) FROM "
                    + TestsConstants.TEST_INDEX_BANK,
                "jdbc"));
    verifyDataRowsInOrder(
        response,
        rows("Ayala", 40540),
        rows("Bates", 40540),
        rows("Mcpherson", 40540),
        rows("Adams", 4180),
        rows("Bond", 5686),
        rows("Duke Willmington", 5686),
        rows("Ratliff", 16418));
  }

  @Test
  public void testRankOverNull() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT lastname, RANK() OVER() FROM %s\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(
        response,
        rows("Duke Willmington", 1),
        rows("Bond", 1),
        rows("Bates", 1),
        rows("Adams", 1),
        rows("Ratliff", 1),
        rows("Ayala", 1),
        rows("Mcpherson", 1));
  }

  @Test
  public void testRankOver() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT age, RANK() OVER(ORDER BY age DESC) FROM %s\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(
        response,
        rows(39, 1),
        rows(36, 2),
        rows(36, 2),
        rows(34, 4),
        rows(33, 5),
        rows(32, 6),
        rows(28, 7));
  }

  @Test
  public void testRankPartition() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT lastname, RANK() OVER(PARTITION BY gender ORDER BY age DESC)\
                 FROM %s\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(
        response,
        rows("Bond", 1),
        rows("Ratliff", 1),
        rows("Adams", 3),
        rows("Duke Willmington", 4),
        rows("Ayala", 1),
        rows("Mcpherson", 2),
        rows("Bates", 3));
  }

  @Test
  public void testDenseRankOver() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT age, DENSE_RANK() OVER(ORDER BY age DESC) FROM %s\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(
        response,
        rows(39, 1),
        rows(36, 2),
        rows(36, 2),
        rows(34, 3),
        rows(33, 4),
        rows(32, 5),
        rows(28, 6));
  }

  /** ORDER BY a column the SELECT list doesn't project: the window is computed over all rows. */
  @Test
  public void testWindowWithOuterSortOnNonProjectedColumn() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT lastname, ROW_NUMBER() OVER(ORDER BY age) AS rn FROM %s ORDER BY age\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRowsInOrder(
        response,
        rows("Bates", 1),
        rows("Duke Willmington", 2),
        rows("Adams", 3),
        rows("Mcpherson", 4),
        rows("Bond", 5),
        rows("Ratliff", 6),
        rows("Ayala", 7));
  }

  @Test
  public void testWindowWithOuterSortOnNonProjectedColumnAndLimit() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT lastname, ROW_NUMBER() OVER(ORDER BY age) AS rn FROM %s\
                 ORDER BY age LIMIT 3\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRowsInOrder(
        response, rows("Bates", 1), rows("Duke Willmington", 2), rows("Adams", 3));
  }

  /** The sort column is projected here, so nothing has to be borrowed and then dropped. */
  @Test
  public void testWindowWithOuterSortOnProjectedColumnAndLimit() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT lastname, age, ROW_NUMBER() OVER(ORDER BY age) AS rn FROM %s\
                 ORDER BY age LIMIT 3\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRowsInOrder(
        response, rows("Bates", 28, 1), rows("Duke Willmington", 32, 2), rows("Adams", 33, 3));
  }

  @Test
  public void testRankWithOuterSortOnNonProjectedColumnAndLimit() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT lastname, RANK() OVER(ORDER BY age DESC) AS r FROM %s\
                 ORDER BY age DESC LIMIT 3\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    // Bond and Ratliff tie on age, so their relative order is not defined.
    verifyDataRows(response, rows("Ayala", 1), rows("Bond", 2), rows("Ratliff", 2));
  }

  /**
   * An aliased select item puts only its alias in scope, not its source column name, so ORDER BY on
   * that column must still be borrowed through the Project. Sorting on the same column the window
   * orders by keeps the expectation identical on both query routes.
   */
  @Test
  public void testWindowWithOuterSortOnAliasedColumn() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT age AS years, ROW_NUMBER() OVER(ORDER BY age) AS rn FROM %s\
                 ORDER BY age LIMIT 3\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRowsInOrder(response, rows(28, 1), rows(32, 2), rows(33, 3));
  }

  /**
   * Regression guard: {@code SELECT *} already keeps every column in scope, so no sort key has to
   * be borrowed and no trailing Project may be added -- {@code AllFields} has no single output name
   * to reference it by.
   */
  @Test
  public void testWindowWithSelectStarAndOuterSort() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT *, ROW_NUMBER() OVER(ORDER BY age) AS rn FROM %s ORDER BY age LIMIT 2\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(2, rows.length());
    JSONArray firstRow = rows.getJSONArray(0);
    assertEquals(1, firstRow.getInt(firstRow.length() - 1));
  }
}
