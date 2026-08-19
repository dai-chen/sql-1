/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class RowBudgetExceededExceptionTest {

  @Test
  void message_includes_forcing_operator_and_formatted_numbers() {
    var ex = new RowBudgetExceededException(12_400_000L, 200_000, "LogicalWindow");
    assertEquals(
        "LogicalWindow requires all rows on one node; gathered 12,400,000 rows, budget 200,000",
        ex.getMessage());
    assertEquals(12_400_000L, ex.getRowsCollected());
    assertEquals(200_000, ex.getRowBudget());
    assertEquals("LogicalWindow", ex.getForcingOperator());
  }

  @Test
  void null_forcing_operator_uses_this_query_as_subject() {
    var ex = new RowBudgetExceededException(500L, 100, null);
    assertEquals(
        "this query requires all rows on one node; gathered 500 rows, budget 100", ex.getMessage());
  }

  @Test
  void empty_forcing_operator_uses_this_query_as_subject() {
    var ex = new RowBudgetExceededException(1_000L, 500, "");
    assertEquals(
        "this query requires all rows on one node; gathered 1,000 rows, budget 500",
        ex.getMessage());
  }

  @Test
  void exception_extends_OpenSearchException() {
    var ex = new RowBudgetExceededException(10L, 5, "LogicalAggregate");
    assertTrue(ex instanceof org.opensearch.OpenSearchException);
  }
}
