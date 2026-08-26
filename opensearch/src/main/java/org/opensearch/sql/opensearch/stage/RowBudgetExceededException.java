/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import org.opensearch.OpenSearchException;

/**
 * Thrown when the shard-local row buffer exceeds the configured budget. This is a hard failure —
 * the query must not silently truncate. The message names the operator that forced all rows to
 * gather on one node, making the user's next step actionable.
 *
 * <p>Extends {@link OpenSearchException} so the message survives the transport layer to the REST
 * response. OpenSearchException subclasses that are NOT registered in the exception registry lose
 * their type across the wire but preserve the message text — which is all we need here since the IT
 * asserts on message content, not exception type.
 */
public class RowBudgetExceededException extends OpenSearchException {

  private final long rowsCollected;
  private final int rowBudget;
  private final String forcingOperator;

  public RowBudgetExceededException(long rowsCollected, int rowBudget, String forcingOperator) {
    super(formatMessage(rowsCollected, rowBudget, forcingOperator));
    this.rowsCollected = rowsCollected;
    this.rowBudget = rowBudget;
    this.forcingOperator = forcingOperator;
  }

  public long getRowsCollected() {
    return rowsCollected;
  }

  public int getRowBudget() {
    return rowBudget;
  }

  public String getForcingOperator() {
    return forcingOperator;
  }

  /**
   * Formats the error message with grouping separators on the numbers. When forcingOperator is
   * null/absent, uses "this query" as the subject so the message still reads correctly.
   */
  static String formatMessage(long rowsCollected, int rowBudget, String forcingOperator) {
    String subject =
        (forcingOperator == null || forcingOperator.isEmpty()) ? "this query" : forcingOperator;
    return String.format(
        "%s requires all rows on one node; gathered %,d rows, budget %,d",
        subject, rowsCollected, rowBudget);
  }
}
