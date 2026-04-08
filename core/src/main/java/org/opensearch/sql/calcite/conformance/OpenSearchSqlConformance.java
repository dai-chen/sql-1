package org.opensearch.sql.calcite.conformance;

import org.apache.calcite.sql.validate.SqlAbstractConformance;

/**
 * SQL conformance for OpenSearch Unified SQL.
 * Extends ANSI SQL with accommodations for OpenSearch SQL compatibility.
 */
public class OpenSearchSqlConformance extends SqlAbstractConformance {

  public static final OpenSearchSqlConformance INSTANCE = new OpenSearchSqlConformance();

  /** Allow GROUP BY alias (e.g., GROUP BY s where s is a SELECT alias). */
  @Override
  public boolean isGroupByAlias() {
    return true;
  }

  /** Allow LIMIT [start,] count syntax. */
  @Override
  public boolean isLimitStartCountAllowed() {
    return true;
  }

  /** FROM clause is not required (e.g., SELECT 1, SELECT NOW()). */
  @Override
  public boolean isFromRequired() {
    return false;
  }
}
