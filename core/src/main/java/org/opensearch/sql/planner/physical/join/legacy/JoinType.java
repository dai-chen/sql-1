/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join.legacy;

public enum JoinType {
  COMMA(","), //
  JOIN("JOIN"), //
  INNER_JOIN("INNER JOIN"), //
  CROSS_JOIN("CROSS JOIN"), //
  NATURAL_JOIN("NATURAL JOIN"), //
  NATURAL_INNER_JOIN("NATURAL INNER JOIN"), //
  LEFT_OUTER_JOIN("LEFT JOIN"), //
  RIGHT_OUTER_JOIN("RIGHT JOIN"), //
  FULL_OUTER_JOIN("FULL JOIN"),//
  STRAIGHT_JOIN("STRAIGHT_JOIN"), //
  OUTER_APPLY("OUTER APPLY"),//
  CROSS_APPLY("CROSS APPLY");

  public final String name;

  JoinType(String name){
    this.name = name;
  }

  public static String toString(JoinType joinType) {
    return joinType.name;
  }
}
