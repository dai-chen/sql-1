/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join.legacy;

/**
 * Join condition in ON clause grouped by OR.
 * <p>
 * For example, "ON (a.name = b.id AND a.age = b.age) OR a.location = b.address"
 * => input list: [
 * [ (a.name, b.id), (a.age, b.age) ],
 * [ (a.location, b.address) ]
 * ]
 * <p>
 * => JoinCondition:
 * leftTableAlias: "a", rightTableAlias: "b"
 * leftColumnNames:  [ ["name", "age"], ["location"] ]
 * rightColumnNames: [ ["id", "age"],   ["address" ] ]
 */
public class JoinCondition {

  private final String leftTableAlias;
  private final String rightTableAlias;

  private final String[][] leftColumnNames;
  private final String[][] rightColumnNames;

  public JoinCondition(String leftTableAlias,
                       String rightTableAlias,
                       int groupSize) {
    this.leftTableAlias = leftTableAlias;
    this.rightTableAlias = rightTableAlias;
    this.leftColumnNames = new String[groupSize][];
    this.rightColumnNames = new String[groupSize][];
  }

  public void addLeftColumnNames(int groupNum, String[] colNames) {
    leftColumnNames[groupNum] = colNames;
  }

  public void addRightColumnNames(int groupNum, String[] colNames) {
    rightColumnNames[groupNum] = colNames;
  }

  public int groupSize() {
    return leftColumnNames.length;
  }

  public String leftTableAlias() {
    return leftTableAlias;
  }

  public String rightTableAlias() {
    return rightTableAlias;
  }

  public String[] leftColumnNames(int groupNum) {
    return leftColumnNames[groupNum];
  }

  public String[] rightColumnNames(int groupNum) {
    return rightColumnNames[groupNum];
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    int groupSize = leftColumnNames.length;
    for (int i = 0; i < groupSize; i++) {
      if (i > 0) {
        str.append(" OR ");
      }

      str.append("( ");
      int condSize = leftColumnNames[i].length;
      for (int j = 0; j < condSize; j++) {
        if (j > 0) {
          str.append(" AND ");
        }
        str.append(leftTableAlias).
            append(".").
            append(leftColumnNames[i][j]).
            append(" = ").
            append(rightTableAlias).
            append(".").
            append(rightColumnNames[i][j]);
      }
      str.append(" )");
    }
    return str.toString();
  }

}
