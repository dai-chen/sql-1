/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join.legacy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Adaptor for Row and ExprValue.
 */
public class ExprValueRow implements Row<ExprValue> {

  private ExprValue row;

  private String alias;

  public ExprValueRow(ExprValue row, String alias) {
    this.row = row;
    this.alias = alias;
  }

  @Override
  public RowKey key(String[] colNames) {
    Map<String, ExprValue> tuple = row.tupleValue();
    Object[] objects = Arrays.stream(colNames).map(tuple::get).map(ExprValue::value).toArray();
    return new RowKey(objects);
  }

  @Override
  public Row<ExprValue> combine(Row<ExprValue> otherRow) {
    // TODO: use full table to avoid conflicts
    Map<String, ExprValue> results = new HashMap<>();
    row.tupleValue().forEach((k, v) -> results.put(alias + "." + k, v));
    otherRow.data().tupleValue().forEach((k, v) -> results.put(
        ((ExprValueRow) otherRow).alias + "." + k, v));
    return new ExprValueRow(ExprTupleValue.fromExprValueMap(results), null);
  }

  @Override
  public void retain(Map<String, String> colNameAlias) {
    row.tupleValue().forEach((key, value) -> {
      if (colNameAlias.containsKey(key)) {
        row.tupleValue().remove(key);
        // TODO: rename
      }
    });
  }

  @Override
  public ExprValue data() {
    return row;
  }
}
