/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;

/** Executes the coordinator continuation over reduced shard rows. */
final class CoordinatorTreeExecutor {

  private CoordinatorTreeExecutor() {}

  record GatheredRows(
      StagePlan.CoordinatorInput target, RelDataType rowType, List<Object[]> rows) {}

  static List<Object[]> execute(
      RelNode coordinatorTree, List<GatheredRows> inputs, long currentTimeNanos) {
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(CalciteFragmentSerde.SCHEMA_NAME, new AbstractSchema() {});
    Map<String, Object> rowsByStashKey = new HashMap<>();
    for (GatheredRows input : inputs) {
      osSchema.add(
          input.target().tableName(),
          new CalciteRowsTable(input.rowType(), input.target().dataContextKey()));
      rowsByStashKey.put(input.target().dataContextKey(), input.rows());
    }
    rowsByStashKey.put(DataContext.Variable.UTC_TIMESTAMP.camelName, currentTimeNanos);
    return EnumerableFragmentExecutor.execute(coordinatorTree, rootSchema, rowsByStashKey);
  }
}
