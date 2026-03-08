/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.calcite.OpenSearchSchema;

/**
 * Schema wrapper that returns {@link AnsiSQLOpenSearchTable} instances, mapping date/time fields to
 * standard Calcite SQL types, excluding metadata fields, and disabling pushdown rules.
 */
public class AnsiSQLOpenSearchSchema extends AbstractSchema {

  private final OpenSearchSchema delegate;

  public AnsiSQLOpenSearchSchema(OpenSearchSchema delegate) {
    this.delegate = delegate;
  }

  private final Map<String, Table> tableMap =
      new HashMap<>() {
        @Override
        public Table get(Object key) {
          Table original = delegate.getTableMap().get(key);
          if (original == null) return null;
          return new AnsiSQLOpenSearchTable(original);
        }

        @Override
        public boolean containsKey(Object key) {
          return delegate.getTableMap().containsKey(key);
        }
      };

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }
}
