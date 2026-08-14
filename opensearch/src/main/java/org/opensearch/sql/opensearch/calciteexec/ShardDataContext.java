/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ShardDataContext implements DataContext {

  private final List<Object[]> rows;
  private final SchemaPlus rootSchema;
  private final JavaTypeFactory typeFactory;

  public ShardDataContext(List<Object[]> rows, SchemaPlus rootSchema) {
    this.rows = rows;
    this.rootSchema = rootSchema;
    this.typeFactory = new JavaTypeFactoryImpl();
  }

  @Override
  public @Nullable SchemaPlus getRootSchema() {
    return rootSchema;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override
  public QueryProvider getQueryProvider() {
    return null;
  }

  @Override
  public @Nullable Object get(String name) {
    if (DocValuesScannableTable.ROW_DATA_KEY.equals(name)) {
      return rows;
    }
    return null;
  }
}
