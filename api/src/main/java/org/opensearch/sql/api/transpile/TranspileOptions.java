/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.transpile;

import java.util.Objects;
import org.apache.calcite.sql.SqlDialect;

/**
 * Configuration options for transpiling logical plans to SQL. Supports dialect selection using
 * Calcite's native {@link SqlDialect.DatabaseProduct} enum and SQL formatting preferences.
 */
public class TranspileOptions {
  private final SqlDialect.DatabaseProduct databaseProduct;
  private final boolean prettyPrint;

  private TranspileOptions(Builder builder) {
    this.databaseProduct = builder.databaseProduct;
    this.prettyPrint = builder.prettyPrint;
  }

  /**
   * Returns the Calcite {@link SqlDialect} instance for the configured database product. This
   * dialect is used for SQL generation.
   *
   * @return the Calcite SQL dialect instance
   */
  public SqlDialect getSqlDialect() {
    return databaseProduct.getDialect();
  }

  /**
   * Returns whether the generated SQL should be pretty-printed with indentation and line breaks.
   *
   * @return true if pretty printing is enabled, false otherwise
   */
  public boolean isPrettyPrint() {
    return prettyPrint;
  }

  /**
   * Creates a new builder for {@link TranspileOptions} with default settings.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link TranspileOptions}, supporting fluent API for configuration. */
  public static class Builder {
    private SqlDialect.DatabaseProduct databaseProduct = SqlDialect.DatabaseProduct.SPARK;
    private boolean prettyPrint = true;

    /**
     * Sets the target database product for transpilation using Calcite's DatabaseProduct enum. This
     * determines the SQL dialect that will be used for code generation.
     *
     * <p>Supported database products include: SPARK, HIVE, PRESTO, MYSQL, POSTGRESQL, ORACLE,
     * MSSQL, DB2, REDSHIFT, SNOWFLAKE, BIGQUERY, and many others. See {@link
     * SqlDialect.DatabaseProduct} for the complete list.
     *
     * @param databaseProduct the database product to target
     * @return this builder instance
     */
    public Builder databaseProduct(SqlDialect.DatabaseProduct databaseProduct) {
      this.databaseProduct =
          Objects.requireNonNull(databaseProduct, "Database product cannot be null");
      return this;
    }

    /**
     * Enables or disables pretty printing of the generated SQL. When enabled, SQL is formatted with
     * proper indentation and line breaks for readability.
     *
     * @param prettyPrint true to enable pretty printing, false otherwise
     * @return this builder instance
     */
    public Builder prettyPrint(boolean prettyPrint) {
      this.prettyPrint = prettyPrint;
      return this;
    }

    /**
     * Builds a {@link TranspileOptions} instance with the configured settings.
     *
     * @return a new TranspileOptions instance
     */
    public TranspileOptions build() {
      return new TranspileOptions(this);
    }
  }
}
