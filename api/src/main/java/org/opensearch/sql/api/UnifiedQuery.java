/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.opensearch.sql.api.transpiler.TranspileOptions;
import org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler;
import org.opensearch.sql.executor.QueryType;

/**
 * Unified Query API providing a fluent interface for PPL query processing. This class offers a
 * chainable API for query configuration and execution.
 */
public class UnifiedQuery {
  private final UnifiedQueryPlanner.Builder plannerBuilder;

  private UnifiedQuery() {
    this.plannerBuilder = UnifiedQueryPlanner.builder();
  }

  /**
   * Static factory method to start building a query with the specified language.
   *
   * @param language the query language type (e.g., PPL)
   * @return a new UnifiedQuery instance configured with the language
   */
  public static UnifiedQuery lang(QueryType language) {
    UnifiedQuery query = new UnifiedQuery();
    query.plannerBuilder.language(language);
    return query;
  }

  /**
   * Registers a catalog with the specified name and schema.
   *
   * @param name the name of the catalog to register
   * @param schema the schema representing the structure of the catalog
   * @return this UnifiedQuery instance for method chaining
   */
  public UnifiedQuery catalog(String name, Schema schema) {
    plannerBuilder.catalog(name, schema);
    return this;
  }

  /**
   * Sets the default namespace path for resolving unqualified table names.
   *
   * @param namespace dot-separated path (e.g., "spark_catalog.default" or "opensearch")
   * @return this UnifiedQuery instance for method chaining
   */
  public UnifiedQuery defaultNamespace(String namespace) {
    plannerBuilder.defaultNamespace(namespace);
    return this;
  }

  /**
   * Enables or disables catalog metadata caching.
   *
   * @param cache whether to enable metadata caching
   * @return this UnifiedQuery instance for method chaining
   */
  public UnifiedQuery cacheMetadata(boolean cache) {
    plannerBuilder.cacheMetadata(cache);
    return this;
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode).
   *
   * @param query the raw query string to plan
   * @return a Calcite logical plan representing the query
   * @throws IllegalStateException if query planning fails
   */
  public RelNode plan(String query) {
    UnifiedQueryPlanner planner = plannerBuilder.build();
    return planner.plan(query);
  }

  /**
   * Transpiles a query to the specified SQL dialect.
   *
   * @param query the raw query string to transpile
   * @param dialect the target database product dialect
   * @return the generated SQL string in the target dialect
   * @throws IllegalStateException if transpilation fails
   */
  public String transpile(String query, SqlDialect.DatabaseProduct dialect) {
    return transpile(query, TranspileOptions.builder().databaseProduct(dialect).build());
  }

  /**
   * Transpiles a query using custom transpile options.
   *
   * @param query the raw query string to transpile
   * @param options custom transpilation options (dialect, formatting, etc.)
   * @return the generated SQL string
   * @throws IllegalStateException if transpilation fails
   */
  public String transpile(String query, TranspileOptions options) {
    UnifiedQueryPlanner planner = plannerBuilder.build();
    RelNode plan = planner.plan(query);
    UnifiedQueryTranspiler transpiler = new UnifiedQueryTranspiler();
    return transpiler.toSql(plan, options);
  }
}
