/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.runtime;

import java.sql.PreparedStatement;
import lombok.Builder;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

/**
 * {@code UnifiedQueryCompiler} compiles Calcite logical plans ({@link RelNode}) into executable
 * JDBC statements. It separates query compilation from execution, following the same pattern as
 * PartiQL's PartiQLCompiler and JDBC's PreparedStatement.
 *
 * <p>The compiler follows the same design patterns as {@link UnifiedQueryPlanner} and {@link
 * org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler}, accepting RelNode as input for single
 * responsibility and composability.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
 *     .language(QueryType.PPL)
 *     .catalog("my_catalog", schema)
 *     .defaultNamespace("my_catalog")
 *     .build();
 *
 * RelNode plan = planner.plan("source my_table | fields name, age");
 *
 * UnifiedQueryCompiler compiler = UnifiedQueryCompiler.builder()
 *     .context(planner.getContext())
 *     .build();
 *
 * // Compile once, execute multiple times with standard JDBC
 * try (PreparedStatement statement = compiler.compile(plan)) {
 *   ResultSet resultSet = statement.executeQuery();
 *   while (resultSet.next()) {
 *     String name = resultSet.getString("name");
 *     int age = resultSet.getInt("age");
 *   }
 * }
 * }</pre>
 */
@Builder
public class UnifiedQueryCompiler {

  /** The Calcite plan context containing connection and schema information. */
  private final CalcitePlanContext context;

  /**
   * Compiles a Calcite logical plan into an executable PreparedStatement.
   *
   * <p>The returned PreparedStatement can be executed multiple times and should be closed when
   * done. Results are returned as standard JDBC ResultSet.
   *
   * @param plan the logical plan to compile (must not be null)
   * @return a compiled PreparedStatement ready for execution
   * @throws IllegalStateException if compilation fails
   */
  public PreparedStatement compile(RelNode plan) {
    try {
      return CalciteToolsHelper.OpenSearchRelRunners.run(context, plan);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to compile logical plan", e);
    }
  }
}
