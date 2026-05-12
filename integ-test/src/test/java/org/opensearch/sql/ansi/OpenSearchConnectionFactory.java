/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ansi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import net.hydromatic.quidem.Quidem;

/**
 * Quidem {@link Quidem.ConnectionFactory} that hands out JDBC connections to the running
 * OpenSearch cluster via the opensearch-sql-jdbc driver.
 *
 * <p>The connection URL points at {@code -Dtests.rest.cluster} (the test infra's REST address for
 * the live cluster) so every Quidem {@code !ok} / {@code !error} / {@code !type} directive hits
 * the real SQL plugin and analytics-engine pipeline.
 *
 * <p>The {@code name} argument of {@link #connect(String, boolean)} comes from {@code !use} lines
 * in {@code .iq} files. All known schema names return the same connection — schema selection in
 * OpenSearch happens via index naming conventions, not via JDBC catalog/schema switching. Unknown
 * names return null so Quidem surfaces a clean error rather than silently masking a typo.
 *
 * <p>Reference connections (for {@code !verify} directives) are not supported yet; we return null
 * which causes Quidem to skip those directives.
 */
public final class OpenSearchConnectionFactory implements Quidem.ConnectionFactory {

  private static final String CLUSTER_PROP = "tests.rest.cluster";

  /** Names accepted in {@code !use <name>} directives. All map to the same OpenSearch endpoint. */
  /** Accept all Calcite test schema names — they all map to the same OpenSearch endpoint. */
  private static final java.util.Set<String> KNOWN_SCHEMAS = java.util.Set.of(
      "opensearch", "ansi", "scott", "post", "blank",
      "aux", "bookstore", "catchall", "foodmart", "jdbc_scott", "orinoco", "seq",
      "mysqlfunc", "mssqlfunc", "oraclefunc",
      "post-big-query",
      "scott-babel", "scott-checked-rounding-half-up", "scott-lenient",
      "scott-mssql", "scott-mysql", "scott-negative-scale",
      "scott-negative-scale-rounding-half-up", "scott-oracle", "scott-spark");

  @Override
  public Connection connect(String name, boolean reference) throws Exception {
    if (reference) {
      // No reference database configured; !verify directives will be skipped.
      return null;
    }
    if (!KNOWN_SCHEMAS.contains(name)) {
      // Unknown name — let Quidem raise its own diagnostic instead of silently connecting.
      return null;
    }
    String cluster = System.getProperty(CLUSTER_PROP);
    if (cluster == null || cluster.isBlank()) {
      throw new IllegalStateException(
          "System property '" + CLUSTER_PROP + "' is not set. The Quidem IT must run against an "
              + "externally-managed cluster: pass -D" + CLUSTER_PROP + "=localhost:9200");
    }
    // Ensure the JDBC driver is registered. Harmless if already done.
    Class.forName("org.opensearch.jdbc.Driver");
    String jdbcUrl = "jdbc:opensearch://" + normalizeHostPort(cluster);
    Properties props = new Properties();
    // Nothing special — we run against local test cluster with no auth.
    return DriverManager.getConnection(jdbcUrl, props);
  }

  /**
   * The {@code tests.rest.cluster} property takes {@code host:port} form, but the JDBC URL needs
   * {@code http://host:port}. Accept both forms for convenience.
   */
  private static String normalizeHostPort(String cluster) {
    if (cluster.startsWith("http://") || cluster.startsWith("https://")) {
      return cluster;
    }
    return "http://" + cluster;
  }
}
