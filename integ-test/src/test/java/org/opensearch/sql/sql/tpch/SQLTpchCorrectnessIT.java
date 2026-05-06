/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.tpch;

import com.google.common.io.Resources;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.client.RestClient;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * TPC-H SQL snapshot-based correctness test.
 *
 * <p><b>Routing:</b> When {@code tests.analytics.force_routing=true} (the default for the
 * {@code analyticsSqlTpchCorrectnessTest} Gradle task), the parent
 * {@link SQLIntegTestCase#init()} flips the cluster setting
 * {@code plugins.calcite.analytics.force_routing=true} so every query is routed through the
 * unified-path {@code RestUnifiedQueryAction} to the analytics engine. The TPC-H indices are
 * created as Parquet-backed indices (via {@code composite.primary_data_format=parquet} in the
 * mapping settings), enabling the analytics engine to scan them via
 * {@code analytics-backend-datafusion}. When the property is {@code false}, queries run through
 * the legacy v2 SQL path (useful for regression / comparison).
 *
 * <p>Replays every TPC-H SQL query through the JDBC endpoint and compares each response
 * against a committed JSON snapshot at {@code src/test/resources/expectedOutput/tpch-sql/qN.json}.
 *
 * <p><b>Snapshot workflow:</b>
 * <ol>
 *   <li>Capture (one-time): run with {@code -Dtests.snapshot.write=true} to write actual
 *       responses as the new expected snapshots. Only successful (non-error) responses are
 *       snapshotted.</li>
 *   <li>Validate (normal): run without the flag. Each query's normalized response is compared
 *       against the snapshot via {@link JSONObject#similar(Object)}. Missing snapshots are
 *       reported as NO_REFERENCE, mismatches as FAIL.</li>
 * </ol>
 *
 * <p>The test never fails via JUnit assertions — the markdown report is the deliverable.
 *
 * <p>Output: {@code integ-test/build/reports/tpch-sql-correctness/REPORT.md}.
 *
 * <p>To regenerate snapshots:
 * <pre>{@code
 * ./gradlew :integ-test:analyticsSqlTpchCorrectnessReport \
 *   -Dtests.rest.cluster=localhost:9200 \
 *   -Dtests.cluster=localhost:9300 \
 *   -Dtests.clustername=runTask \
 *   -Dtests.snapshot.write=true \
 *   -Dproject.root=<path-to-integ-test>
 * }</pre>
 *
 * <p>To fall back to the legacy v2 path:
 * <pre>{@code
 * ./gradlew :integ-test:analyticsSqlTpchCorrectnessReport \
 *   -Dtests.analytics.force_routing=false \
 *   ...
 * }</pre>
 */
@FixMethodOrder(MethodSorters.JVM)
public class SQLTpchCorrectnessIT extends SQLIntegTestCase {

  /** Number of TPC-H queries (q1..qN). The set ships 22 queries. */
  private static final int N_QUERIES = 22;

  /** Matches the leading {@code /* ... *}{@code /} block in each .ppl file. */
  private static final Pattern SQL_BLOCK =
      Pattern.compile("/\\*\\s*(.*?)\\s*\\*/", Pattern.DOTALL);

  /** Classpath-relative directory for expected JSON snapshots. */
  private static final String SNAPSHOT_DIR_RESOURCE = "expectedOutput/tpch-sql";

  /** When true, write actual response to expected JSON file instead of asserting. */
  private static final boolean WRITE_SNAPSHOTS =
      Boolean.getBoolean("tests.snapshot.write");

  /** Deterministic fields to keep in normalized responses. */
  private static final java.util.Set<String> KEEP_FIELDS =
      java.util.Set.of("schema", "datarows", "total", "size");

  /** Whether the analytics-engine force-routing path is active. */
  private static final boolean FORCE_ROUTING =
      Boolean.parseBoolean(System.getProperty("tests.analytics.force_routing", "true"));

  /** If index setup failed, the reason is stored here so runAllSqlQueries can report it. */
  private String initFailure = null;

  /** TPC-H tables loaded for every query. Order follows CalcitePPLTpchIT for consistency. */
  private static final Index[] TPCH_TABLES = new Index[] {
      Index.TPCH_CUSTOMER,
      Index.TPCH_LINEITEM,
      Index.TPCH_ORDERS,
      Index.TPCH_SUPPLIER,
      Index.TPCH_PART,
      Index.TPCH_PARTSUPP,
      Index.TPCH_NATION,
      Index.TPCH_REGION,
  };

  @Override
  protected void init() throws Exception {
    super.init();
    try {
      for (Index tbl : TPCH_TABLES) {
        loadIndex(tbl);
      }
    } catch (Exception e) {
      // Record init failure but DO NOT throw — we want runAllSqlQueries to still emit a report
      // documenting the failure. Throwing here would short-circuit JUnit and skip reporting,
      // which violates the doc's hard constraint that the REPORT is the deliverable.
      initFailure = summarize(e);
      logger.error("Index setup failed, queries will all FAIL with init error: {}", initFailure);
    }
  }

  /**
   * Override to use the new TestUtils which handles parquet indices gracefully.
   * Parquet-backed indices reject {@code refresh=wait_for} (the legacy default), causing hangs.
   * The new TestUtils uses {@code refresh=true} with a retry fallback that strips the refresh
   * policy when the cluster returns HTTP 400 "true refresh policy is not supported."
   */
  @Override
  protected synchronized void loadIndex(Index index, RestClient client) throws IOException {
    String indexName = index.getName();
    String mapping = index.getMapping();
    String dataSet = index.getDataSet();

    if (!org.opensearch.sql.legacy.TestUtils.isIndexExist(client, indexName)) {
      org.opensearch.sql.legacy.TestUtils.createIndexByRestClient(client, indexName, mapping);
      org.opensearch.sql.util.TestUtils.loadDataByRestClient(client, indexName, dataSet);
    }
  }

  @Test
  public void runAllSqlQueries() throws IOException {
    List<Result> results = new ArrayList<>();
    // If init() failed (e.g., parquet bulk-load timeout), emit a report where every query is
    // marked FAIL with the init error, then return cleanly. This preserves the "REPORT is the
    // deliverable" contract from docs/mustang-followup-ansi-sql-it.md constraint #2.
    if (initFailure != null) {
      for (int i = 1; i <= N_QUERIES; i++) {
        results.add(Result.of(i, Status.FAIL, null, 0, 0, "init failed: " + initFailure, null));
      }
      writeReport(results);
      return;
    }
    for (int i = 1; i <= N_QUERIES; i++) {
      String fileBody = loadFromFile("tpch/queries/q" + i + ".ppl");
      String sql = extractSqlFromComment(fileBody);
      if (sql == null || sql.isBlank()) {
        results.add(Result.of(i, Status.SKIP, null, 0, 0, "no SQL block in q" + i + ".ppl", null));
        continue;
      }
      long start = System.currentTimeMillis();
      try {
        JSONObject response = executeJdbcRequest(sql);
        long elapsedMs = System.currentTimeMillis() - start;
        processResponse(i, sql, response, elapsedMs, results);
      } catch (Exception e) {
        long elapsedMs = System.currentTimeMillis() - start;
        results.add(Result.of(i, Status.FAIL, sql, elapsedMs, 0, summarize(e), null));
      }
    }
    writeReport(results);
  }

  /** Process a successful HTTP response — may still contain an error envelope. */
  private void processResponse(
      int id, String sql, JSONObject response, long elapsedMs, List<Result> results)
      throws IOException {
    // Error envelope: record as FAIL, do NOT snapshot.
    if (response.has("error")) {
      String errText = response.get("error") instanceof JSONObject
          ? errorSummary(response.getJSONObject("error"))
          : response.get("error").toString();
      results.add(Result.of(id, Status.FAIL, sql, elapsedMs, 0, errText, null));
      return;
    }

    // Verify the query actually returned data — an empty result set indicates the test data
    // does not satisfy the query's filter conditions or JOIN keys.
    if (response.has("datarows") && response.getJSONArray("datarows").length() == 0) {
      results.add(Result.of(id, Status.FAIL, sql, elapsedMs, 0,
          "empty result set (0 rows) — test data may not satisfy query filters", null));
      return;
    }

    JSONObject normalized = normalize(response);
    int rowCount = normalized.optInt("total",
        normalized.has("datarows") ? normalized.getJSONArray("datarows").length() : 0);

    if (WRITE_SNAPSHOTS) {
      writeSnapshot(id, normalized);
      results.add(Result.of(id, Status.SNAPSHOT, sql, elapsedMs, rowCount, null, null));
    } else {
      JSONObject expected = loadSnapshot(id);
      if (expected == null) {
        results.add(Result.of(id, Status.NO_REFERENCE, sql, elapsedMs, rowCount, null, null));
      } else if (expected.similar(normalized)) {
        results.add(Result.of(id, Status.PASS, sql, elapsedMs, rowCount, null, null));
      } else {
        String diff = diffSummary(expected, normalized);
        results.add(Result.of(id, Status.FAIL, sql, elapsedMs, rowCount, null, diff));
      }
    }
  }

  /** Keep only deterministic fields: schema, datarows, total, size. */
  private static JSONObject normalize(JSONObject response) {
    JSONObject out = new JSONObject();
    for (String key : KEEP_FIELDS) {
      if (response.has(key)) {
        out.put(key, response.get(key));
      }
    }
    return out;
  }

  /** Write a pretty-printed snapshot to the source tree. */
  private static void writeSnapshot(int id, JSONObject normalized) throws IOException {
    Path snapshotFile = resolveSnapshotSourcePath(id);
    Files.createDirectories(snapshotFile.getParent());
    Files.writeString(snapshotFile, normalized.toString(2) + "\n", StandardCharsets.UTF_8);
  }

  /** Load a snapshot from the classpath. Returns null if not found. */
  private static JSONObject loadSnapshot(int id) {
    String resource = SNAPSHOT_DIR_RESOURCE + "/q" + id + ".json";
    try (InputStream is =
        SQLTpchCorrectnessIT.class.getClassLoader().getResourceAsStream(resource)) {
      if (is == null) return null;
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      return new JSONObject(content);
    } catch (Exception e) {
      return null;
    }
  }

  /** Resolve the source-tree path for writing snapshots (not the build classpath). */
  private static Path resolveSnapshotSourcePath(int id) {
    String projectRoot = System.getProperty("project.root");
    Path base =
        projectRoot != null && !projectRoot.isEmpty()
            ? Paths.get(projectRoot)
            : Paths.get("").toAbsolutePath();
    return base.resolve("src/test/resources/" + SNAPSHOT_DIR_RESOURCE + "/q" + id + ".json");
  }

  /** Compute a brief diff summary between expected and actual JSON. */
  private static String diffSummary(JSONObject expected, JSONObject actual) {
    StringBuilder sb = new StringBuilder();
    for (String key : java.util.Set.of("schema", "datarows", "total", "size")) {
      boolean eHas = expected.has(key);
      boolean aHas = actual.has(key);
      if (eHas != aHas) {
        sb.append(key).append(": ").append(eHas ? "present" : "missing")
            .append(" in expected, ").append(aHas ? "present" : "missing")
            .append(" in actual. ");
        continue;
      }
      if (!eHas) continue;
      if (key.equals("datarows")) {
        JSONArray eRows = expected.getJSONArray("datarows");
        JSONArray aRows = actual.getJSONArray("datarows");
        if (eRows.length() != aRows.length()) {
          sb.append("datarows: expected ").append(eRows.length())
              .append(" rows, got ").append(aRows.length()).append(". ");
        } else {
          for (int r = 0; r < eRows.length(); r++) {
            if (!eRows.get(r).toString().equals(aRows.get(r).toString())) {
              sb.append("datarows: first differing row idx ").append(r)
                  .append(", expected=").append(clip(eRows.get(r).toString(), 80))
                  .append(", actual=").append(clip(aRows.get(r).toString(), 80)).append(". ");
              break;
            }
          }
        }
      } else {
        String eVal = expected.get(key).toString();
        String aVal = actual.get(key).toString();
        if (!eVal.equals(aVal)) {
          sb.append(key).append(": expected=").append(clip(eVal, 60))
              .append(", actual=").append(clip(aVal, 60)).append(". ");
        }
      }
    }
    return sb.length() == 0 ? "unknown diff" : sb.toString().trim();
  }

  // ── Report writer ──────────────────────────────────────────────────────────

  private void writeReport(List<Result> results) throws IOException {
    int passed = 0, failed = 0, noRef = 0, snapped = 0, skipped = 0;
    long totalMs = 0;
    for (Result r : results) {
      switch (r.status) {
        case PASS -> { passed++; totalMs += r.elapsedMs; }
        case FAIL -> { failed++; totalMs += r.elapsedMs; }
        case NO_REFERENCE -> { noRef++; totalMs += r.elapsedMs; }
        case SNAPSHOT -> { snapped++; totalMs += r.elapsedMs; }
        case SKIP -> skipped++;
      }
    }
    int total = results.size();
    int comparable = passed + failed;
    double passPct = comparable == 0 ? 0.0 : (100.0 * passed / comparable);

    String routingMode = FORCE_ROUTING ? "analytics-engine (unified path)" : "legacy v2 SQL";

    Path reportDir = resolveReportDir();
    Files.createDirectories(reportDir);
    Path reportFile = reportDir.resolve("REPORT.md");

    StringBuilder sb = new StringBuilder();
    sb.append("# TPC-H SQL Correctness Report\n\n");
    sb.append("Generated: ").append(new java.util.Date()).append("\n\n");
    sb.append("Cluster: `")
        .append(System.getProperty("tests.rest.cluster", "<unspecified>"))
        .append("`\n\n");
    sb.append("Routing: `tests.analytics.force_routing=")
        .append(FORCE_ROUTING)
        .append("`\n\n");
    sb.append("Query path: **").append(routingMode).append("**\n\n");
    sb.append("Snapshot mode: ").append(WRITE_SNAPSHOTS ? "**WRITE** (capturing)" : "validate")
        .append("\n\n");

    sb.append("## Summary\n\n");
    sb.append("| Metric | Value |\n|---|---:|\n");
    sb.append("| Total queries | ").append(total).append(" |\n");
    sb.append("| Passed | ").append(passed).append(" |\n");
    sb.append("| Failed | ").append(failed).append(" |\n");
    sb.append("| No reference | ").append(noRef).append(" |\n");
    sb.append("| Snapshots written | ").append(snapped).append(" |\n");
    if (skipped > 0) sb.append("| Skipped | ").append(skipped).append(" |\n");
    sb.append(String.format(Locale.ENGLISH, "| Pass rate | **%.1f%%** (of %d comparable) |%n",
        passPct, comparable));
    sb.append(String.format(Locale.ENGLISH, "| Total time | %.1fs |%n%n", totalMs / 1000.0));

    sb.append("## Per-query results\n\n");
    sb.append("| # | Status | SQL | Test Results |\n");
    sb.append("|---:|:--|---|---|\n");
    for (Result r : results) {
      String sqlCell = r.sql == null ? "" : cell(clip(r.sql, 100));
      String resultCell = "";
      if (r.status == Status.PASS || r.status == Status.SNAPSHOT) {
        resultCell = r.rowCount + " row(s) returned";
      } else if (r.error != null) {
        resultCell = cell(clip(r.error, 200));
      } else if (r.diff != null) {
        resultCell = cell(clip(r.diff, 200));
      }
      sb.append("| ").append(r.id).append(" | ").append(r.status.label).append(" | ")
          .append(sqlCell).append(" | ").append(resultCell)
          .append(" |\n");
    }
    sb.append("\n");

    // Mismatch details section
    List<Result> mismatches = results.stream()
        .filter(r -> r.status == Status.FAIL && r.diff != null).toList();
    if (!mismatches.isEmpty()) {
      sb.append("## Mismatch details\n\n");
      for (Result r : mismatches) {
        sb.append("### q").append(r.id).append("\n\n");
        sb.append("```sql\n").append(r.sql).append("\n```\n\n");
        sb.append("```\n").append(r.diff).append("\n```\n\n");
      }
    }

    // Errors section
    List<Result> errors = results.stream()
        .filter(r -> r.status == Status.FAIL && r.error != null).toList();
    if (!errors.isEmpty()) {
      sb.append("## Errors\n\n");
      for (Result r : errors) {
        sb.append("### q").append(r.id).append("\n\n");
        sb.append("```sql\n").append(r.sql).append("\n```\n\n");
        sb.append("```\n").append(r.error).append("\n```\n\n");
      }
    }

    Files.writeString(reportFile, sb.toString(), StandardCharsets.UTF_8);
    logger.info("Wrote TPC-H SQL correctness report: {}", reportFile.toAbsolutePath());
    logger.info(
        "TPC-H correctness: {}/{} passed ({}%), {} failed, {} no-ref, {} snapshotted",
        passed, comparable,
        String.format(Locale.ENGLISH, "%.1f", passPct),
        failed, noRef, snapped);
  }

  // ── Helpers (mirrored from SQLTpchIT) ────────────────────────────────

  /** Extract the contents of the first /* ... *‍/ block in the file. */
  private static String extractSqlFromComment(String fileBody) {
    Matcher m = SQL_BLOCK.matcher(fileBody);
    if (!m.find()) return null;
    String body = m.group(1).replaceAll("\\s+", " ").trim();
    if (body.endsWith(";")) body = body.substring(0, body.length() - 1).trim();
    return body;
  }

  /** Pull "{type}: {reason/details}" from the error JSON. Caller clips for table cells. */
  /** Pull concise error reason from the error JSON. */
  private static String errorSummary(JSONObject error) {
    String reason = error.optString("reason", "");
    if (reason.isEmpty()) reason = error.optString("details", "");
    return reason;
  }

  private static String summarize(Exception e) {
    // Extract meaningful error from the exception message.
    // The RuntimeException wraps a ResponseException whose message format is:
    // "method [...], host [...], URI [...], status line [HTTP/1.1 NNN Reason] actual error text"
    // We extract just the "actual error text" part.
    String fullMsg = e.getMessage();
    if (fullMsg == null) fullMsg = "";

    // Try to extract from the message directly (works for both ResponseException and RuntimeException wrapping it)
    if (fullMsg.contains("status line [")) {
      int statusIdx = fullMsg.indexOf("status line [");
      // Find the closing "] " of the status line bracket
      int bracketStart = statusIdx + "status line [".length();
      int bracketEnd = fullMsg.indexOf(']', bracketStart);
      if (bracketEnd >= 0) {
        String afterBracket = fullMsg.substring(bracketEnd + 1).trim();
        // Response body might be JSON on next line or plain text
        if (afterBracket.startsWith("\n") || afterBracket.startsWith("\r")) {
          afterBracket = afterBracket.trim();
        }
        if (!afterBracket.isEmpty()) {
          // Try to parse as JSON error
          try {
            JSONObject json = new JSONObject(afterBracket);
            if (json.has("error")) {
              Object err = json.get("error");
              if (err instanceof JSONObject errObj) {
                return errorSummary(errObj);
              }
              return err.toString();
            }
          } catch (Exception ignored) {
            // Not JSON — use as plain text
          }
          return afterBracket;
        }
      }
    }

    // Also try getCause() for ResponseException
    Throwable cause = e.getCause();
    if (cause != null && cause.getMessage() != null && cause.getMessage().contains("status line [")) {
      return summarize(new RuntimeException(cause.getMessage()));
    }

    fullMsg = fullMsg.replace('\n', ' ').replace('\r', ' ').trim();
    return e.getClass().getSimpleName() + ": " + fullMsg;
  }

  private static String clip(String s, int max) {
    if (s == null) return "";
    return s.length() > max ? s.substring(0, max - 1) + "…" : s;
  }

  private static String cell(String s) {
    if (s == null) return "";
    return s.replace('|', '/').replace('\n', ' ').replace('\r', ' ').trim();
  }

  private static Path resolveReportDir() {
    String projectRoot = System.getProperty("project.root");
    Path base =
        projectRoot != null && !projectRoot.isEmpty()
            ? Paths.get(projectRoot)
            : Paths.get("").toAbsolutePath();
    return base.resolve("build/reports/tpch-sql-correctness");
  }

  /**
   * Mirrors {@link org.opensearch.sql.ppl.PPLIntegTestCase#loadFromFile} for resources on the test
   * classpath.
   */
  private static String loadFromFile(String filename) {
    try {
      URI uri = Resources.getResource(filename).toURI();
      return new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // ── PODs ───────────────────────────────────────────────────────────────────

  private record Result(
      int id, Status status, String sql, long elapsedMs, int rowCount, String error, String diff) {
    static Result of(
        int id, Status status, String sql, long ms, int rowCount, String error, String diff) {
      return new Result(id, status, sql, ms, rowCount, error, diff);
    }
  }

  private enum Status {
    PASS("✓"),
    FAIL("✗"),
    SKIP("-"),
    NO_REFERENCE("?"),
    SNAPSHOT("✓");

    final String label;

    Status(String label) {
      this.label = label;
    }
  }
}
