/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.clickbench;

import com.google.common.io.Resources;
import java.io.IOException;
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
import org.json.JSONObject;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * ClickBench SQL pass-rate report.
 *
 * <p>Runs every ClickBench query through the SQL endpoint ({@code /_plugins/_sql}). Each query's
 * SQL form lives in the leading {@code /* ... *}{@code /} block of {@code clickbench/queries/qN.ppl};
 * we extract that text, send it, and tally pass/fail per query.
 *
 * <p>Designed for the analytics-engine compatibility story: with {@code
 * -Dtests.analytics.force_routing=true} the SQL ITs route every query through the analytics-engine
 * path. Without it the queries hit whichever path RestUnifiedQueryAction picks for each (legacy v2
 * for non-{@code parquet_*} indices in the default config).
 *
 * <p>The test always succeeds — the report itself is the deliverable.
 *
 * <p>Output: {@code integ-test/build/reports/clickbench-sql/REPORT.md}, with a one-row-per-query
 * status table, the inline SQL, and the failure cause (when present).
 */
@FixMethodOrder(MethodSorters.JVM)
public class SQLClickBenchIT extends SQLIntegTestCase {

  /** Number of ClickBench queries (q1..qN). The set ships 43 queries. */
  private static final int N_QUERIES = 43;

  /** Matches the leading {@code /* ... *}{@code /} block in each .ppl file. */
  private static final Pattern SQL_BLOCK = Pattern.compile("/\\*\\s*(.*?)\\s*\\*/", Pattern.DOTALL);

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.CLICK_BENCH);
  }

  @Test
  public void runAllSqlQueries() throws IOException {
    List<Result> results = new ArrayList<>();
    for (int i = 1; i <= N_QUERIES; i++) {
      String fileBody = loadFromFile("clickbench/queries/q" + i + ".ppl");
      String sql = extractSqlFromComment(fileBody);
      if (sql == null || sql.isBlank()) {
        results.add(Result.skipped(i, "no SQL block in q" + i + ".ppl"));
        continue;
      }
      long start = System.currentTimeMillis();
      try {
        JSONObject response = executeQuery(sql);
        long elapsedMs = System.currentTimeMillis() - start;
        if (response.has("error")) {
          // Some failure modes return 200 with an error envelope rather than throwing.
          results.add(Result.failed(i, sql, elapsedMs, errorSummary(response.getJSONObject("error"))));
        } else {
          results.add(Result.passed(i, sql, elapsedMs, rowCount(response)));
        }
      } catch (Exception e) {
        long elapsedMs = System.currentTimeMillis() - start;
        results.add(Result.failed(i, sql, elapsedMs, summarize(e)));
      }
    }
    writeReport(results);
  }

  /** Extract the contents of the first /* ... *‍/ block in the file. */
  private static String extractSqlFromComment(String fileBody) {
    Matcher m = SQL_BLOCK.matcher(fileBody);
    if (!m.find()) {
      return null;
    }
    // Collapse newlines into spaces and strip the trailing semicolon — the SQL endpoint accepts
    // both forms but a one-line preview is friendlier in the report.
    String body = m.group(1).replaceAll("\\s+", " ").trim();
    if (body.endsWith(";")) {
      body = body.substring(0, body.length() - 1).trim();
    }
    return body;
  }

  private static int rowCount(JSONObject response) {
    if (response.has("datarows")) {
      return response.getJSONArray("datarows").length();
    }
    if (response.has("total")) {
      return response.optInt("total", 0);
    }
    return 0;
  }

  /** Pull "{type}: {reason/details}" from the error JSON. Caller clips for table cells. */
  private static String errorSummary(JSONObject error) {
    String type = error.optString("type", "<unknown>");
    String details = error.optString("details", "");
    if (details.isEmpty()) {
      details = error.optString("reason", "");
    }
    return type + ": " + details;
  }

  /**
   * Reduce an exception to a single line: simple class name + message. For ResponseException, the
   * message includes the full HTTP status line + JSON body — we keep it all so the failure-samples
   * section in the report can show the actual cause; the per-row cell clips separately.
   */
  private static String summarize(Exception e) {
    String msg = e.getMessage();
    if (msg == null) msg = "";
    msg = msg.replace('\n', ' ').replace('\r', ' ').trim();
    return e.getClass().getSimpleName() + ": " + msg;
  }

  private static String clip(String s, int max) {
    if (s == null) return "";
    return s.length() > max ? s.substring(0, max - 1) + "…" : s;
  }

  private static String cell(String s) {
    if (s == null) return "";
    return s.replace('|', '/').replace('\n', ' ').replace('\r', ' ').trim();
  }

  private void writeReport(List<Result> results) throws IOException {
    int passed = 0, failed = 0, skipped = 0;
    long totalMs = 0;
    for (Result r : results) {
      switch (r.status) {
        case PASS -> {
          passed++;
          totalMs += r.elapsedMs;
        }
        case FAIL -> {
          failed++;
          totalMs += r.elapsedMs;
        }
        case SKIP -> skipped++;
      }
    }
    int total = results.size();
    double passPct = total == 0 ? 0.0 : (100.0 * passed / total);

    Path reportDir = resolveReportDir();
    Files.createDirectories(reportDir);
    Path reportFile = reportDir.resolve("REPORT.md");

    StringBuilder sb = new StringBuilder();
    sb.append("# ClickBench SQL Compatibility Report\n\n");
    sb.append("Generated: ").append(new java.util.Date()).append("\n\n");
    sb.append("Cluster under test: `")
        .append(System.getProperty("tests.rest.cluster", "<unspecified>"))
        .append("`\n\n");
    boolean forceRouting =
        Boolean.parseBoolean(System.getProperty("tests.analytics.force_routing", "false"));
    sb.append("Routing: ")
        .append(
            forceRouting
                ? "every SQL query forced through the analytics-engine path "
                    + "(`tests.analytics.force_routing=true`)."
                : "default — `RestUnifiedQueryAction.isAnalyticsIndex` decides per query "
                    + "(legacy v2 path for non-`parquet_*` indices).")
        .append("\n\n");

    sb.append("## Summary\n\n");
    sb.append("| Metric | Value |\n|---|---:|\n");
    sb.append("| Queries run | ").append(total).append(" |\n");
    sb.append("| Passed | ").append(passed).append(" |\n");
    sb.append("| Failed | ").append(failed).append(" |\n");
    if (skipped > 0) {
      sb.append("| Skipped | ").append(skipped).append(" |\n");
    }
    sb.append(
            String.format(Locale.ENGLISH, "| Pass rate | **%.1f%%** |%n", passPct));
    sb.append(
            String.format(
                Locale.ENGLISH, "| Total time | %.1fs |%n%n", totalMs / 1000.0));

    sb.append("## Per-query results\n\n");
    sb.append("| # | Status | Time (ms) | SQL | Error |\n");
    sb.append("|---:|:--|---:|---|---|\n");
    for (Result r : results) {
      String sqlCell = r.sql == null ? "" : cell(clip(r.sql, 100));
      String errCell = r.error == null ? "" : cell(clip(r.error, 100));
      String time = r.status == Status.SKIP ? "" : Long.toString(r.elapsedMs);
      sb.append("| ").append(r.id).append(" | ").append(r.status.label).append(" | ")
          .append(time).append(" | ").append(sqlCell).append(" | ").append(errCell)
          .append(" |\n");
    }
    sb.append("\n");

    if (failed > 0) {
      sb.append("## Failure samples\n\n");
      sb.append(
          "Full SQL + full error message for each failing query (un-clipped) — useful when the "
              + "table cells above truncate.\n\n");
      for (Result r : results) {
        if (r.status != Status.FAIL) continue;
        sb.append("### q").append(r.id).append("\n\n");
        sb.append("```sql\n").append(r.sql).append("\n```\n\n");
        sb.append("```\n").append(r.error).append("\n```\n\n");
      }
    }

    Files.writeString(reportFile, sb.toString(), StandardCharsets.UTF_8);
    logger.info("Wrote ClickBench SQL report: {}", reportFile.toAbsolutePath());
    logger.info(
        "ClickBench SQL: {}/{} passed ({}%), {} failed, {} skipped",
        passed, total, String.format(Locale.ENGLISH, "%.1f", passPct), failed, skipped);
  }

  /**
   * The test JVM's working dir varies (Gradle daemon, IDE), so locate the report dir relative to
   * the project root we're handed via system property.
   */
  private static Path resolveReportDir() {
    String projectRoot = System.getProperty("project.root");
    Path base =
        projectRoot != null && !projectRoot.isEmpty()
            ? Paths.get(projectRoot)
            : Paths.get("").toAbsolutePath();
    return base.resolve("build/reports/clickbench-sql");
  }

  /** Small POD for collected per-query results. */
  private record Result(
      int id, Status status, String sql, long elapsedMs, String error, int rowCount) {

    static Result passed(int id, String sql, long ms, int rows) {
      return new Result(id, Status.PASS, sql, ms, null, rows);
    }

    static Result failed(int id, String sql, long ms, String error) {
      return new Result(id, Status.FAIL, sql, ms, error, 0);
    }

    static Result skipped(int id, String reason) {
      return new Result(id, Status.SKIP, null, 0, reason, 0);
    }
  }

  private enum Status {
    PASS("✓ pass"),
    FAIL("✗ fail"),
    SKIP("- skip");

    final String label;

    Status(String label) {
      this.label = label;
    }
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
}
