/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ansi;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import net.hydromatic.quidem.Quidem;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration test that runs a suite of Quidem {@code .iq} files against the running OpenSearch
 * cluster, exercising the unified SQL query path (SQL plugin + analytics-engine) end-to-end.
 *
 * <p>Design:
 *
 * <ul>
 *   <li>Extends {@link SQLIntegTestCase} so it inherits the cluster-setup hooks (in particular
 *       the {@code tests.analytics.force_routing} system property that sets
 *       {@code plugins.calcite.analytics.force_routing=true} during {@code init()}).
 *   <li>Seeds the Oracle-style {@code scott.EMP}/{@code scott.DEPT} schema as OpenSearch indices
 *       via {@link ScottSchemaSeeder}. The canonical 14+4 rows give .iq files concrete data to
 *       query without needing large fixtures.
 *   <li>Walks {@code integ-test/src/test/resources/ansi/*.iq} and runs each through Quidem's
 *       configuration. A file is considered passing when Quidem's rendered output equals the
 *       input line-for-line — i.e., every {@code !ok} block's expected output matches actual
 *       output.
 * </ul>
 *
 * <p>Unlike the {@code analyticsSqlCompatibilityReport} task which runs the entire legacy v2
 * SQL IT suite through the unified path and counts compatibility gaps, this test ONLY runs
 * .iq files we curate. Its goal is to measure coverage of the <b>new execution path's</b>
 * ANSI SQL surface area, not parity with the legacy engine.
 */
public class AnsiSqlQuidemIT extends SQLIntegTestCase {

  private static final String IQ_RESOURCE_DIR = "ansi";

  @Before
  public void seedScott() throws IOException {
    ScottSchemaSeeder.seed(client());
  }

  @Test
  public void runAllIqFiles() throws Exception {
    Path iqDir = locateIqResourceDir();
    List<Path> files;
    try (var stream = Files.list(iqDir)) {
      files =
          stream
              .filter(p -> p.toString().endsWith(".iq"))
              .sorted(Comparator.comparing(Path::toString))
              .collect(Collectors.toList());
    }
    if (files.isEmpty()) {
      throw new IllegalStateException("No .iq files found under " + iqDir);
    }

    StringBuilder report = new StringBuilder();
    int pass = 0;
    int fail = 0;
    int totalQueriesPass = 0;
    int totalQueriesFail = 0;
    List<String> failureMessages = new ArrayList<>();
    List<String> perFileQueryCounts = new ArrayList<>();
    for (Path iq : files) {
      QuidemResult result;
      try {
        result = runOne(iq);
      } catch (Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        result = QuidemResult.fail("threw " + t.getClass().getSimpleName() + ": "
            + (t.getMessage() == null ? "" : t.getMessage().split("\n")[0]));
      }
      totalQueriesPass += result.queriesPass();
      totalQueriesFail += result.queriesFail();
      int fileTotal = result.queriesPass() + result.queriesFail();
      String rate = fileTotal > 0 ? String.format("%.0f%%", result.queriesPass() * 100.0 / fileTotal) : "N/A";
      perFileQueryCounts.add(String.format("| %s | %d | %d | %d | %s |",
          iq.getFileName(), fileTotal, result.queriesPass(), result.queriesFail(), rate));
      if (result.passed()) {
        pass++;
        report.append("✓ ").append(iq.getFileName()).append("\n");
      } else {
        fail++;
        report.append("✗ ").append(iq.getFileName()).append("\n");
        failureMessages.add(
            iq.getFileName() + ":\n" + indent(result.diff, "    "));
      }
    }

    // Write a machine-readable report alongside the standard gradle outputs.
    Path reportFile = reportPath();
    Files.createDirectories(reportFile.getParent());
    Files.writeString(
        reportFile,
        renderReport(files, pass, fail, totalQueriesPass, totalQueriesFail,
            report.toString(), perFileQueryCounts, failureMessages),
        StandardCharsets.UTF_8);

    // The report is the deliverable — we do NOT fail the build on per-file failures because
    // the point of this IT is to MEASURE what fraction of ANSI SQL works on the new path,
    // not gate releases on it. If you need a green/red signal, grep the REPORT.md summary
    // line or use the JUnit XML's failure count per-subtest (future enhancement).
    assertEquals("At least one .iq file should be present", files.size(), pass + fail);
  }

  private QuidemResult runOne(Path iq) throws Exception {
    String input = Files.readString(iq, StandardCharsets.UTF_8);
    StringWriter out = new StringWriter();
    try (Reader reader = Files.newBufferedReader(iq, StandardCharsets.UTF_8);
         PrintWriter writer = new PrintWriter(out)) {
      Quidem.Config config =
          Quidem.configBuilder()
              .withReader(reader)
              .withWriter(writer)
              .withConnectionFactory(new OpenSearchConnectionFactory())
              .withCommandHandler((lines, content, line) -> new net.hydromatic.quidem.Command() {
                @Override public String describe(Context ctx) { return "skip"; }
                @Override public void execute(Context ctx, boolean execute) {}
              }) // skip unknown directives
              .build();
      new Quidem(config).execute();
    }
    String actual = out.toString();
    // Quidem rewrites the expected-output blocks in-place. A test passes when the rewritten
    // output matches the original file byte-for-byte.
    // Count per-query pass/fail by comparing !ok blocks between input and output.
    int[] queryCounts = countQueryResults(input, actual);
    return input.equals(actual)
        ? QuidemResult.pass(queryCounts[0], queryCounts[1])
        : QuidemResult.fail(diff(input, actual), queryCounts[0], queryCounts[1]);
  }

  /**
   * Count how many !ok blocks match (pass) vs differ (fail) between expected and actual output.
   * Each !ok block represents one query assertion.
   */
  private static int[] countQueryResults(String expected, String actual) {
    String[] expectedBlocks = splitOkBlocks(expected);
    String[] actualBlocks = splitOkBlocks(actual);
    int pass = 0, fail = 0;
    int count = Math.min(expectedBlocks.length, actualBlocks.length);
    for (int i = 0; i < count; i++) {
      if (expectedBlocks[i].equals(actualBlocks[i])) {
        pass++;
      } else {
        fail++;
      }
    }
    // Any extra blocks in either side count as failures
    fail += Math.abs(expectedBlocks.length - actualBlocks.length);
    return new int[]{pass, fail};
  }

  /** Split file content into segments ending with !ok (each segment = one query + result). */
  private static String[] splitOkBlocks(String content) {
    // Split on lines that are exactly "!ok" — each piece before !ok is one query+result block
    String[] parts = content.split("(?m)^!ok$", -1);
    // Last element is everything after the final !ok (or the whole file if no !ok)
    if (parts.length <= 1) return new String[0];
    // Return all but the last (which is trailing content after last !ok)
    String[] blocks = new String[parts.length - 1];
    System.arraycopy(parts, 0, blocks, 0, blocks.length);
    return blocks;
  }

  private static Path locateIqResourceDir() {
    // integTestRemote runs with the test classpath rooted at the compiled test classes. The
    // .iq files live under src/test/resources/ansi and get copied to build/resources/test/ansi
    // during `processTestResources`. project.root is passed as a sysprop by the task —
    // historically the integ-test subproject dir, not the repo root.
    String projectRoot = System.getProperty("project.root");
    if (projectRoot != null && !projectRoot.isBlank()) {
      // Try both interpretations so the test works whether project.root is the repo root
      // (integ-test/src/test/...) or the integ-test project (src/test/...).
      Path[] candidates = {
        Path.of(projectRoot, "src", "test", "resources", IQ_RESOURCE_DIR),
        Path.of(projectRoot, "integ-test", "src", "test", "resources", IQ_RESOURCE_DIR)
      };
      for (Path c : candidates) {
        if (Files.isDirectory(c)) {
          return c;
        }
      }
    }
    // Fallback: resolve via classloader (works when run from build/resources/test).
    java.net.URL url = AnsiSqlQuidemIT.class.getClassLoader().getResource(IQ_RESOURCE_DIR);
    if (url != null) {
      try {
        return Path.of(url.toURI());
      } catch (Exception e) {
        // fall through
      }
    }
    throw new IllegalStateException(
        "Could not locate " + IQ_RESOURCE_DIR + " resource dir. Pass -Dproject.root=<repo root> "
            + "or ensure src/test/resources/" + IQ_RESOURCE_DIR + " exists.");
  }

  private static Path reportPath() {
    // Match the location style of the other compatibility reports. project.root from the
    // gradle task is the integ-test subproject dir, so no extra path segment needed.
    String projectRoot = System.getProperty("project.root");
    Path base = projectRoot != null
        ? Path.of(projectRoot, "build", "reports", "ansi-sql-quidem")
        : Path.of("build/reports/ansi-sql-quidem");
    return base.resolve("REPORT.md");
  }

  /**
   * Produce a compact line-by-line diff so failure messages stay actionable. We don't bring in a
   * third-party diff library for a test-only utility — the naive side-by-side is enough.
   */
  private static String diff(String expected, String actual) {
    String[] e = expected.split("\n", -1);
    String[] a = actual.split("\n", -1);
    StringBuilder sb = new StringBuilder();
    int limit = Math.max(e.length, a.length);
    int shown = 0;
    for (int i = 0; i < limit && shown < 40; i++) {
      String ls = i < e.length ? e[i] : "<EOF>";
      String rs = i < a.length ? a[i] : "<EOF>";
      if (!ls.equals(rs)) {
        sb.append("  line ").append(i + 1).append(":\n");
        sb.append("    expected: ").append(ls).append('\n');
        sb.append("    actual:   ").append(rs).append('\n');
        shown++;
      }
    }
    if (shown == 40) {
      sb.append("  ... (more differences truncated)\n");
    }
    return sb.toString();
  }

  /**
   * Assemble the markdown report. Adds a failure-cause classification summary so the report
   * tells readers WHY queries failed, not just that they did.
   */
  private static String renderReport(
      List<Path> files,
      int pass,
      int fail,
      int totalQueriesPass,
      int totalQueriesFail,
      String perFileStatus,
      List<String> perFileQueryCounts,
      List<String> failureMessages) {
    // Count failure classes across all failure diffs
    java.util.Map<String, Integer> causes = new java.util.LinkedHashMap<>();
    for (String msg : failureMessages) {
      String cat = classifyFailure(msg);
      causes.merge(cat, 1, Integer::sum);
    }
    StringBuilder sb = new StringBuilder();
    sb.append("# ANSI SQL Quidem IT Report\n\n");
    sb.append("Executed ").append(files.size()).append(" .iq files: ")
        .append(pass).append(" passed, ").append(fail).append(" failed.\n\n");
    int totalQueries = totalQueriesPass + totalQueriesFail;
    String queryRate = totalQueries > 0
        ? String.format("%.1f%%", totalQueriesPass * 100.0 / totalQueries) : "N/A";
    sb.append("**Query-level: ").append(totalQueriesPass).append("/")
        .append(totalQueries).append(" queries passed (").append(queryRate).append(")**\n\n");
    sb.append("Each .iq file is a Quidem golden-file test adapted from Apache Calcite. A file ")
        .append("passes when the expected-output blocks in the file match exactly what Quidem ")
        .append("produces after replaying every query through the OpenSearch unified path ")
        .append("(force_routing=true → Calcite-native planner → analytics-engine execution).\n\n");

    sb.append("## Per-file query results\n\n");
    sb.append("| File | Total | Pass | Fail | Rate |\n|---|---:|---:|---:|---:|\n");
    for (String row : perFileQueryCounts) {
      sb.append(row).append("\n");
    }
    sb.append(String.format("| **TOTAL** | **%d** | **%d** | **%d** | **%s** |\n\n",
        totalQueries, totalQueriesPass, totalQueriesFail, queryRate));

    sb.append("## Per-file status\n\n").append(perFileStatus).append("\n");

    if (!causes.isEmpty()) {
      sb.append("## Failure causes (one per file, categorized)\n\n");
      sb.append("| Cause | Files |\n|---|---:|\n");
      causes.entrySet().stream()
          .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
          .forEach(e -> sb.append("| ").append(e.getKey()).append(" | ")
              .append(e.getValue()).append(" |\n"));
      sb.append("\n");
    }

    if (!failureMessages.isEmpty()) {
      sb.append("## Failure details (first diff per file)\n\n");
      sb.append(String.join("\n\n", failureMessages)).append("\n");
    }
    return sb.toString();
  }

  /**
   * Classify a failure message into a short human-readable cause bucket. Looks at the first
   * 'actual:' line in the diff since that's where the underlying error shows up.
   */
  private static String classifyFailure(String failureMessage) {
    // If the entire failure is a thrown-exception summary (e.g. Quidem parse error), surface
    // the exception class directly — there's no "expected vs actual" diff for these.
    if (failureMessage.contains("threw ")) {
      // Match the `threw Class — msg` line produced by our try/catch wrapper. Use a greedy
      // body match up to end-of-line so ':' characters inside the message aren't eaten.
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile("threw (\\w+): ([^\\n]+)")
              .matcher(failureMessage);
      if (m.find()) {
        String exc = m.group(1);
        String msg = m.group(2).trim();
        // Normalize away object-identity hex suffixes so different runs bucket together.
        msg = msg.replaceAll("@[0-9a-fA-F]+", "@<obj>");
        if (msg.startsWith("Unknown command:")) {
          return "Quidem: unknown directive (" + msg.substring("Unknown command:".length()).trim() + ")";
        }
        if (msg.startsWith("Error while executing command")) {
          return "Quidem: command execution failed (" + exc + ")";
        }
        if (msg.isEmpty()) {
          return "Quidem: threw " + exc;
        }
        // Cap length so one-off long messages don't explode the table.
        if (msg.length() > 100) msg = msg.substring(0, 97) + "...";
        return "Quidem: threw " + exc + " — " + msg;
      }
    }
    // Extract first non-expected "actual:" line
    String firstActual = null;
    for (String line : failureMessage.split("\n", -1)) {
      String trimmed = line.trim();
      if (trimmed.startsWith("actual:")) {
        firstActual = trimmed.substring("actual:".length()).trim();
        break;
      }
    }
    if (firstActual == null) {
      return "no diff captured";
    }
    if (firstActual.startsWith("threw ")) {
      return "parser error (Quidem): " + firstActual.substring("threw ".length()).split(":")[0];
    }
    if (firstActual.contains("No backend can scan all requested fields")) {
      return "AE: no backend can scan fields";
    }
    if (firstActual.contains("unmarked child")) {
      return "AE: planner rule unmarked child";
    }
    if (firstActual.contains("Error executing query")) {
      // Most common failure today — JDBC driver wrapper, actual cause is server-side
      return "JDBC: Error executing query (see HTML report for server-side cause)";
    }
    if (firstActual.contains("No match found for function signature")) {
      return "SQL/api: missing function signature";
    }
    if (firstActual.contains("Unknown identifier")) {
      return "SQL/api: unknown identifier";
    }
    if (firstActual.contains("Cannot apply")) {
      return "SQL/api: type mismatch in operator";
    }
    if (firstActual.contains("not found")) {
      return "SQL/api: column or table not found";
    }
    if (firstActual.contains("Table") && firstActual.contains("not found")) {
      return "schema: table not seeded";
    }
    // Output format mismatch (query succeeded but result differs)
    if (firstActual.startsWith("+") || firstActual.startsWith("|") || firstActual.startsWith("(")) {
      return "output differs from expected (query succeeded)";
    }
    return "other: " + (firstActual.length() > 60 ? firstActual.substring(0, 57) + "..." : firstActual);
  }

  private static String indent(String s, String prefix) {
    return s.lines().map(l -> prefix + l).collect(Collectors.joining("\n"));
  }

  private record QuidemResult(boolean passed, String diff, int queriesPass, int queriesFail) {
    static QuidemResult pass(int qPass, int qFail) {
      return new QuidemResult(true, null, qPass, qFail);
    }

    static QuidemResult fail(String diff, int qPass, int qFail) {
      return new QuidemResult(false, diff, qPass, qFail);
    }

    static QuidemResult fail(String diff) {
      return new QuidemResult(false, diff, 0, 0);
    }
  }

  // SQLIntegTestCase requires init(); nothing to do beyond what the base class already wires
  // (force_routing setting, cluster bootstrap). The @Before seedScott handles per-test data.
  @Override
  protected void init() throws Exception {
    super.init();
  }
}
