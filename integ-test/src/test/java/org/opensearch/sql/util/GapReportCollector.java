/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.opensearch.sql.executor.QueryType;

/**
 * Accumulates gap analysis results across all test methods and prints a categorized report grouped
 * by error message on JVM shutdown.
 */
public class GapReportCollector {

  private static final ConcurrentLinkedQueue<Entry> entries = new ConcurrentLinkedQueue<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(GapReportCollector::printReport));
  }

  private static class Entry {
    final String testClass;
    final String testMethod;
    final String query;
    final QueryType queryType;
    final UnifiedQueryGapAnalyzer.GapResult result;

    Entry(
        String testClass,
        String testMethod,
        String query,
        QueryType queryType,
        UnifiedQueryGapAnalyzer.GapResult result) {
      this.testClass = testClass;
      this.testMethod = testMethod;
      this.query = query;
      this.queryType = queryType;
      this.result = result;
    }
  }

  public static void collect(
      String testClass,
      String testMethod,
      String query,
      QueryType queryType,
      UnifiedQueryGapAnalyzer.GapResult result) {
    if (result != null) {
      entries.add(new Entry(testClass, testMethod, query, queryType, result));
    }
  }

  public static void printReport() {
    if (entries.isEmpty()) {
      return;
    }
    List<Entry> all = new ArrayList<>(entries);
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("=".repeat(80)).append("\n");
    sb.append("  UNIFIED QUERY GAP ANALYSIS REPORT\n");
    sb.append("=".repeat(80)).append("\n\n");

    for (QueryType qt : QueryType.values()) {
      List<Entry> typed = all.stream().filter(e -> e.queryType == qt).toList();
      if (typed.isEmpty()) continue;

      long success = typed.stream().filter(e -> e.result.success).count();
      long failed = typed.size() - success;
      sb.append(
          String.format(
              "--- %s: %d total, %d success (%.1f%%), %d failed (%.1f%%) ---\n",
              qt,
              typed.size(),
              success,
              100.0 * success / typed.size(),
              failed,
              100.0 * failed / typed.size()));
      sb.append("\n");

      // Group failures by error message
      Map<String, List<Entry>> groups = new LinkedHashMap<>();
      for (Entry e : typed) {
        if (!e.result.success) {
          String key = e.result.phase + " | " + e.result.errorMessage;
          groups.computeIfAbsent(key, k -> new ArrayList<>()).add(e);
        }
      }

      for (Map.Entry<String, List<Entry>> group : groups.entrySet()) {
        List<Entry> items = group.getValue();
        Entry sample = items.get(0);
        if (sample.result.phase
            == UnifiedQueryGapAnalyzer.GapResult.Phase.RESULT_MISMATCH) {
          sb.append(
              String.format(
                  "  [%s] Query succeeded but returned different results (%d occurrences)\n",
                  sample.result.phase, items.size()));
          for (Entry e : items) {
            sb.append(
                String.format(
                    "    - %s.%s: %s\n      Diff: %s\n",
                    e.testClass, e.testMethod, e.query, e.result.errorMessage));
          }
        } else {
          sb.append(
              String.format(
                  "  [%s] %s (%d occurrences)\n",
                  sample.result.phase, sample.result.errorMessage, items.size()));
          sb.append(String.format("  Exception: %s\n", sample.result.exceptionClass));
          for (Entry e : items) {
            sb.append(String.format("    - %s.%s: %s\n", e.testClass, e.testMethod, e.query));
          }
        }
        sb.append("\n");
      }
    }

    sb.append("=".repeat(80)).append("\n");
    System.err.println(sb);
  }
}
