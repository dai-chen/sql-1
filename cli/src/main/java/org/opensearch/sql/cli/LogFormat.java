/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Getter;

/** Registry of pre-built log format parsers. */
public class LogFormat {

  @Getter private final String name;
  @Getter private final Pattern pattern;
  @Getter private final String[] columns;

  private static final Map<String, LogFormat> REGISTRY = new LinkedHashMap<>();

  static {
    register(
        new LogFormat(
            "opensearch-log",
            Pattern.compile(
                "\\[([^\\]]+)\\]\\[([^\\]]+?)\\s*\\]\\[([^\\]]+?)\\s*\\]\\s*\\[([^\\]]+?)\\s*\\]\\s*(.*)",
                Pattern.DOTALL),
            new String[] {"timestamp", "level", "component", "node", "message"}));
  }

  public LogFormat(String name, Pattern pattern, String[] columns) {
    this.name = name;
    this.pattern = pattern;
    this.columns = columns;
  }

  public static LogFormat get(String name) {
    return REGISTRY.get(name);
  }

  public static void register(LogFormat format) {
    REGISTRY.put(format.getName(), format);
  }

  public static Map<String, LogFormat> all() {
    return Map.copyOf(REGISTRY);
  }
}
