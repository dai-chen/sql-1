/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.spi;

import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 * Mock Parquet engine capabilities — supports basic relational operators and standard functions.
 */
public class ParquetEngineCapabilities implements EngineCapabilities {

  private static final Set<Class<? extends RelNode>> SUPPORTED_OPERATORS =
      Set.of(LogicalFilter.class, LogicalProject.class, LogicalAggregate.class, LogicalSort.class);

  private static final Set<String> SUPPORTED_FUNCTIONS =
      Set.of(
          "+",
          "-",
          "*",
          "/",
          "=",
          "<>",
          "<",
          ">",
          "<=",
          ">=",
          "UPPER",
          "LOWER",
          "SUBSTRING",
          "TRIM",
          "CONCAT",
          "COUNT",
          "SUM",
          "AVG",
          "MIN",
          "MAX");

  private static final Set<String> UNSUPPORTED_FUNCTIONS =
      Set.of(
          "match",
          "match_phrase",
          "match_bool_prefix",
          "match_phrase_prefix",
          "multi_match",
          "simple_query_string",
          "query_string");

  @Override
  public boolean supportsOperator(Class<? extends RelNode> operatorClass) {
    return SUPPORTED_OPERATORS.contains(operatorClass);
  }

  @Override
  public boolean supportsFunction(String functionName) {
    return !UNSUPPORTED_FUNCTIONS.contains(functionName)
        && SUPPORTED_FUNCTIONS.contains(functionName);
  }
}
