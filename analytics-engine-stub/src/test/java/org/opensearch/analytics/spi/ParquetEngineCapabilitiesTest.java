/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.spi;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParquetEngineCapabilitiesTest {

  private ParquetEngineCapabilities capabilities;

  @BeforeEach
  void setUp() {
    capabilities = new ParquetEngineCapabilities();
  }

  @Test
  void testSupportsLogicalFilter() {
    assertTrue(capabilities.supportsOperator(LogicalFilter.class));
  }

  @Test
  void testSupportsLogicalProject() {
    assertTrue(capabilities.supportsOperator(LogicalProject.class));
  }

  @Test
  void testSupportsLogicalAggregate() {
    assertTrue(capabilities.supportsOperator(LogicalAggregate.class));
  }

  @Test
  void testSupportsLogicalSort() {
    assertTrue(capabilities.supportsOperator(LogicalSort.class));
  }

  @Test
  void testDoesNotSupportUnsupportedOperator() {
    assertFalse(capabilities.supportsOperator(LogicalJoin.class));
  }

  @Test
  void testSupportsStandardFunctions() {
    assertTrue(capabilities.supportsFunction("COUNT"));
    assertTrue(capabilities.supportsFunction("SUM"));
    assertTrue(capabilities.supportsFunction("UPPER"));
    assertTrue(capabilities.supportsFunction("LOWER"));
    assertTrue(capabilities.supportsFunction("SUBSTRING"));
  }

  @Test
  void testDoesNotSupportFullTextSearchFunctions() {
    assertFalse(capabilities.supportsFunction("match"));
    assertFalse(capabilities.supportsFunction("match_phrase"));
    assertFalse(capabilities.supportsFunction("match_bool_prefix"));
  }
}
