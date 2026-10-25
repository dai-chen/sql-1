/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CalciteExecAggregatorEstimateTest {

  @Test
  void empty_row_costs_array_header_only() {
    assertEquals(16L, CalciteExecAggregator.estimateRowBytes(new Object[] {}));
  }

  @Test
  void null_values_cost_nothing_beyond_header() {
    assertEquals(16L, CalciteExecAggregator.estimateRowBytes(new Object[] {null, null, null}));
  }

  @Test
  void string_estimate_is_40_plus_2_times_length() {
    Object[] row = new Object[] {"hello"};
    // 16 (header) + 40 + 2*5 = 66
    assertEquals(66L, CalciteExecAggregator.estimateRowBytes(row));
  }

  @Test
  void long_and_double_cost_16_each() {
    Object[] row = new Object[] {42L, 3.14};
    // 16 (header) + 16 + 16 = 48
    assertEquals(48L, CalciteExecAggregator.estimateRowBytes(row));
  }

  @Test
  void mixed_row_estimate() {
    Object[] row = new Object[] {"test", 123L, null, true};
    // 16 (header) + (40 + 2*4=48) + 16 + 0 + 16 = 96
    assertEquals(96L, CalciteExecAggregator.estimateRowBytes(row));
  }

  @Test
  void estimate_is_positive_for_nonempty_rows() {
    Object[] row = new Object[] {"a", 1, 2L, 3.0, true};
    assertTrue(CalciteExecAggregator.estimateRowBytes(row) > 0);
  }
}
