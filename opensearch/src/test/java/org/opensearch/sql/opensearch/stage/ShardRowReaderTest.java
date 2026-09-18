/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ShardRowReaderTest {

  @Test
  void blankNumericSourceValueIsNull() {
    assertNull(ShardRowReader.number(" ", Number::intValue));
  }

  @Test
  void numericSourceValueUsesRequestedType() {
    assertEquals(42, ShardRowReader.number("42", Number::intValue));
  }

  @Test
  void timestampMatchesExistingCalciteTransportFormat() {
    assertEquals("1970-01-01 00:00:00", ShardRowReader.timestamp(0L));
  }
}
