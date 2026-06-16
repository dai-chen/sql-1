/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import org.junit.Assume;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Capability gate for integration tests. Tests declare the {@link Capability} they need — via the
 * {@link RequiresCapability} annotation or {@code requireCapability(...)} — and are skipped on a
 * backend that lacks it.
 */
public final class BackendCapabilities {

  /**
   * Skip the current test when the active backend lacks the required {@link Capability} (no-op
   * otherwise). Today the analytics-engine route (DataFusion/parquet, enabled by {@code
   * -Dtests.analytics.parquet_indices=true}) supports none of the defined capabilities, so any
   * required capability is skipped whenever that route is active. The capability's {@code reason()}
   * — plus an optional call-site {@code note} — becomes the skip message.
   */
  public static void requireCapability(Capability capability) {
    requireCapability(capability, null);
  }

  public static void requireCapability(Capability capability, String note) {
    Assume.assumeTrue(skipMessage(capability, note), !TestUtils.AnalyticsIndexConfig.isEnabled());
  }

  private static String skipMessage(Capability capability, String note) {
    String base = capability.name() + ": " + capability.reason();
    return (note == null || note.isBlank()) ? base : base + " — " + note;
  }

  private BackendCapabilities() {}
}
