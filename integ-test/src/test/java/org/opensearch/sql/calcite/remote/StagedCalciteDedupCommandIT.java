/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.After;
import org.opensearch.sql.common.setting.Settings;

/**
 * Runs all CalciteDedupCommandIT tests through the staged execution path by disabling pushdown and
 * disallowing fallback. This exercises the end-to-end staged path (US-008): StagePlanner → staged
 * SearchRequest → InternalCalciteExec.reduce → CoordinatorTreeExecutor.
 *
 * <p>Green as of US-015: rule 3 promotes the dedup rank chain into the shard fragment, so the
 * window runs per shard under {@code RANK_LIMIT} instead of landing on the coordinator.
 */
public class StagedCalciteDedupCommandIT extends CalciteDedupCommandIT {

  @Override
  public void init() throws Exception {
    super.init();
    // Disable pushdown so the staged execution gate activates
    updateClusterSettings(
        new ClusterSetting(
            "transient", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "false"));
    // Disallow fallback so any staging failure surfaces as an error, never silent degradation
    updateClusterSettings(
        new ClusterSetting(
            "transient", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "false"));
  }

  @After
  public void tearDown() throws Exception {
    // Restore settings to null (cluster default) so subsequent IT classes are not poisoned
    updateClusterSettings(
        new ClusterSetting("transient", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), null));
    updateClusterSettings(
        new ClusterSetting("transient", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), null));
    super.tearDown();
  }
}
