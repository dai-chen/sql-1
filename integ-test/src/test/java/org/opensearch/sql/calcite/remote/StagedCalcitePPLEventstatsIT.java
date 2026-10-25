/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.After;
import org.opensearch.sql.common.setting.Settings;

/**
 * Runs all CalcitePPLEventstatsIT tests through the staged execution path by disabling pushdown and
 * disallowing fallback. This exercises the end-to-end staged path (US-008): StagePlanner → staged
 * SearchRequest → InternalCalciteExec.reduce → CoordinatorTreeExecutor.
 *
 * <p>The US-010 baseline measured 26/27 failing here with {@code CannotPlanException} and its
 * class-level {@code @Ignore} named US-015 as the unblocker. That attribution was wrong and the
 * annotation had gone stale: eventstats has no rank filter, so rule 3 never applies to it. What
 * actually fixed these 27 tests was US-014 adding {@code PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW} to
 * {@link org.opensearch.sql.opensearch.stage.CoordinatorTreeExecutor}, which lets the coordinator
 * run the CONCAT-path window. Verified during US-015 by stashing the US-015 production sources and
 * re-running: 27/0/0 at HEAD as well. The annotation is therefore deleted rather than re-dated.
 */
public class StagedCalcitePPLEventstatsIT extends CalcitePPLEventstatsIT {

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
