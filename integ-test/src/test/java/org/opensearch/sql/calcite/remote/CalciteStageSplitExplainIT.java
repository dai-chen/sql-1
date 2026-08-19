/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Standalone IT that verifies the staged-explain contract without inheriting the full
 * CalciteExplainIT suite. Tests:
 *
 * <ol>
 *   <li>When pushdown is disabled, the explain response includes shardFragment, combine, and
 *       coordinatorTree.
 *   <li>When pushdown is enabled (default), those three keys are absent.
 * </ol>
 */
public class CalciteStageSplitExplainIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
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

  @Test
  public void testStagedExplainRendersThreeSections() throws IOException {
    // Disable pushdown so the staged execution gate activates
    updateClusterSettings(
        new ClusterSetting(
            "transient", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "false"));
    // Disallow fallback so any staging failure surfaces as an error
    updateClusterSettings(
        new ClusterSetting(
            "transient", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "false"));

    String query =
        "source=opensearch-sql_test_index_account | where age > 30 | fields age, firstname";
    String result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_staged_where_fields.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testNonStagedExplainOmitsStageSections() throws IOException {
    // Pushdown is enabled by default — the plan is NOT staged
    String query =
        "source=opensearch-sql_test_index_account | where age > 30 | fields age, firstname";
    String result = explainQueryYaml(query);

    Assert.assertFalse(
        "shardFragment must be absent when pushdown is enabled", result.contains("shardFragment:"));
    Assert.assertFalse(
        "combine must be absent when pushdown is enabled", result.contains("combine:"));
    Assert.assertFalse(
        "coordinatorTree must be absent when pushdown is enabled",
        result.contains("coordinatorTree:"));
  }
}
