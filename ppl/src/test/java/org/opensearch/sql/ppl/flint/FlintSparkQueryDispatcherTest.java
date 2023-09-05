/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.flint;

import org.junit.Test;

public class FlintSparkQueryDispatcherTest {

  private FlintSparkQueryDispatcher dispatcher = new FlintSparkQueryDispatcher();

  @Test
  public void test_create_skipping_index_dispatch() {
    dispatcher.dispatch("CREATE SKIPPING INDEX ON myS3.alb_logs (client_ip VALUE_SET) WITH (auto_refresh = true)");
  }

  @Test
  public void test_drop_skipping_index_dispatch() {
    // The job submitted above will be cancelled here
    dispatcher.dispatch("DROP SKIPPING INDEX ON myS3.alb_logs");
  }

  @Test
  public void test_create_covering_index() {
    dispatcher.dispatch("CREATE INDEX elb_and_requestUri ON myS3.alb_logs (elb, requestUri) WITH (auto_refresh = true)");
  }

  @Test
  public void test_create_materialized_view() {
    dispatcher.dispatch("CREATE MATERIALIZED VIEW myS3.elb_request_counts AS SELECT elb, COUNT(*) FROM myS3.alb_logs GROUP BY elb");
  }
}