/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMultisearchCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testMultisearchNonPushdownFallback() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | multisearch [source=%s | where age > 35] [source=%s | where age < 25]", 
        TEST_INDEX_BANK, TEST_INDEX_BANK, TEST_INDEX_BANK));
    
    verifySchema(result, 
        columnName("account_number"), 
        columnName("firstname"), 
        columnName("lastname"),
        columnName("age"),
        columnName("gender"),
        columnName("address"),
        columnName("employer"),
        columnName("email"),
        columnName("city"),
        columnName("state"));
  }

  @Test
  public void testMultisearchWithEvalNonPushdown() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | multisearch [source=%s | where age > 35 | eval category='senior'] [source=%s | where age < 25 | eval category='junior'] | fields firstname, lastname, age, category",
        TEST_INDEX_BANK, TEST_INDEX_BANK, TEST_INDEX_BANK));
    
    verifySchema(result, 
        columnName("firstname"), 
        columnName("lastname"),
        columnName("age"), 
        columnName("category"));
  }
}
