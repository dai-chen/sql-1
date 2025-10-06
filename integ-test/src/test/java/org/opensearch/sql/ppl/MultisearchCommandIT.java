/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;

public class MultisearchCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testMultisearchBasic() throws IOException {
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
        
    // Should contain results from both age groups
    verifyDataRows(result, rows(new Object[]{1, "Amber", "Duke", 32, "M", "880 Holmes Lane", "Pyrami", "amberduke@pyrami.com", "Brogan", "IL"}));
  }

  @Test
  public void testMultisearchWithEval() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | multisearch [source=%s | where age > 35 | eval category='senior'] [source=%s | where age < 25 | eval category='junior'] | fields firstname, lastname, age, category",
        TEST_INDEX_BANK, TEST_INDEX_BANK, TEST_INDEX_BANK));
    
    verifySchema(result, 
        columnName("firstname"), 
        columnName("lastname"),
        columnName("age"), 
        columnName("category"));
  }

  @Test
  public void testMultisearchWithStats() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | multisearch [source=%s | where gender='M' | stats count(*) by state | eval type='male'] [source=%s | where gender='F' | stats count(*) by state | eval type='female'] | fields state, type",
        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));
    
    verifySchema(result, 
        columnName("state"), 
        columnName("type"));
  }

  @Test
  public void testMultisearchWithFieldSelection() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | fields firstname, lastname, age | multisearch [source=%s | where age > 35] [source=%s | where age < 25]",
        TEST_INDEX_BANK, TEST_INDEX_BANK, TEST_INDEX_BANK));
    
    verifySchema(result, 
        columnName("firstname"), 
        columnName("lastname"),
        columnName("age"));
  }

  @Test
  public void testMultisearchThreeSubsearches() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | multisearch [source=%s | where age > 35] [source=%s | where age < 25] [source=%s | where age between 25 and 35] | head 20",
        TEST_INDEX_BANK, TEST_INDEX_BANK, TEST_INDEX_BANK, TEST_INDEX_BANK));
    
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
  public void testMultisearchErrorTooFewSubsearches() throws IOException {
    ResponseException exception = assertThrows(ResponseException.class, 
        () -> executeQuery(String.format("source=%s | multisearch [source=%s | where age > 35]", TEST_INDEX_BANK, TEST_INDEX_BANK)));
    
    assertTrue(exception.getMessage().contains("requires at least 2 subsearches"));
  }

  @Test 
  public void testMultisearchWithComplexPipeline() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | multisearch [source=%s | where gender='M' | stats avg(age) by state] [source=%s | where gender='F' | stats avg(age) by state] | sort state",
        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));
    
    verifySchema(result, 
        columnName("avg(age)"), 
        columnName("state"));
  }
}
