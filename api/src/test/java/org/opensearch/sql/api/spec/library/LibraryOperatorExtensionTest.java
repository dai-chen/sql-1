/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.library;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.Before;
import org.junit.Test;

public class LibraryOperatorExtensionTest {

  private SqlOperatorTable operatorTable;

  @Before
  public void setUp() {
    operatorTable = new LibraryOperatorExtension().operators();
  }

  @Test
  public void datetimeFunctionRegistered() {
    assertTrue("DATETIME should be registered", hasOperator("DATETIME"));
  }

  @Test
  public void concatWsFunctionRegistered() {
    assertTrue("CONCAT_WS should be registered", hasOperator("CONCAT_WS"));
  }

  @Test
  public void rtrimFunctionRegistered() {
    assertTrue("RTRIM should be registered", hasOperator("RTRIM"));
  }

  @Test
  public void unknownFunctionNotRegistered() {
    assertFalse(
        "Unknown function should not be registered",
        hasOperator("definitely_not_a_calcite_function"));
  }

  private boolean hasOperator(String name) {
    List<SqlOperator> result = new ArrayList<>();
    operatorTable.lookupOperatorOverloads(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        SqlSyntax.FUNCTION,
        result,
        SqlNameMatchers.withCaseSensitive(false));
    return !result.isEmpty();
  }
}
