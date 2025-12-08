/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.evaluator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Mixin interface providing fluent assertion API for query evaluation results. Tests can implement
 * this interface to gain access to verification methods and matchers.
 */
public interface QueryEvaluatorAssertion {

  /** Creates ResponseAssertion from a query response */
  default ResponseAssertion verify(ExecutionEngine.QueryResponse response) {
    return new ResponseAssertion(response);
  }

  /** Creates row matcher for data verification */
  default Matcher<ExprValue> row(Object... expectedValues) {
    return new TypeSafeMatcher<ExprValue>() {
      @Override
      protected boolean matchesSafely(ExprValue actual) {
        Map<String, ExprValue> actualMap = actual.tupleValue();
        List<String> columnNames = new ArrayList<>(actualMap.keySet());

        if (expectedValues.length != columnNames.size()) {
          return false;
        }

        for (int i = 0; i < expectedValues.length; i++) {
          ExprValue expectedValue = ExprValueUtils.fromObjectValue(expectedValues[i]);
          ExprValue actualValue = actualMap.get(columnNames.get(i));
          if (!expectedValue.equals(actualValue)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("row with values: " + Arrays.toString(expectedValues));
      }
    };
  }

  /** Creates column matcher for schema verification */
  default Matcher<ExecutionEngine.Schema.Column> col(String name, ExprType type) {
    return new TypeSafeMatcher<ExecutionEngine.Schema.Column>() {
      @Override
      protected boolean matchesSafely(ExecutionEngine.Schema.Column column) {
        return name.equals(column.getName()) && type.equals(column.getExprType());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("column with name: " + name + " and type: " + type);
      }
    };
  }

  /** Fluent assertion helper for query responses */
  class ResponseAssertion {
    final ExecutionEngine.QueryResponse response;

    ResponseAssertion(ExecutionEngine.QueryResponse response) {
      this.response = response;
    }

    @SafeVarargs
    public final ResponseAssertion expectSchema(
        Matcher<ExecutionEngine.Schema.Column>... matchers) {
      List<ExecutionEngine.Schema.Column> columns = response.getSchema().getColumns();
      assertEquals("Column count mismatch", matchers.length, columns.size());

      for (int i = 0; i < matchers.length; i++) {
        assertThat("Column " + i + " mismatch", columns.get(i), matchers[i]);
      }
      return this;
    }

    @SafeVarargs
    public final ResponseAssertion expectData(Matcher<ExprValue>... matchers) {
      List<ExprValue> results = response.getResults();
      assertEquals("Result count mismatch", matchers.length, results.size());

      for (int i = 0; i < matchers.length; i++) {
        assertThat("Row " + i + " mismatch", results.get(i), matchers[i]);
      }
      return this;
    }
  }
}
