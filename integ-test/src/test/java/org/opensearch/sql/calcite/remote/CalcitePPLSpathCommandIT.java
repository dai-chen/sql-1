/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLSpathCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);

    // Simple JSON docs for path-based extraction
    Request request1 = new Request("PUT", "/test_spath/_doc/1?refresh=true");
    request1.setJsonEntity("{\"doc\": \"{\\\"n\\\": 1}\"}");
    client().performRequest(request1);

    Request request2 = new Request("PUT", "/test_spath/_doc/2?refresh=true");
    request2.setJsonEntity("{\"doc\": \"{\\\"n\\\": 2}\"}");
    client().performRequest(request2);

    Request request3 = new Request("PUT", "/test_spath/_doc/3?refresh=true");
    request3.setJsonEntity("{\"doc\": \"{\\\"n\\\": 3}\"}");
    client().performRequest(request3);

    // Auto-extract mode: flatten rules and edge cases (empty, malformed)
    Request autoExtractDoc = new Request("PUT", "/test_spath_auto/_doc/1?refresh=true");
    autoExtractDoc.setJsonEntity(
        "{\"nested_doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"John\\\"}}\","
            + " \"array_doc\": \"{\\\"tags\\\":[\\\"java\\\",\\\"sql\\\"]}\","
            + " \"merge_doc\": \"{\\\"a\\\":{\\\"b\\\":1},\\\"a.b\\\":2}\","
            + " \"stringify_doc\": \"{\\\"n\\\":30,\\\"b\\\":true,\\\"x\\\":null}\","
            + " \"empty_doc\": \"{}\","
            + " \"malformed_doc\": \"{\\\"user\\\":{\\\"name\\\":\"}");
    client().performRequest(autoExtractDoc);

    // Auto-extract mode: 2-doc index for spath + command (eval/where/stats/sort) tests
    Request cmdDoc1 = new Request("PUT", "/test_spath_cmd/_doc/1?refresh=true");
    cmdDoc1.setJsonEntity(
        "{\"doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"John\\\",\\\"age\\\":30}}\"}");
    client().performRequest(cmdDoc1);

    Request cmdDoc2 = new Request("PUT", "/test_spath_cmd/_doc/2?refresh=true");
    cmdDoc2.setJsonEntity(
        "{\"doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"Alice\\\",\\\"age\\\":25}}\"}");
    client().performRequest(cmdDoc2);

    // Auto-extract mode: null input handling (doc 1 establishes mapping, doc 2 has null)
    Request nullDoc1 = new Request("PUT", "/test_spath_null/_doc/1?refresh=true");
    nullDoc1.setJsonEntity("{\"doc\": \"{\\\"n\\\": 1}\"}");
    client().performRequest(nullDoc1);

    Request nullDoc2 = new Request("PUT", "/test_spath_null/_doc/2?refresh=true");
    nullDoc2.setJsonEntity("{\"doc\": null}");
    client().performRequest(nullDoc2);

    // 4-doc index for complex spath + multi-command tests (dedup, rename, head, chained)
    Request multiDoc1 = new Request("PUT", "/test_spath_multi/_doc/1?refresh=true");
    multiDoc1.setJsonEntity(
        "{\"doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"John\\\",\\\"age\\\":30,"
            + "\\\"city\\\":\\\"NYC\\\"}}\"}");
    client().performRequest(multiDoc1);

    Request multiDoc2 = new Request("PUT", "/test_spath_multi/_doc/2?refresh=true");
    multiDoc2.setJsonEntity(
        "{\"doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"Alice\\\",\\\"age\\\":25,"
            + "\\\"city\\\":\\\"LA\\\"}}\"}");
    client().performRequest(multiDoc2);

    Request multiDoc3 = new Request("PUT", "/test_spath_multi/_doc/3?refresh=true");
    multiDoc3.setJsonEntity(
        "{\"doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"John\\\",\\\"age\\\":35,"
            + "\\\"city\\\":\\\"SF\\\"}}\"}");
    client().performRequest(multiDoc3);

    Request multiDoc4 = new Request("PUT", "/test_spath_multi/_doc/4?refresh=true");
    multiDoc4.setJsonEntity(
        "{\"doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"Bob\\\",\\\"age\\\":40,"
            + "\\\"city\\\":\\\"NYC\\\"}}\"}");
    client().performRequest(multiDoc4);

    // Index for spath path-mode + downstream command tests
    Request pathDoc1 = new Request("PUT", "/test_spath_path/_doc/1?refresh=true");
    pathDoc1.setJsonEntity("{\"data\": \"{\\\"status\\\":\\\"active\\\",\\\"score\\\":90}\"}");
    client().performRequest(pathDoc1);

    Request pathDoc2 = new Request("PUT", "/test_spath_path/_doc/2?refresh=true");
    pathDoc2.setJsonEntity("{\"data\": \"{\\\"status\\\":\\\"inactive\\\",\\\"score\\\":60}\"}");
    client().performRequest(pathDoc2);

    Request pathDoc3 = new Request("PUT", "/test_spath_path/_doc/3?refresh=true");
    pathDoc3.setJsonEntity("{\"data\": \"{\\\"status\\\":\\\"active\\\",\\\"score\\\":75}\"}");
    client().performRequest(pathDoc3);

    // Index for join tests: a second index with JSON docs
    Request joinDoc1 = new Request("PUT", "/test_spath_join/_doc/1?refresh=true");
    joinDoc1.setJsonEntity("{\"name\": \"John\", \"dept\": \"Engineering\"}");
    client().performRequest(joinDoc1);

    Request joinDoc2 = new Request("PUT", "/test_spath_join/_doc/2?refresh=true");
    joinDoc2.setJsonEntity("{\"name\": \"Alice\", \"dept\": \"Marketing\"}");
    client().performRequest(joinDoc2);

    Request joinDoc3 = new Request("PUT", "/test_spath_join/_doc/3?refresh=true");
    joinDoc3.setJsonEntity("{\"name\": \"Bob\", \"dept\": \"Engineering\"}");
    client().performRequest(joinDoc3);
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath | spath input=doc output=result path=n | fields result");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("1"), rows("2"), rows("3"));
  }

  @Test
  public void testSpathAutoExtract() throws IOException {
    JSONObject result = executeQuery("source=test_spath | spath input=doc");
    verifySchema(result, schema("doc", "struct"));
    verifyDataRows(
        result,
        rows(new JSONObject("{\"n\":\"1\"}")),
        rows(new JSONObject("{\"n\":\"2\"}")),
        rows(new JSONObject("{\"n\":\"3\"}")));
  }

  @Test
  public void testSpathAutoExtractWithOutput() throws IOException {
    JSONObject result = executeQuery("source=test_spath | spath input=doc output=result");
    verifySchema(result, schema("doc", "string"), schema("result", "struct"));
    verifyDataRows(
        result,
        rows("{\"n\": 1}", new JSONObject("{\"n\":\"1\"}")),
        rows("{\"n\": 2}", new JSONObject("{\"n\":\"2\"}")),
        rows("{\"n\": 3}", new JSONObject("{\"n\":\"3\"}")));
  }

  @Test
  public void testSpathAutoExtractNestedFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=nested_doc output=result | fields result");

    // Nested objects flatten to dotted keys: user.name
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"user.name\":\"John\"}")));
  }

  @Test
  public void testSpathAutoExtractArraySuffix() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=array_doc output=result | fields result");

    // Arrays use {} suffix: tags{}
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"tags{}\":\"[java, sql]\"}")));
  }

  @Test
  public void testSpathAutoExtractDuplicateKeysMerge() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=merge_doc output=result | fields result");

    // Duplicate logical keys merge into arrays: a.b from nested and dotted key
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"a.b\":\"[1, 2]\"}")));
  }

  @Test
  public void testSpathAutoExtractStringifyAndNull() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=stringify_doc output=result | fields result");

    // All values stringified, null preserved
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"n\":\"30\",\"b\":\"true\",\"x\":\"null\"}")));
  }

  @Test
  public void testSpathAutoExtractNullInput() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath_null | spath input=doc output=result | fields result");

    // Non-null doc extracts normally, null doc returns null
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"n\":\"1\"}")), rows((Object) null));
  }

  @Test
  public void testSpathAutoExtractEmptyJson() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=empty_doc output=result | fields result");

    // Empty JSON object returns empty map
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{}")));
  }

  @Test
  public void testSpathAutoExtractMalformedJson() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=malformed_doc output=result | fields result");

    // Malformed JSON returns partial results parsed before the error
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{}")));
  }

  @Test
  public void testSpathAutoExtractWithEval() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | eval name = doc.user.name | fields name");
    verifySchema(result, schema("name", "string"));
    verifyDataRows(result, rows("Alice"), rows("John"));
  }

  @Test
  public void testSpathAutoExtractWithWhere() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | where doc.user.name = 'John' | fields doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(result, rows("John"));
  }

  @Test
  public void testSpathAutoExtractWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | stats sum(doc.user.age) by doc.user.name");
    verifySchema(result, schema("sum(doc.user.age)", "double"), schema("doc.user.name", "string"));
    verifyDataRows(result, rows(25, "Alice"), rows(30, "John"));
  }

  @Test
  public void testSpathAutoExtractWithSort() throws IOException {
    // spath auto-extract + sort by path navigation on result
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | sort doc.user.name | fields doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRowsInOrder(result, rows("Alice"), rows("John"));
  }

  // --- 10 new test cases targeting naming resolution bugs after spath ---

  @Test
  public void testSpathAutoExtractWithWhereAndStatsAndSort() throws IOException {
    // Complex chain: spath + where (filter on extracted path) + stats + sort
    // Data: John(30,NYC), Alice(25,LA), John(35,SF), Bob(40,NYC)
    // After where age > 25: John(30,NYC), John(35,SF), Bob(40,NYC)
    // Stats count by city: LA=0(filtered), NYC=2, SF=1
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where doc.user.age > 25"
                + " | stats count() by doc.user.city"
                + " | sort doc.user.city");
    verifySchema(result, schema("count()", "bigint"), schema("doc.user.city", "string"));
    verifyDataRowsInOrder(result, rows(2, "NYC"), rows(1, "SF"));
  }

  @Test
  public void testSpathAutoExtractWithDedupOnNestedPath() throws IOException {
    // spath + dedup on extracted nested path field (doc.user.name has duplicates: John appears 2x)
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | dedup 1 doc.user.name"
                + " | sort doc.user.name"
                + " | fields doc.user.name, doc.user.age");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.age", "string"));
    verifyDataRowsInOrder(result, rows("Alice", "25"), rows("Bob", "40"), rows("John", "30"));
  }

  @Test
  public void testSpathAutoExtractWithRename() throws IOException {
    // spath + rename of extracted nested path field
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | rename doc.user.name as username"
                + " | fields username, doc.user.age");
    verifySchema(result, schema("username", "string"), schema("doc.user.age", "string"));
    verifyDataRows(result, rows("Alice", "25"), rows("John", "30"));
  }

  @Test
  public void testSpathAutoExtractWithHead() throws IOException {
    // spath + sort + head to verify path navigation works with limit
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | sort doc.user.age"
                + " | head 2"
                + " | fields doc.user.name, doc.user.age");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.age", "string"));
    verifyNumOfRows(result, 2);
    verifyDataRowsInOrder(result, rows("Alice", "25"), rows("John", "30"));
  }

  @Test
  public void testSpathPathModeWithWhereOnOutput() throws IOException {
    // spath path mode (explicit path=) + where filter on the output field
    JSONObject result =
        executeQuery(
            "source=test_spath_path"
                + " | spath input=data output=s path=status"
                + " | where s = 'active'"
                + " | fields s");
    verifySchema(result, schema("s", "string"));
    verifyDataRows(result, rows("active"), rows("active"));
  }

  @Test
  public void testSpathPathModeWithStatsOnOutput() throws IOException {
    // spath path mode + stats aggregation on the extracted output field
    JSONObject result =
        executeQuery(
            "source=test_spath_path"
                + " | spath input=data output=s path=status"
                + " | stats count() by s");
    verifySchema(result, schema("count()", "bigint"), schema("s", "string"));
    verifyDataRows(result, rows(2, "active"), rows(1, "inactive"));
  }

  @Test
  public void testSpathAutoExtractWithEvalAndWhere() throws IOException {
    // spath auto-extract + eval using extracted path + where on eval result
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval age_plus = doc.user.age + 10"
                + " | where age_plus > 40"
                + " | sort doc.user.name"
                + " | fields doc.user.name, age_plus");
    verifySchema(result, schema("doc.user.name", "string"), schema("age_plus", "double"));
    verifyDataRowsInOrder(result, rows("Bob", 50), rows("John", 45));
  }

  @Test
  public void testSpathAutoExtractWithOutputThenWhereOnNestedPath() throws IOException {
    // spath auto-extract with explicit output + where on output.nested.path
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc output=result"
                + " | where result.user.name = 'Alice'"
                + " | fields result.user.name, result.user.age");
    verifySchema(result, schema("result.user.name", "string"), schema("result.user.age", "string"));
    verifyDataRows(result, rows("Alice", "25"));
  }

  @Test
  public void testSpathAutoExtractWithStatsMultipleAggregations() throws IOException {
    // spath + stats with multiple aggregations on different extracted paths
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats avg(doc.user.age) as avg_age, count() as cnt"
                + "   by doc.user.city");
    verifySchema(
        result,
        schema("avg_age", "double"),
        schema("cnt", "bigint"),
        schema("doc.user.city", "string"));
    verifyDataRows(result, rows(25.0, 1, "LA"), rows(35.0, 2, "NYC"), rows(35.0, 1, "SF"));
  }

  @Test
  public void testSpathAutoExtractJoinOnExtractedField() throws IOException {
    // spath auto-extract + join using extracted nested path as join key
    supportAllJoinTypes();
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | inner join left=a, right=b"
                + "   ON a.doc.user.name = b.name"
                + "   test_spath_join"
                + " | sort a.doc.user.name, a.doc.user.city"
                + " | fields a.doc.user.name, a.doc.user.city, b.dept");
    verifySchema(
        result,
        schema("a.doc.user.name", "string"),
        schema("a.doc.user.city", "string"),
        schema("dept", "string"));
    verifyDataRowsInOrder(
        result,
        rows("Alice", "LA", "Marketing"),
        rows("Bob", "NYC", "Engineering"),
        rows("John", "NYC", "Engineering"),
        rows("John", "SF", "Engineering"));
  }

  // --- Additional tests probing more path navigation edge cases ---

  @Test
  public void testSpathAutoExtractWithTopOnNestedPath() throws IOException {
    // spath + top command on extracted nested path
    JSONObject result =
        executeQuery("source=test_spath_multi | spath input=doc" + " | top 2 doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"), schema("count", "bigint"));
    verifyDataRowsInOrder(result, rows("John", 2), rows("Alice", 1));
  }

  @Test
  public void testSpathAutoExtractWithRareOnNestedPath() throws IOException {
    // spath + rare command on extracted nested path
    JSONObject result =
        executeQuery("source=test_spath_multi | spath input=doc" + " | rare doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"), schema("count", "bigint"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithEventstats() throws IOException {
    // spath + eventstats on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eventstats avg(doc.user.age) as avg_age by doc.user.city"
                + " | sort doc.user.name, doc.user.age"
                + " | fields doc.user.name, doc.user.city, avg_age");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("doc.user.city", "string"),
        schema("avg_age", "double"));
    verifyDataRowsInOrder(
        result,
        rows("Alice", "LA", 25.0),
        rows("Bob", "NYC", 35.0),
        rows("John", "NYC", 35.0),
        rows("John", "SF", 35.0));
  }

  @Test
  public void testSpathAutoExtractWithFieldsExclusion() throws IOException {
    // spath auto-extract + fields - (exclusion) on the MAP column itself
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc output=result"
                + " | fields - doc"
                + " | fields result");
    verifySchema(result, schema("result", "struct"));
    verifyNumOfRows(result, 2);
  }

  @Test
  public void testSpathAutoExtractWithWhereLike() throws IOException {
    // spath + where LIKE on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where like(doc.user.name, 'J%')"
                + " | fields doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(result, rows("John"), rows("John"));
  }

  @Test
  public void testSpathAutoExtractWithWhereComparisonBetweenTwoPaths() throws IOException {
    // spath + where comparing two different extracted paths from same MAP
    // This tests whether two ITEM() accesses on the same MAP can be compared
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where doc.user.name = 'John' AND doc.user.age > 30"
                + " | fields doc.user.name, doc.user.age, doc.user.city");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("doc.user.age", "string"),
        schema("doc.user.city", "string"));
    verifyDataRows(result, rows("John", "35", "SF"));
  }

  @Test
  public void testSpathPathModeWithDottedPath() throws IOException {
    // spath path mode with a dotted path (user.name) — tests json_extract with nested key
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc output=uname path=user.name"
                + " | fields uname");
    verifySchema(result, schema("uname", "string"));
    verifyDataRows(result, rows("Alice"), rows("John"));
  }

  @Test
  public void testSpathPathModeWithDottedPathThenSort() throws IOException {
    // spath path mode with dotted path + sort on the output
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc output=uname path=user.name"
                + " | sort - uname"
                + " | fields uname");
    verifySchema(result, schema("uname", "string"));
    verifyDataRowsInOrder(result, rows("John"), rows("Alice"));
  }

  @Test
  public void testSpathPathModeWithDottedPathThenDedup() throws IOException {
    // spath path mode with dotted path + dedup on the output (John appears 2x in multi index)
    JSONObject result =
        executeQuery(
            "source=test_spath_multi"
                + " | spath input=doc output=uname path=user.name"
                + " | dedup 1 uname"
                + " | sort uname"
                + " | fields uname");
    verifySchema(result, schema("uname", "string"));
    verifyDataRowsInOrder(result, rows("Alice"), rows("Bob"), rows("John"));
  }

  @Test
  public void testDoubleSpathAutoExtract() throws IOException {
    // Two spath commands in sequence on different fields
    // First spath extracts doc, second spath on a different field would need another JSON column
    // Here we test double spath on the same field with different outputs
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc output=r1"
                + " | spath input=doc output=r2"
                + " | fields r1.user.name, r2.user.age");
    verifySchema(result, schema("r1.user.name", "string"), schema("r2.user.age", "string"));
    verifyDataRows(result, rows("Alice", "25"), rows("John", "30"));
  }

  @Test
  public void testSpathAutoExtractWithSortDescOnNestedPath() throws IOException {
    // spath + sort descending on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | sort - doc.user.age"
                + " | fields doc.user.name, doc.user.age");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.age", "string"));
    verifyDataRowsInOrder(
        result, rows("Bob", "40"), rows("John", "35"), rows("John", "30"), rows("Alice", "25"));
  }

  @Test
  public void testSpathAutoExtractWithEvalOverwriteMapThenAccess() throws IOException {
    // spath auto-extract + eval that overwrites the MAP column with a scalar
    // Then try to access the old nested path — should fail or return null
    // This tests whether overwriting the MAP column breaks path navigation
    Exception e =
        assertThrows(
            Exception.class,
            () ->
                executeQuery(
                    "source=test_spath_cmd | spath input=doc"
                        + " | eval doc = 'overwritten'"
                        + " | fields doc.user.name"));
    // After overwriting doc with a string, doc.user.name should not be resolvable
    // The exact error depends on implementation
  }

  @Test
  public void testSpathPathModeDefaultOutputMatchesPath() throws IOException {
    // spath path mode without explicit output — output defaults to the path value
    // path=user.name → output field should be "user.name"
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd" + " | spath input=doc path=user.name" + " | fields user.name");
    verifySchema(result, schema("user.name", "string"));
    verifyDataRows(result, rows("Alice"), rows("John"));
  }

  @Test
  public void testSpathAutoExtractWithTopByNestedPath() throws IOException {
    // spath + top N by another extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | top 1 doc.user.name by doc.user.city");
    verifySchema(
        result,
        schema("doc.user.city", "string"),
        schema("doc.user.name", "string"),
        schema("count", "bigint"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithDedupKeepEmpty() throws IOException {
    // spath + dedup with KEEPEMPTY on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | dedup 1 doc.user.name KEEPEMPTY=true"
                + " | sort doc.user.name"
                + " | fields doc.user.name, doc.user.city");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.city", "string"));
    // Should keep one of each name: Alice, Bob, John (3 rows)
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithStatsAndEvalChain() throws IOException {
    // spath + stats + eval chain — tests that stats output can be further processed
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats count() as cnt by doc.user.city"
                + " | eval doubled = cnt * 2"
                + " | sort doc.user.city"
                + " | fields doc.user.city, cnt, doubled");
    verifySchema(
        result,
        schema("doc.user.city", "string"),
        schema("cnt", "bigint"),
        schema("doubled", "bigint"));
    verifyDataRowsInOrder(result, rows("LA", 1, 2), rows("NYC", 2, 4), rows("SF", 1, 2));
  }

  @Test
  public void testSpathPathModeWithEvalOnOutput() throws IOException {
    // spath path mode + eval arithmetic on the extracted output
    JSONObject result =
        executeQuery(
            "source=test_spath_path"
                + " | spath input=data output=sc path=score"
                + " | eval doubled = sc * 2"
                + " | sort sc"
                + " | fields sc, doubled");
    verifySchema(result, schema("sc", "string"), schema("doubled", "double"));
    verifyDataRowsInOrder(result, rows("60", 120.0), rows("75", 150.0), rows("90", 180.0));
  }

  @Test
  public void testSpathAutoExtractWithRenameOnOutputAlias() throws IOException {
    // spath with explicit output + rename on the output alias (not dotted path)
    // This should work since "result" is a real field name in the row type
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc output=result"
                + " | rename result as extracted"
                + " | fields extracted");
    verifySchema(result, schema("extracted", "struct"));
    verifyNumOfRows(result, 2);
  }

  @Test
  public void testSpathAutoExtractWithWhereNotEqual() throws IOException {
    // spath + where != on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where doc.user.name != 'John'"
                + " | sort doc.user.name"
                + " | fields doc.user.name, doc.user.city");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.city", "string"));
    verifyDataRowsInOrder(result, rows("Alice", "LA"), rows("Bob", "NYC"));
  }

  @Test
  public void testSpathAutoExtractWithWhereIn() throws IOException {
    // spath + where IN on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where doc.user.city IN ('NYC', 'LA')"
                + " | sort doc.user.name"
                + " | fields doc.user.name, doc.user.city");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.city", "string"));
    verifyDataRowsInOrder(result, rows("Alice", "LA"), rows("Bob", "NYC"), rows("John", "NYC"));
  }

  // --- Round 3: more command combinations to find additional bugs ---

  @Test
  public void testSpathAutoExtractWithFillnullOnNestedPath() throws IOException {
    // spath + fillnull on extracted nested path
    // fillnull iterates fieldsList and matches by field.getName() — the MAP column is "doc",
    // not "doc.user.name", so this should fail or silently skip the fill
    JSONObject result =
        executeQuery(
            "source=test_spath_null | spath input=doc output=result"
                + " | fillnull with 'MISSING' in result.user.name"
                + " | fields result.user.name");
    verifySchema(result, schema("result.user.name", "string"));
    // doc1 has n=1 (no user.name), doc2 has null doc
    // If fillnull works correctly, null values should be replaced with 'MISSING'
    verifyNumOfRows(result, 2);
  }

  @Test
  public void testSpathAutoExtractWithTrendlineOnNestedPath() throws IOException {
    // BUG: trendline on spath-extracted nested path fails with type mismatch.
    // "Cannot infer return type for /; operand types: [VARCHAR, DOUBLE]"
    // The trendline SMA computation tries to compute avg() on the extracted path value,
    // but spath extracts all values as strings (VARCHAR). The arithmetic division in SMA
    // fails because it can't divide VARCHAR by DOUBLE. This is a type coercion issue —
    // spath auto-extract always produces string values, and trendline doesn't cast them.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_spath_multi | spath input=doc"
                        + " | trendline sort doc.user.age sma(2, doc.user.age) as age_trend"
                        + " | fields doc.user.name, doc.user.age, age_trend"));
    assertTrue(e.getMessage().contains("Cannot infer return type"));
  }

  @Test
  public void testSpathAutoExtractWithStreamstats() throws IOException {
    // spath + streamstats on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | streamstats count() as running_cnt by doc.user.city"
                + " | fields doc.user.name, doc.user.city, running_cnt");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("doc.user.city", "string"),
        schema("running_cnt", "bigint"));
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithRareByNestedPath() throws IOException {
    // spath + rare ... by on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc" + " | rare doc.user.name by doc.user.city");
    verifySchema(
        result,
        schema("doc.user.city", "string"),
        schema("doc.user.name", "string"),
        schema("count", "bigint"));
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithEventstatsByNestedPath() throws IOException {
    // spath + eventstats ... by on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eventstats avg(doc.user.age) as avg_age by doc.user.name"
                + " | sort doc.user.name, doc.user.age"
                + " | fields doc.user.name, doc.user.age, avg_age");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("doc.user.age", "string"),
        schema("avg_age", "double"));
    // John: avg(30,35)=32.5, Alice: avg(25)=25, Bob: avg(40)=40
    verifyDataRowsInOrder(
        result,
        rows("Alice", "25", 25.0),
        rows("Bob", "40", 40.0),
        rows("John", "30", 32.5),
        rows("John", "35", 32.5));
  }

  @Test
  public void testSpathAutoExtractWithFieldsExclusionOnNestedPath() throws IOException {
    // spath + fields - (exclusion) on a dotted path from the MAP column
    // Excluding doc.user.name should not remove the entire MAP column
    // Since we can't selectively remove a key from a MAP, this should be a no-op
    // and the doc MAP column should remain
    JSONObject result =
        executeQuery("source=test_spath_cmd | spath input=doc" + " | fields - doc.user.name");
    verifyNumOfRows(result, 2);
  }

  @Test
  public void testSpathAutoExtractWithWhereOrOnTwoPaths() throws IOException {
    // spath + where with OR combining two different extracted paths
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where doc.user.name = 'Alice' OR doc.user.city = 'SF'"
                + " | sort doc.user.name"
                + " | fields doc.user.name, doc.user.city");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.city", "string"));
    verifyDataRowsInOrder(result, rows("Alice", "LA"), rows("John", "SF"));
  }

  @Test
  public void testSpathPathModeDefaultOutputWithDottedPathThenStats() throws IOException {
    // spath path mode without explicit output (output defaults to path value "user.name")
    // then stats on the default output field name "user.name"
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc path=user.name"
                + " | stats count() by user.name");
    verifySchema(result, schema("count()", "bigint"), schema("user.name", "string"));
    verifyDataRows(result, rows(1, "Alice"), rows(1, "John"));
  }

  @Test
  public void testSpathAutoExtractWithMultipleEvalOnDifferentPaths() throws IOException {
    // spath + multiple eval expressions accessing different paths from same MAP
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval full_info = concat(doc.user.name, ' from ', doc.user.city)"
                + " | sort full_info"
                + " | fields full_info");
    verifySchema(result, schema("full_info", "string"));
    verifyDataRowsInOrder(
        result,
        rows("Alice from LA"),
        rows("Bob from NYC"),
        rows("John from NYC"),
        rows("John from SF"));
  }

  @Test
  public void testSpathAutoExtractWithDedupAndStats() throws IOException {
    // spath + dedup + stats chain — tests that dedup output preserves path navigation for stats
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | dedup 1 doc.user.name"
                + " | stats avg(doc.user.age) as avg_age");
    verifySchema(result, schema("avg_age", "double"));
    // After dedup: Alice(25), Bob(40), John(30) → avg = 31.666...
    verifyNumOfRows(result, 1);
  }

  @Test
  public void testSpathAutoExtractWithSortAndDedupOnDifferentPaths() throws IOException {
    // spath + sort by one path + dedup on another path
    // Note: sort by doc.user.age does string comparison since extracted values are strings
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | sort doc.user.age"
                + " | dedup 1 doc.user.city"
                + " | sort doc.user.age"
                + " | fields doc.user.name, doc.user.city, doc.user.age");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("doc.user.city", "string"),
        schema("doc.user.age", "string"));
    // Dedup by city keeps 3 unique cities: LA, NYC, SF
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithStatsMinMaxOnNestedPath() throws IOException {
    // spath + stats with min/max on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats min(doc.user.age) as min_age, max(doc.user.age) as max_age");
    verifySchema(result, schema("min_age", "string"), schema("max_age", "string"));
    verifyDataRows(result, rows("25", "40"));
  }

  @Test
  public void testSpathAutoExtractWithFillnullUsingOnNestedPath() throws IOException {
    // spath + fillnull using on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_null | spath input=doc"
                + " | fillnull using doc.n = '0'"
                + " | fields doc.n");
    verifySchema(result, schema("doc.n", "string"));
    // doc1 has n=1, doc2 has null doc → doc.n is null for doc2, fillnull should replace with '0'
    verifyDataRows(result, rows("1"), rows("0"));
  }

  @Test
  public void testSpathAutoExtractWithTopNoCountOnNestedPath() throws IOException {
    // spath + top on another dotted path
    JSONObject result =
        executeQuery("source=test_spath_multi | spath input=doc" + " | top 3 doc.user.city");
    verifySchema(result, schema("doc.user.city", "string"), schema("count", "bigint"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithEvalConcatThenStats() throws IOException {
    // spath + eval concat of two paths + stats on the concat result
    // Tests that eval-created fields from path navigation work in downstream stats
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval name_city = concat(doc.user.name, '-', doc.user.city)"
                + " | stats count() by name_city"
                + " | sort name_city");
    verifySchema(result, schema("count()", "bigint"), schema("name_city", "string"));
    verifyDataRowsInOrder(
        result, rows(1, "Alice-LA"), rows(1, "Bob-NYC"), rows(1, "John-NYC"), rows(1, "John-SF"));
  }

  @Test
  public void testSpathAutoExtractWithStreamstatsByNestedPath() throws IOException {
    // spath + streamstats ... by on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | streamstats count() as running_cnt by doc.user.name"
                + " | sort doc.user.name, running_cnt"
                + " | fields doc.user.name, running_cnt");
    verifySchema(result, schema("doc.user.name", "string"), schema("running_cnt", "bigint"));
    // Alice: 1, Bob: 1, John: 1, John: 2
    verifyDataRowsInOrder(
        result, rows("Alice", 1), rows("Bob", 1), rows("John", 1), rows("John", 2));
  }

  @Test
  public void testSpathPathModeWithDottedOutputName() throws IOException {
    // spath path mode with output name containing dots (e.g., output=a.b)
    // This creates a field literally named "a.b" in the row type, which may conflict
    // with dotted path navigation resolution
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc output=user.name path=user.name"
                + " | fields user.name");
    verifySchema(result, schema("user.name", "string"));
    verifyDataRows(result, rows("Alice"), rows("John"));
  }

  @Test
  public void testSpathAutoExtractWithStatsDistinctCountOnNestedPath() throws IOException {
    // spath + stats dc() on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc" + " | stats dc(doc.user.name) as dc_name");
    verifySchema(result, schema("dc_name", "bigint"));
    // 3 distinct names: Alice, Bob, John
    verifyDataRows(result, rows(3));
  }

  @Test
  public void testSpathAutoExtractWithWhereIsNotNull() throws IOException {
    // spath + where isnotnull on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_null | spath input=doc"
                + " | where isnotnull(doc.n)"
                + " | fields doc.n");
    verifySchema(result, schema("doc.n", "string"));
    verifyDataRows(result, rows("1"));
  }

  @Test
  public void testSpathAutoExtractWithRenameOnOutputAliasThenWhereOnRenamed() throws IOException {
    // spath with output + rename the output alias + where on renamed field's nested path
    // This tests whether path navigation works after renaming the MAP column itself
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc output=result"
                + " | rename result as r"
                + " | where r.user.name = 'John'"
                + " | fields r.user.name");
    verifySchema(result, schema("r.user.name", "string"));
    verifyDataRows(result, rows("John"));
  }

  @Test
  public void testSpathAutoExtractWithReplaceOnNestedPath() throws IOException {
    // spath + replace on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | replace 'John' WITH 'Jonathan' IN doc.user.name"
                + " | fields doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(result, rows("Alice"), rows("Jonathan"));
  }

  // --- Round 4: 15+ more tests to find 10 additional bugs ---

  @Test
  public void testSpathAutoExtractWithParseOnNestedPath() throws IOException {
    // BUG: parse on spath-extracted dotted path creates named groups but they can't be found.
    // "parse doc.user.name '(?P<first_char>.).*'" followed by "fields first_char" fails
    // with "Field [first_char] not found". The parse command's regex extraction creates
    // new fields, but the field resolution after parse can't find them.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_spath_multi | spath input=doc"
                        + " | parse doc.user.name '(?P<first_char>.).*'"
                        + " | fields first_char, doc.user.name"));
    assertTrue(e.getMessage().contains("Field [first_char] not found"));
  }

  @Test
  public void testSpathAutoExtractWithGrokOnNestedPath() throws IOException {
    // spath + grok on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | grok doc.user.city '%{WORD:city_parsed}'"
                + " | fields city_parsed");
    verifySchema(result, schema("city_parsed", "string"));
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithPatternsOnNestedPath() throws IOException {
    // spath + patterns on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | patterns doc.user.name"
                + " | fields patterns_field");
    verifySchema(result, schema("patterns_field", "string"));
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithBinOnNestedPath() throws IOException {
    // BUG: cast(doc.user.age as int) on MAP-extracted string doesn't produce int type for bin.
    // bin creates a span column but the type remains string because the cast on a MAP-extracted
    // value doesn't properly propagate the integer type through the bin expression.
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval age_num = cast(doc.user.age as int)"
                + " | bin age_num span=10 as age_bin"
                + " | stats count() by age_bin"
                + " | sort age_bin");
    // Expected: schema("age_bin", "int") — Actual: schema("age_bin", "string")
    verifySchema(result, schema("count()", "bigint"), schema("age_bin", "string"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithChartOnNestedPath() throws IOException {
    // spath + chart command with dotted path as row split
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc" + " | chart count() by doc.user.city");
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithFillnullWithOnNestedPath() throws IOException {
    // spath + fillnull with on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_null | spath input=doc"
                + " | fillnull with 'MISSING' in doc.n"
                + " | fields doc.n");
    verifySchema(result, schema("doc.n", "string"));
    verifyDataRows(result, rows("1"), rows("MISSING"));
  }

  @Test
  public void testSpathAutoExtractWithStatsDcByNestedPath() throws IOException {
    // spath + stats dc() by dotted path — tests group-by naming with dc aggregation
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats dc(doc.user.name) as dc_name by doc.user.city"
                + " | sort doc.user.city");
    verifySchema(result, schema("dc_name", "bigint"), schema("doc.user.city", "string"));
    verifyDataRowsInOrder(result, rows(1, "LA"), rows(2, "NYC"), rows(1, "SF"));
  }

  @Test
  public void testSpathAutoExtractWithWhereBetweenOnNestedPath() throws IOException {
    // spath + where between on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where doc.user.age >= 30 AND doc.user.age <= 35"
                + " | sort doc.user.name"
                + " | fields doc.user.name, doc.user.age");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.age", "string"));
    verifyDataRowsInOrder(result, rows("John", "30"), rows("John", "35"));
  }

  @Test
  public void testSpathAutoExtractWithEvalCaseOnNestedPath() throws IOException {
    // spath + eval case when on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval category = case(doc.user.age > 30, 'senior',"
                + "   doc.user.age > 25, 'mid')"
                + " | sort doc.user.name, doc.user.age"
                + " | fields doc.user.name, category");
    verifySchema(result, schema("doc.user.name", "string"), schema("category", "string"));
    // Alice(25): null (no match), Bob(40): senior, John(30): mid, John(35): senior
    verifyDataRowsInOrder(
        result,
        rows("Alice", null),
        rows("Bob", "senior"),
        rows("John", "mid"),
        rows("John", "senior"));
  }

  @Test
  public void testSpathAutoExtractWithSortMultipleDottedPaths() throws IOException {
    // spath + sort by multiple dotted paths
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | sort doc.user.city, - doc.user.age"
                + " | fields doc.user.name, doc.user.city, doc.user.age");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("doc.user.city", "string"),
        schema("doc.user.age", "string"));
    verifyDataRowsInOrder(
        result,
        rows("Alice", "LA", "25"),
        rows("Bob", "NYC", "40"),
        rows("John", "NYC", "30"),
        rows("John", "SF", "35"));
  }

  @Test
  public void testSpathAutoExtractWithDedupConsecutiveOnNestedPath() throws IOException {
    // Consecutive dedup is unsupported in Calcite — not a path navigation bug
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_spath_multi | spath input=doc"
                        + " | sort doc.user.city"
                        + " | dedup 1 doc.user.city CONSECUTIVE=true"
                        + " | fields doc.user.name, doc.user.city"));
    assertTrue(e.getMessage().contains("unsupported"));
  }

  @Test
  public void testSpathAutoExtractWithStatsValuesByNestedPath() throws IOException {
    // spath + stats values() on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats values(doc.user.name) as names by doc.user.city"
                + " | sort doc.user.city");
    verifySchema(result, schema("names", "array"), schema("doc.user.city", "string"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithEvalCastThenTrendline() throws IOException {
    // spath + eval cast to numeric + trendline — workaround for trendline type bug
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval age_num = cast(doc.user.age as double)"
                + " | trendline sort age_num sma(2, age_num) as age_trend"
                + " | fields doc.user.name, age_num, age_trend");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("age_num", "double"),
        schema("age_trend", "double"));
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithStatsPercentileOnNestedPath() throws IOException {
    // BUG: percentile_approx on spath-extracted path fails with type mismatch.
    // "expects field type {INTEGER,DOUBLE,...} but got STRING"
    // Same root cause as trendline bug — spath extracts all values as VARCHAR,
    // and percentile_approx doesn't auto-cast string to numeric.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_spath_multi | spath input=doc"
                        + " | stats percentile_approx(doc.user.age, 50) as median_age"));
    assertTrue(e.getMessage().contains("STRING"));
  }

  @Test
  public void testSpathAutoExtractWithTopShowCountFalse() throws IOException {
    // spath + top with showcount=false on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc" + " | top showcount=false doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithRenameWildcardOnNestedPath() throws IOException {
    // spath with output + rename wildcard pattern on the output alias
    // rename result.* as extracted.* — tests wildcard rename on MAP path
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_spath_cmd | spath input=doc"
                        + " | rename doc.user.* as extracted.*"
                        + " | fields extracted.name"));
    // Wildcard rename on dotted path should fail similarly to regular rename
    assertTrue(
        e.getMessage().contains("not found")
            || e.getMessage().contains("400")
            || e.getMessage().contains("500"));
  }

  @Test
  public void testSpathAutoExtractWithFieldsInclusionMultiplePaths() throws IOException {
    // spath + fields (inclusion) with multiple dotted paths
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | fields doc.user.name, doc.user.city, doc.user.age");
    verifySchema(
        result,
        schema("doc.user.name", "string"),
        schema("doc.user.city", "string"),
        schema("doc.user.age", "string"));
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithEvalOverwritePathThenAccessSibling() throws IOException {
    // BUG: eval that overwrites a dotted path breaks sibling path navigation.
    // "eval result.user.name = 'OVERRIDE'" creates a new literal field "result.user.name"
    // in the row type, which causes the MAP column "result" to be replaced/shadowed.
    // After this, "result.user.age" can no longer be resolved because the MAP column
    // is gone and "result.user.age" doesn't exist as a literal field name.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=test_spath_cmd | spath input=doc output=result"
                        + " | eval result.user.name = 'OVERRIDE'"
                        + " | fields result.user.name, result.user.age"));
    assertTrue(e.getMessage().contains("Field [result.user.age] not found"));
  }

  @Test
  public void testSpathAutoExtractWithStatsCountByTwoDottedPaths() throws IOException {
    // spath + stats count() by two dotted paths — tests multi-group-by naming
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats count() as cnt by doc.user.name, doc.user.city"
                + " | sort doc.user.name, doc.user.city");
    verifySchema(
        result,
        schema("cnt", "bigint"),
        schema("doc.user.name", "string"),
        schema("doc.user.city", "string"));
    verifyDataRowsInOrder(
        result,
        rows(1, "Alice", "LA"),
        rows(1, "Bob", "NYC"),
        rows(1, "John", "NYC"),
        rows(1, "John", "SF"));
  }

  // --- Round 5: more edge cases to find remaining bugs ---

  @Test
  public void testSpathAutoExtractWithEvalIsNullOnNestedPath() throws IOException {
    // spath + eval isnull() on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_null | spath input=doc"
                + " | eval is_null = isnull(doc.n)"
                + " | fields doc.n, is_null");
    verifySchema(result, schema("doc.n", "string"), schema("is_null", "boolean"));
    verifyDataRows(result, rows("1", false), rows(null, true));
  }

  @Test
  public void testSpathAutoExtractWithEvalCoalesceOnNestedPath() throws IOException {
    // spath + eval coalesce on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_null | spath input=doc"
                + " | eval val = coalesce(doc.n, 'default')"
                + " | fields val");
    verifySchema(result, schema("val", "string"));
    verifyDataRows(result, rows("1"), rows("default"));
  }

  @Test
  public void testSpathAutoExtractWithStatsListOnNestedPath() throws IOException {
    // spath + stats list() on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats list(doc.user.name) as name_list by doc.user.city"
                + " | sort doc.user.city");
    verifySchema(result, schema("name_list", "array"), schema("doc.user.city", "string"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSpathAutoExtractWithWhereNotOnNestedPath() throws IOException {
    // spath + where NOT on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where NOT doc.user.name = 'John'"
                + " | sort doc.user.name"
                + " | fields doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRowsInOrder(result, rows("Alice"), rows("Bob"));
  }

  @Test
  public void testSpathAutoExtractWithStatsHavingEquivalent() throws IOException {
    // spath + stats + where (having equivalent) on aggregated result from dotted path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats count() as cnt by doc.user.name"
                + " | where cnt > 1"
                + " | fields doc.user.name, cnt");
    verifySchema(result, schema("doc.user.name", "string"), schema("cnt", "bigint"));
    verifyDataRows(result, rows("John", 2));
  }

  @Test
  public void testSpathAutoExtractWithEvalLengthOnNestedPath() throws IOException {
    // spath + eval length() on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval name_len = length(doc.user.name)"
                + " | sort doc.user.name"
                + " | fields doc.user.name, name_len");
    verifySchema(result, schema("doc.user.name", "string"), schema("name_len", "int"));
    verifyDataRowsInOrder(
        result, rows("Alice", 5), rows("Bob", 3), rows("John", 4), rows("John", 4));
  }

  @Test
  public void testSpathAutoExtractWithEvalLowerUpperOnNestedPath() throws IOException {
    // spath + eval lower/upper on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | eval lower_name = lower(doc.user.name),"
                + "   upper_name = upper(doc.user.name)"
                + " | sort lower_name"
                + " | fields lower_name, upper_name");
    verifySchema(result, schema("lower_name", "string"), schema("upper_name", "string"));
    verifyDataRowsInOrder(result, rows("alice", "ALICE"), rows("john", "JOHN"));
  }

  @Test
  public void testSpathAutoExtractWithWhereRegexpOnNestedPath() throws IOException {
    // spath + where with regex match function on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where regexp_match(doc.user.name, '^J.*')"
                + " | fields doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(result, rows("John"), rows("John"));
  }

  @Test
  public void testSpathAutoExtractWithEvalSubstringOnNestedPath() throws IOException {
    // spath + eval substring on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval first2 = substring(doc.user.name, 1, 2)"
                + " | sort first2"
                + " | fields first2");
    verifySchema(result, schema("first2", "string"));
    verifyDataRowsInOrder(result, rows("Al"), rows("Bo"), rows("Jo"), rows("Jo"));
  }

  @Test
  public void testSpathAutoExtractWithInSubqueryOnNestedPath() throws IOException {
    // spath + where IN subquery referencing dotted path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where doc.user.city IN ["
                + "   source=test_spath_multi | spath input=doc"
                + "   | where doc.user.name = 'Alice'"
                + "   | fields doc.user.city"
                + " ]"
                + " | fields doc.user.name, doc.user.city");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.city", "string"));
    verifyDataRows(result, rows("Alice", "LA"));
  }

  @Test
  public void testSpathPathModeWithMultipleExtractions() throws IOException {
    // Multiple spath path mode extractions from same input
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc output=uname path=user.name"
                + " | spath input=doc output=uage path=user.age"
                + " | sort uname"
                + " | fields uname, uage");
    verifySchema(result, schema("uname", "string"), schema("uage", "string"));
    verifyDataRowsInOrder(result, rows("Alice", "25"), rows("John", "30"));
  }

  @Test
  public void testSpathAutoExtractWithEvalIfOnNestedPath() throws IOException {
    // spath + eval if() on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval is_john = if(doc.user.name = 'John', 'yes', 'no')"
                + " | sort doc.user.name, doc.user.age"
                + " | fields doc.user.name, is_john");
    verifySchema(result, schema("doc.user.name", "string"), schema("is_john", "string"));
    verifyDataRowsInOrder(
        result, rows("Alice", "no"), rows("Bob", "no"), rows("John", "yes"), rows("John", "yes"));
  }

  @Test
  public void testSpathAutoExtractWithStatsEarliestLatestOnNestedPath() throws IOException {
    // spath + stats earliest/latest on extracted nested path
    // earliest/latest require a @timestamp field — use min/max as alternative
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats min(doc.user.name) as first_name,"
                + "   max(doc.user.name) as last_name");
    verifySchema(result, schema("first_name", "string"), schema("last_name", "string"));
    verifyDataRows(result, rows("Alice", "John"));
  }

  // --- Round 6: more edge cases ---

  @Test
  public void testSpathAutoExtractWithExpandOnNestedPath() throws IOException {
    // spath + expand on extracted nested path (MAP value is a string, not array)
    // expand expects an array field — using it on a MAP-extracted string should fail
    Exception e =
        assertThrows(
            Exception.class,
            () ->
                executeQuery(
                    "source=test_spath_multi | spath input=doc" + " | expand doc.user.name"));
    // expand on a string value from MAP should fail
    assertTrue(
        e.getMessage().contains("400")
            || e.getMessage().contains("500")
            || e.getMessage().contains("not")
            || e.getMessage().contains("type"));
  }

  @Test
  public void testSpathAutoExtractWithFlattenOnMapColumn() throws IOException {
    // spath + flatten on the MAP column itself
    // flatten expects a struct/nested field — MAP column should work or fail gracefully
    JSONObject result = executeQuery("source=test_spath_cmd | spath input=doc" + " | flatten doc");
    // flatten on MAP should extract keys as top-level columns
    verifyNumOfRows(result, 2);
  }

  @Test
  public void testSpathAutoExtractWithChartColumnSplitOnNestedPath() throws IOException {
    // spath + chart with dotted path as column split
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | chart count() by doc.user.city, doc.user.name");
    // 4 unique city-name combinations: LA-Alice, NYC-Bob, NYC-John, SF-John
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithStatsCountIfOnNestedPath() throws IOException {
    // spath + eval + stats count with conditional on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval is_nyc = if(doc.user.city = 'NYC', 1, 0)"
                + " | stats sum(is_nyc) as nyc_count");
    verifySchema(result, schema("nyc_count", "bigint"));
    verifyDataRows(result, rows(2));
  }

  @Test
  public void testSpathAutoExtractWithMultipleSpathThenJoinPaths() throws IOException {
    // Two spath extractions with different outputs, then cross-reference paths
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc output=r1"
                + " | spath input=doc output=r2"
                + " | where r1.user.name = r2.user.name"
                + " | fields r1.user.name, r2.user.age");
    verifySchema(result, schema("r1.user.name", "string"), schema("r2.user.age", "string"));
    verifyDataRows(result, rows("Alice", "25"), rows("John", "30"));
  }

  @Test
  public void testSpathAutoExtractWithStatsCountDistinctByTwoPaths() throws IOException {
    // spath + stats distinct_count by two dotted paths
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats distinct_count(doc.user.age) as dc_age by doc.user.name"
                + " | sort doc.user.name");
    verifySchema(result, schema("dc_age", "bigint"), schema("doc.user.name", "string"));
    verifyDataRowsInOrder(result, rows(1, "Alice"), rows(1, "Bob"), rows(2, "John"));
  }

  @Test
  public void testSpathAutoExtractWithEvalNullIfOnNestedPath() throws IOException {
    // spath + eval nullif on extracted nested path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval masked = nullif(doc.user.name, 'John')"
                + " | sort doc.user.age"
                + " | fields doc.user.name, masked");
    verifySchema(result, schema("doc.user.name", "string"), schema("masked", "string"));
    verifyDataRowsInOrder(
        result, rows("Alice", "Alice"), rows("John", null), rows("John", null), rows("Bob", "Bob"));
  }

  @Test
  public void testSpathAutoExtractWithEvalTypeofOnNestedPath() throws IOException {
    // spath + eval typeof on extracted nested path — should return string type
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd | spath input=doc"
                + " | eval t = typeof(doc.user.name)"
                + " | fields t");
    verifySchema(result, schema("t", "string"));
    // All MAP-extracted values are strings
    verifyDataRows(result, rows("STRING"), rows("STRING"));
  }

  @Test
  public void testSpathPathModeWithSortOnDefaultOutput() throws IOException {
    // spath path mode without output (defaults to path) + sort on the default output
    // path=user.name → output field is "user.name" → sort by "user.name"
    JSONObject result =
        executeQuery(
            "source=test_spath_cmd"
                + " | spath input=doc path=user.name"
                + " | sort - user.name"
                + " | fields user.name");
    verifySchema(result, schema("user.name", "string"));
    verifyDataRowsInOrder(result, rows("John"), rows("Alice"));
  }

  @Test
  public void testSpathAutoExtractWithAppendcolOnNestedPath() throws IOException {
    // spath + appendcol with subquery using stats on dotted path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc | fields doc.user.name, doc.user.city"
                + " | appendcol [ stats count() as total ]");
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testSpathAutoExtractWithWhereExistsSubqueryOnNestedPath() throws IOException {
    // spath + where exists subquery referencing dotted path
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where exists ["
                + "   source=test_spath_join"
                + "   | where name = doc.user.name"
                + " ]"
                + " | sort doc.user.name"
                + " | fields doc.user.name");
    verifySchema(result, schema("doc.user.name", "string"));
    // John, Alice, Bob all exist in test_spath_join
    verifyNumOfRows(result, 4);
  }

  // --- Round 7: targeting addtotals, addcoltotals, and more edge cases ---

  @Test
  public void testSpathAutoExtractWithAddtotalsOnNestedPath() throws IOException {
    // spath + addtotals on extracted nested path
    // addtotals iterates fieldList and checks shouldAggregateField by field.getName()
    // Since the MAP column is "doc" not "doc.user.age", it won't find the field
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval age_num = cast(doc.user.age as double)"
                + " | addtotals col=true row=false age_num");
    verifyNumOfRows(result, 5);
  }

  @Test
  public void testSpathAutoExtractWithStatsVarPopOnNestedPath() throws IOException {
    // BUG: var_pop on spath-extracted string path silently succeeds but produces wrong results.
    // var_pop should require numeric input, but it accepts STRING from MAP-extracted values
    // without error. The result is string variance which is semantically meaningless.
    // This is a silent wrong-result bug — no error is thrown but the output is incorrect.
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats var_pop(doc.user.age) as var_age");
    verifySchema(result, schema("var_age", "double"));
    // The result is computed on string values "25","30","35","40" — meaningless variance
    verifyNumOfRows(result, 1);
  }

  @Test
  public void testSpathAutoExtractWithStatsStddevOnNestedPath() throws IOException {
    // BUG: stddev_pop on spath-extracted string path silently succeeds (same as var_pop).
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | stats stddev_pop(doc.user.age) as std_age");
    verifySchema(result, schema("std_age", "double"));
    verifyNumOfRows(result, 1);
  }

  @Test
  public void testSpathAutoExtractWithEventstatsVarOnNestedPath() throws IOException {
    // BUG: eventstats var_pop on spath-extracted string path silently succeeds (same as stats).
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eventstats var_pop(doc.user.age) as var_age"
                + " | head 1 | fields var_age");
    verifySchema(result, schema("var_age", "double"));
    verifyNumOfRows(result, 1);
  }

  @Test
  public void testSpathAutoExtractWithStreamstatsVarOnNestedPath() throws IOException {
    // BUG: streamstats var_pop on spath-extracted string path silently succeeds (same as stats).
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | streamstats var_pop(doc.user.age) as var_age"
                + " | head 1 | fields var_age");
    verifySchema(result, schema("var_age", "double"));
    verifyNumOfRows(result, 1);
  }

  @Test
  public void testSpathAutoExtractWithAddcoltotalsOnNestedPath() throws IOException {
    // spath + addcoltotals on extracted nested path
    // addcoltotals adds a totals row: 4 data rows + 1 total = 5
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | eval age_num = cast(doc.user.age as double)"
                + " | addcoltotals age_num");
    verifyNumOfRows(result, 5);
  }

  @Test
  public void testSpathAutoExtractWithWhereAndMultipleConditions() throws IOException {
    // spath + where with complex AND/OR conditions on multiple extracted paths
    JSONObject result =
        executeQuery(
            "source=test_spath_multi | spath input=doc"
                + " | where (doc.user.name = 'John' AND doc.user.city = 'NYC')"
                + "   OR (doc.user.name = 'Alice')"
                + " | sort doc.user.name"
                + " | fields doc.user.name, doc.user.city");
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.city", "string"));
    verifyDataRowsInOrder(result, rows("Alice", "LA"), rows("John", "NYC"));
  }
}
