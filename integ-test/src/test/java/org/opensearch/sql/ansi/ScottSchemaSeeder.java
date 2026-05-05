/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ansi;

import java.io.IOException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

/**
 * Seeds test data for Calcite .iq golden-file tests. Creates indices matching both the "scott"
 * schema (14-row EMP, 4-row DEPT) and the "post" schema (EMPS with 5 rows) used by Apache
 * Calcite's test suite.
 *
 * <p>The seeder is idempotent: indices are deleted and recreated on each run.
 */
public final class ScottSchemaSeeder {

  private static final String PARQUET_SETTINGS =
      "\"number_of_shards\": 1, \"number_of_replicas\": 0,"
          + " \"pluggable.dataformat.enabled\": true,"
          + " \"pluggable.dataformat\": \"composite\","
          + " \"composite.primary_data_format\": \"parquet\"";

  private ScottSchemaSeeder() {}

  public static void seed(RestClient client) throws IOException {
    seedEmp(client);
    seedDept(client);
    seedEmps(client);
    seedDays(client);
  }

  private static void seedEmp(RestClient client) throws IOException {
    deleteIfExists(client, "emp");
    Request req = new Request("PUT", "/emp");
    req.setJsonEntity("{\n"
        + "  \"settings\": { " + PARQUET_SETTINGS + " },\n"
        + "  \"mappings\": { \"properties\": {\n"
        + "    \"ename\":    { \"type\": \"keyword\" },\n"
        + "    \"deptno\":   { \"type\": \"integer\" },\n"
        + "    \"gender\":   { \"type\": \"keyword\" }\n"
        + "  }}\n}");
    client.performRequest(req);

    // Post schema EMP: 9 rows (matches Calcite's !use post expected output)
    String bulk = ""
        + idx("emp") + "{\"ename\":\"Jane\",\"deptno\":10,\"gender\":\"F\"}\n"
        + idx("emp") + "{\"ename\":\"Bob\",\"deptno\":10,\"gender\":\"M\"}\n"
        + idx("emp") + "{\"ename\":\"Eric\",\"deptno\":20,\"gender\":\"M\"}\n"
        + idx("emp") + "{\"ename\":\"Susan\",\"deptno\":30,\"gender\":\"F\"}\n"
        + idx("emp") + "{\"ename\":\"Alice\",\"deptno\":30,\"gender\":\"F\"}\n"
        + idx("emp") + "{\"ename\":\"Adam\",\"deptno\":50,\"gender\":\"M\"}\n"
        + idx("emp") + "{\"ename\":\"Eve\",\"deptno\":50,\"gender\":\"F\"}\n"
        + idx("emp") + "{\"ename\":\"Grace\",\"deptno\":60,\"gender\":\"F\"}\n"
        + idx("emp") + "{\"ename\":\"Wilma\",\"deptno\":null,\"gender\":\"F\"}\n";
    bulkPost(client, bulk);
    refresh(client, "emp");
  }

  private static void seedDept(RestClient client) throws IOException {
    deleteIfExists(client, "dept");
    Request req = new Request("PUT", "/dept");
    req.setJsonEntity("{\n"
        + "  \"settings\": { " + PARQUET_SETTINGS + " },\n"
        + "  \"mappings\": { \"properties\": {\n"
        + "    \"deptno\": { \"type\": \"integer\" },\n"
        + "    \"dname\":  { \"type\": \"keyword\" }\n"
        + "  }}\n}");
    client.performRequest(req);

    // Post schema DEPT: matches Calcite's !use post expected output
    String bulk = ""
        + idx("dept") + "{\"deptno\":10,\"dname\":\"Sales\"}\n"
        + idx("dept") + "{\"deptno\":20,\"dname\":\"Marketing\"}\n"
        + idx("dept") + "{\"deptno\":30,\"dname\":\"Engineering\"}\n"
        + idx("dept") + "{\"deptno\":40,\"dname\":\"Empty\"}\n";
    bulkPost(client, bulk);
    refresh(client, "dept");
  }

  private static void seedEmps(RestClient client) throws IOException {
    deleteIfExists(client, "emps");
    Request req = new Request("PUT", "/emps");
    req.setJsonEntity("{\n"
        + "  \"settings\": { " + PARQUET_SETTINGS + " },\n"
        + "  \"mappings\": { \"properties\": {\n"
        + "    \"empno\":   { \"type\": \"integer\" },\n"
        + "    \"name\":    { \"type\": \"keyword\" },\n"
        + "    \"deptno\":  { \"type\": \"integer\" },\n"
        + "    \"gender\":  { \"type\": \"keyword\" },\n"
        + "    \"city\":    { \"type\": \"keyword\" },\n"
        + "    \"empid\":   { \"type\": \"integer\" },\n"
        + "    \"age\":     { \"type\": \"integer\" },\n"
        + "    \"slacker\":  { \"type\": \"boolean\" },\n"
        + "    \"manager\": { \"type\": \"boolean\" },\n"
        + "    \"joinedat\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd\" }\n"
        + "  }}\n}");
    client.performRequest(req);

    // Post schema EMPS table (5 rows) — used by Calcite's !use post tests
    String bulk = ""
        + idx("emps") + "{\"empno\":100,\"name\":\"Fred\",\"deptno\":10,\"gender\":null,\"city\":null,\"empid\":40,\"age\":25,\"slacker\":true,\"manager\":false,\"joinedat\":\"1996-08-03\"}\n"
        + idx("emps") + "{\"empno\":110,\"name\":\"Eric\",\"deptno\":20,\"gender\":\"M\",\"city\":\"San Francisco\",\"empid\":3,\"age\":80,\"slacker\":null,\"manager\":false,\"joinedat\":\"2001-01-01\"}\n"
        + idx("emps") + "{\"empno\":110,\"name\":\"John\",\"deptno\":40,\"gender\":\"M\",\"city\":\"Vancouver\",\"empid\":2,\"age\":null,\"slacker\":false,\"manager\":true,\"joinedat\":\"2002-05-03\"}\n"
        + idx("emps") + "{\"empno\":120,\"name\":\"Wilma\",\"deptno\":20,\"gender\":\"F\",\"city\":null,\"empid\":1,\"age\":5,\"slacker\":null,\"manager\":true,\"joinedat\":\"2005-09-07\"}\n"
        + idx("emps") + "{\"empno\":130,\"name\":\"Alice\",\"deptno\":40,\"gender\":\"F\",\"city\":\"Vancouver\",\"empid\":2,\"age\":null,\"slacker\":false,\"manager\":true,\"joinedat\":\"2007-01-01\"}\n";
    bulkPost(client, bulk);
    refresh(client, "emps");
  }

  private static void seedDays(RestClient client) throws IOException {
    deleteIfExists(client, "days");
    Request req = new Request("PUT", "/days");
    req.setJsonEntity("{\n"
        + "  \"settings\": { " + PARQUET_SETTINGS + " },\n"
        + "  \"mappings\": { \"properties\": {\n"
        + "    \"day\":      { \"type\": \"integer\" },\n"
        + "    \"week_day\": { \"type\": \"keyword\" }\n"
        + "  }}\n}");
    client.performRequest(req);

    String bulk = ""
        + idx("days") + "{\"day\":1,\"week_day\":\"Sunday\"}\n"
        + idx("days") + "{\"day\":2,\"week_day\":\"Monday\"}\n"
        + idx("days") + "{\"day\":3,\"week_day\":\"Tuesday\"}\n"
        + idx("days") + "{\"day\":4,\"week_day\":\"Wednesday\"}\n"
        + idx("days") + "{\"day\":5,\"week_day\":\"Thursday\"}\n"
        + idx("days") + "{\"day\":6,\"week_day\":\"Friday\"}\n"
        + idx("days") + "{\"day\":7,\"week_day\":\"Saturday\"}\n";
    bulkPost(client, bulk);
    refresh(client, "days");
  }

  private static String idx(String index) {
    return "{\"index\":{\"_index\":\"" + index + "\"}}\n";
  }

  private static void deleteIfExists(RestClient client, String index) throws IOException {
    Request req = new Request("DELETE", "/" + index);
    req.addParameter("ignore_unavailable", "true");
    try {
      client.performRequest(req);
    } catch (org.opensearch.client.ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  private static void bulkPost(RestClient client, String body) throws IOException {
    Request req = new Request("POST", "/_bulk");
    req.setJsonEntity(body);
    Response resp = client.performRequest(req);
    if (resp.getStatusLine().getStatusCode() >= 300) {
      throw new IOException("Bulk load failed: " + resp.getStatusLine());
    }
  }

  private static void refresh(RestClient client, String index) throws IOException {
    try {
      client.performRequest(new Request("POST", "/" + index + "/_refresh"));
    } catch (Exception e) {
      // Parquet indices may not support refresh — ignore
    }
  }
}
