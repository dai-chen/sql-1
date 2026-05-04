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
 * Seeds the canonical Oracle/Calcite "scott" schema into OpenSearch as two indices:
 * {@code scott_emp} (14 rows) and {@code scott_dept} (4 rows). Used by {@code .iq} files that
 * reference the classic EMP/DEPT tables for JOIN / aggregate / window-function tests.
 *
 * <p>Data is the public-domain SCOTT schema from Oracle sample databases — it's tiny (14 + 4
 * rows), has well-understood referential integrity (EMP.DEPTNO → DEPT.DEPTNO), and includes a
 * self-referential column (EMP.MGR → EMP.EMPNO) useful for recursive-CTE tests.
 *
 * <p>Call {@link #seed(RestClient)} from a {@code @BeforeClass} so the data lives for the duration
 * of a test run and is torn down with the testcluster. The seeder is idempotent: if the indices
 * already exist they're deleted and recreated so every run starts from a known state.
 */
public final class ScottSchemaSeeder {

  private ScottSchemaSeeder() {}

  public static void seed(RestClient client) throws IOException {
    deleteIfExists(client, "scott_emp");
    deleteIfExists(client, "scott_dept");
    createEmpIndex(client);
    createDeptIndex(client);
    bulkLoadEmp(client);
    bulkLoadDept(client);
    refresh(client, "scott_emp");
    refresh(client, "scott_dept");
  }

  private static void deleteIfExists(RestClient client, String index) throws IOException {
    Request req = new Request("DELETE", "/" + index);
    // 404 is fine — index didn't exist, which is the state we want.
    req.addParameter("ignore_unavailable", "true");
    try {
      client.performRequest(req);
    } catch (org.opensearch.client.ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  private static void createEmpIndex(RestClient client) throws IOException {
    Request req = new Request("PUT", "/scott_emp");
    req.setJsonEntity(
        "{\n"
            + "  \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"EMPNO\":    { \"type\": \"integer\" },\n"
            + "      \"ENAME\":    { \"type\": \"keyword\" },\n"
            + "      \"JOB\":      { \"type\": \"keyword\" },\n"
            + "      \"MGR\":      { \"type\": \"integer\" },\n"
            + "      \"HIREDATE\": { \"type\": \"date\",    \"format\": \"yyyy-MM-dd\" },\n"
            + "      \"SAL\":      { \"type\": \"double\" },\n"
            + "      \"COMM\":     { \"type\": \"double\" },\n"
            + "      \"DEPTNO\":   { \"type\": \"integer\" }\n"
            + "    }\n"
            + "  }\n"
            + "}");
    client.performRequest(req);
  }

  private static void createDeptIndex(RestClient client) throws IOException {
    Request req = new Request("PUT", "/scott_dept");
    req.setJsonEntity(
        "{\n"
            + "  \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"DEPTNO\": { \"type\": \"integer\" },\n"
            + "      \"DNAME\":  { \"type\": \"keyword\" },\n"
            + "      \"LOC\":    { \"type\": \"keyword\" }\n"
            + "    }\n"
            + "  }\n"
            + "}");
    client.performRequest(req);
  }

  private static void bulkLoadEmp(RestClient client) throws IOException {
    // Canonical SCOTT.EMP data from Oracle's sample database (public domain).
    // 14 rows covering ANALYST, CLERK, MANAGER, PRESIDENT, SALESMAN jobs across 3 depts.
    String bulk =
        line("scott_emp", 7369) + row(7369, "SMITH",  "CLERK",     7902, "1980-12-17",  800.00, null,   20)
      + line("scott_emp", 7499) + row(7499, "ALLEN",  "SALESMAN",  7698, "1981-02-20", 1600.00, 300.00, 30)
      + line("scott_emp", 7521) + row(7521, "WARD",   "SALESMAN",  7698, "1981-02-22", 1250.00, 500.00, 30)
      + line("scott_emp", 7566) + row(7566, "JONES",  "MANAGER",   7839, "1981-04-02", 2975.00, null,   20)
      + line("scott_emp", 7654) + row(7654, "MARTIN", "SALESMAN",  7698, "1981-09-28", 1250.00, 1400.0, 30)
      + line("scott_emp", 7698) + row(7698, "BLAKE",  "MANAGER",   7839, "1981-05-01", 2850.00, null,   30)
      + line("scott_emp", 7782) + row(7782, "CLARK",  "MANAGER",   7839, "1981-06-09", 2450.00, null,   10)
      + line("scott_emp", 7788) + row(7788, "SCOTT",  "ANALYST",   7566, "1987-04-19", 3000.00, null,   20)
      + line("scott_emp", 7839) + row(7839, "KING",   "PRESIDENT", null, "1981-11-17", 5000.00, null,   10)
      + line("scott_emp", 7844) + row(7844, "TURNER", "SALESMAN",  7698, "1981-09-08", 1500.00, 0.00,   30)
      + line("scott_emp", 7876) + row(7876, "ADAMS",  "CLERK",     7788, "1987-05-23", 1100.00, null,   20)
      + line("scott_emp", 7900) + row(7900, "JAMES",  "CLERK",     7698, "1981-12-03",  950.00, null,   30)
      + line("scott_emp", 7902) + row(7902, "FORD",   "ANALYST",   7566, "1981-12-03", 3000.00, null,   20)
      + line("scott_emp", 7934) + row(7934, "MILLER", "CLERK",     7782, "1982-01-23", 1300.00, null,   10);
    bulkPost(client, bulk);
  }

  private static void bulkLoadDept(RestClient client) throws IOException {
    String bulk =
        line("scott_dept", 10) + deptRow(10, "ACCOUNTING", "NEW YORK")
      + line("scott_dept", 20) + deptRow(20, "RESEARCH",   "DALLAS")
      + line("scott_dept", 30) + deptRow(30, "SALES",      "CHICAGO")
      + line("scott_dept", 40) + deptRow(40, "OPERATIONS", "BOSTON");
    bulkPost(client, bulk);
  }

  private static String line(String index, int id) {
    return "{\"index\":{\"_index\":\"" + index + "\",\"_id\":\"" + id + "\"}}\n";
  }

  private static String row(int empno, String ename, String job, Integer mgr, String hiredate,
                             double sal, Double comm, int deptno) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"EMPNO\":").append(empno);
    sb.append(",\"ENAME\":\"").append(ename).append('"');
    sb.append(",\"JOB\":\"").append(job).append('"');
    sb.append(",\"MGR\":").append(mgr == null ? "null" : mgr.toString());
    sb.append(",\"HIREDATE\":\"").append(hiredate).append('"');
    sb.append(",\"SAL\":").append(sal);
    sb.append(",\"COMM\":").append(comm == null ? "null" : comm.toString());
    sb.append(",\"DEPTNO\":").append(deptno);
    sb.append("}\n");
    return sb.toString();
  }

  private static String deptRow(int deptno, String dname, String loc) {
    return "{\"DEPTNO\":" + deptno + ",\"DNAME\":\"" + dname + "\",\"LOC\":\"" + loc + "\"}\n";
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
    client.performRequest(new Request("POST", "/" + index + "/_refresh"));
  }
}
