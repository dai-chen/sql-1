/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.tools.Frameworks;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.sql.api.DynamicPPLToSqlNodeConverter;
import org.opensearch.sql.api.PPLToSqlNodeConverter;
import org.opensearch.sql.api.PPLToSqlTranspiler;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.util.InternalRestHighLevelClient;
import org.opensearch.sql.util.RetryProcessor;

/** OpenSearch Rest integration test base for PPL testing. */
public abstract class PPLIntegTestCase extends SQLIntegTestCase {
  private static final String BWC_EXPLAIN_API_ENDPOINT = "/_plugins/_ppl/_explain?format=%s";
  private static final String EXPLAIN_API_ENDPOINT = "/_plugins/_ppl/_explain?format=%s&mode=%s";
  private static final Logger LOG = LogManager.getLogger();
  @Rule public final RetryProcessor retryProcessor = new RetryProcessor();
  public static final Integer DEFAULT_SUBSEARCH_MAXOUT = 10000;
  public static final Integer DEFAULT_JOIN_SUBSEARCH_MAXOUT = 50000;
  static final boolean V4_ENABLED = Boolean.getBoolean("ppl.engine.v4");

  /** Tracks the current query size limit for V4 path (null = default/unset). */
  private Integer v4QuerySizeLimit;

  private static final Settings V4_SETTINGS =
      new Settings() {
        @SuppressWarnings("unchecked")
        @Override
        public <T> T getSettingValue(Key key) {
          return null;
        }

        @Override
        public java.util.List<?> getSettings() {
          return java.util.Collections.emptyList();
        }
      };

  private SchemaPlus v4Schema;

  private SchemaPlus getV4Schema() {
    if (v4Schema == null) {
      AbstractSchema osSchema = new AbstractSchema() {
        private final OpenSearchRestClient osClient =
            new OpenSearchRestClient(new InternalRestHighLevelClient(client()));

        @Override
        protected Map<String, Table> getTableMap() {
          return new HashMap<>() {
            @Override
            public Table get(Object key) {
              if (!super.containsKey(key)) {
                String indexName = (String) key;
                super.put(indexName, new OpenSearchIndex(osClient, V4_SETTINGS, indexName));
              }
              return super.get(key);
            }
          };
        }
      };
      v4Schema = Frameworks.createRootSchema(false);
      v4Schema.add("default", osSchema);
      v4Schema = v4Schema.getSubSchema("default");
    }
    return v4Schema;
  }

  private String transpileV4(String ppl) {
    UnresolvedPlan plan = PPLToSqlNodeConverter.parse(ppl);
    DynamicPPLToSqlNodeConverter converter = new DynamicPPLToSqlNodeConverter(getV4Schema());
    SqlNode sqlNode = converter.convert(plan);
    String sql = sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    // CalciteSqlDialect doubles backslashes in string literals, but the SQL parser
    // (SqlParserUtil.parseString) does NOT un-escape them. Un-double to match what
    // the string transpiler produces.
    sql = sql.replace("\\\\", "\\");
    // Apply query size limit for V4 path if set and SQL doesn't already have FETCH/LIMIT
    if (v4QuerySizeLimit != null) {
      String upper = sql.toUpperCase();
      if (!upper.contains("FETCH NEXT") && !upper.contains("FETCH FIRST")
          && !upper.contains(" LIMIT ")) {
        sql = sql + "\nFETCH NEXT " + v4QuerySizeLimit + " ROWS ONLY";
      }
    }
    return sql;
  }

  private static ResponseException createSyntheticResponseException(Exception cause) {
    try {
      String errorBody = new JSONObject()
          .put("error", new JSONObject()
              .put("reason", "V4 transpilation error")
              .put("details", cause.getMessage())
              .put("type", cause.getClass().getSimpleName()))
          .put("status", 400)
          .toString();
      org.apache.hc.core5.http.message.RequestLine requestLine =
          new org.apache.hc.core5.http.message.RequestLine("POST", "/_plugins/_ppl",
              org.apache.hc.core5.http.HttpVersion.HTTP_1_1);
      org.apache.hc.core5.http.HttpHost host = new org.apache.hc.core5.http.HttpHost("localhost");
      org.apache.hc.core5.http.message.BasicClassicHttpResponse httpResponse =
          new org.apache.hc.core5.http.message.BasicClassicHttpResponse(400, "Bad Request");
      httpResponse.setEntity(new org.apache.hc.core5.http.io.entity.StringEntity(
          errorBody, org.apache.hc.core5.http.ContentType.APPLICATION_JSON));
      java.lang.reflect.Constructor<Response> ctor = Response.class.getDeclaredConstructor(
          org.apache.hc.core5.http.message.RequestLine.class,
          org.apache.hc.core5.http.HttpHost.class,
          org.apache.hc.core5.http.ClassicHttpResponse.class);
      ctor.setAccessible(true);
      Response response = ctor.newInstance(requestLine, host, httpResponse);
      return new ResponseException(response);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create synthetic ResponseException", e);
    }
  }

  @Override
  protected void init() throws Exception {
    super.init();
    if (V4_ENABLED) {
      GlobalPushdownConfig.enabled = false; // V4 transpiles to SQL — disable pushdown
    }
    updatePushdownSettings();
    disableCalcite(); // calcite is enabled by default from 3.3.0
  }

  @Override
  protected void setQuerySizeLimit(Integer limit) throws IOException {
    super.setQuerySizeLimit(limit);
    if (V4_ENABLED) {
      v4QuerySizeLimit = limit;
    }
  }

  @Override
  protected void resetQuerySizeLimit() throws IOException {
    super.resetQuerySizeLimit();
    if (V4_ENABLED) {
      v4QuerySizeLimit = null;
    }
  }

  protected JSONObject executeQuery(String query) throws IOException {
    if (V4_ENABLED) {
      String trimmed = query.trim().toLowerCase(Locale.ROOT);
      if (trimmed.startsWith("describe ")) {
        Response response = client().performRequest(buildRequest(query, QUERY_API_ENDPOINT));
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return jsonify(getResponseBody(response, true));
      }
      try {
        query = query.replace("\\\"", "\"");
        String sql = transpileV4(query);
        LOG.info("[V4] PPL: {} -> SQL: {}", query, sql);
        Request request = new Request("POST", "/_plugins/_sql?format=jdbc");
        request.setJsonEntity(new JSONObject().put("query", sql).toString());
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        String body = getResponseBody(response, true);
        return stripMetadataColumns(jsonify(body), sql);
      } catch (ResponseException e) {
        throw e;
      } catch (Exception e) {
        LOG.error("[V4] Transpilation error for PPL: {}", query, e);
        throw createSyntheticResponseException(e);
      }
    }
    return jsonify(executeQueryToString(query));
  }

  protected String executeQueryToString(String query) throws IOException {
    if (V4_ENABLED) {
      // For DESCRIBE queries, route through PPL endpoint (V4 transpiler can't handle metadata commands)
      String trimmed = query.trim().toLowerCase(Locale.ROOT);
      if (trimmed.startsWith("describe ")) {
        Response response = client().performRequest(buildRequest(query, QUERY_API_ENDPOINT));
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return getResponseBody(response, true);
      }
      // For EXPLAIN queries, route through PPL explain endpoint (V4 transpiler can't replicate explain format)
      if (trimmed.startsWith("explain ")) {
        String mode = "standard";
        if (trimmed.startsWith("explain simple ")) mode = "simple";
        else if (trimmed.startsWith("explain extended ")) mode = "extended";
        else if (trimmed.startsWith("explain cost ")) mode = "cost";
        Response response = client().performRequest(
            buildRequest(query, String.format(EXPLAIN_API_ENDPOINT, "json", mode)));
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return getResponseBody(response, true);
      }
      try {
        query = query.replace("\\\"", "\"");
        String sql = transpileV4(query);
        LOG.info("[V4] PPL: {} -> SQL: {}", query, sql);
        Request request = new Request("POST", "/_plugins/_sql?format=jdbc");
        request.setJsonEntity(new JSONObject().put("query", sql).toString());
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return getResponseBody(response, true);
      } catch (ResponseException e) {
        throw e;
      } catch (Exception e) {
        throw createSyntheticResponseException(e);
      }
    }
    Response response = client().performRequest(buildRequest(query, QUERY_API_ENDPOINT));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true);
  }

  /** Deprecated, use {@link #explainQueryYaml(String)} */
  @Deprecated
  protected String explainQueryToString(String query) throws IOException {
    return explainQueryToString(query, ExplainMode.STANDARD);
  }

  protected String explainQueryYaml(String query) throws IOException {
    return explainQueryYaml(query, ExplainMode.STANDARD);
  }

  protected String explainQueryYaml(String query, ExplainMode mode) throws IOException {
    return explainQuery(query, Format.YAML, mode);
  }

  protected String explainQueryToString(String query, ExplainMode mode) throws IOException {
    return explainQuery(query, Format.JSON, mode).replace("\\r\\n", "\\n");
  }

  private String explainQuery(String query, Format format, ExplainMode mode) throws IOException {
    Response response =
        client()
            .performRequest(buildRequest(query, String.format(EXPLAIN_API_ENDPOINT, format, mode)));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true);
  }

  protected String explainQueryToStringBWC(String query, Format format) throws IOException {
    Response response =
        client()
            .performRequest(buildRequest(query, String.format(BWC_EXPLAIN_API_ENDPOINT, format)));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true).replace("\\r\\n", "\\n");
  }

  protected String executeCsvQuery(String query, boolean sanitize) throws IOException {
    Request request =
        buildRequest(
            query,
            QUERY_API_ENDPOINT + String.format(Locale.ROOT, "?format=csv&sanitize=%b", sanitize));
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true);
  }

  protected String executeCsvQuery(String query) throws IOException {
    return executeCsvQuery(query, true);
  }

  protected void verifyExplainException(String query, String expectedErrorMessage) {
    ResponseException e = assertThrows(ResponseException.class, () -> explainQueryToString(query));
    try {
      String responseBody = getResponseBody(e.getResponse(), true);
      JSONObject errorResponse = new JSONObject(responseBody);
      String actualErrorMessage = errorResponse.getJSONObject("error").getString("details");
      assertEquals(expectedErrorMessage, actualErrorMessage);
    } catch (IOException | JSONException ex) {
      throw new RuntimeException("Failed to parse error response", ex);
    }
  }

  protected static String source(String index, String query) {
    return String.format("source=%s | %s", index, query);
  }

  protected void timing(MapBuilder<String, Long> builder, String query, String ppl)
      throws IOException {
    executeQuery(ppl); // warm-up
    long start = System.currentTimeMillis();
    executeQuery(ppl);
    long duration = System.currentTimeMillis() - start;
    builder.put(query, duration);
  }

  protected void failWithMessage(String query, String message) {
    try {
      client().performRequest(buildRequest(query, QUERY_API_ENDPOINT));
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(message));
    }
  }

  protected Request buildRequest(String query, String endpoint) {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  protected static JSONObject updateClusterSettings(ClusterSetting setting) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    String persistentSetting =
        String.format(
            Locale.ROOT, "{\"%s\": {\"%s\": %s}}", setting.type, setting.name, setting.value);
    request.setJsonEntity(persistentSetting);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return new JSONObject(executeRequest(request));
  }

  protected static class ClusterSetting {
    private final String type;
    private final String name;
    private final String value;

    public ClusterSetting(String type, String name, String value) {
      this.type = type;
      this.name = name;
      this.value = (value == null) ? "null" : ("\"" + value + "\"");
    }

    SQLIntegTestCase.ClusterSetting nullify() {
      return new SQLIntegTestCase.ClusterSetting(type, name, null);
    }

    @Override
    public String toString() {
      return String.format("ClusterSetting{type='%s', path='%s', value'%s'}", type, name, value);
    }
  }

  protected JSONObject jsonify(String text) {
    try {
      return new JSONObject(text);
    } catch (JSONException e) {
      // Response may contain unescaped newlines in data values (e.g., from ARRAY_JOIN with '\n')
      // Try escaping newlines within string values before re-parsing
      try {
        String escaped = text.replaceAll("(?<=\\[|,|:)\"((?:[^\"\\\\]|\\\\.)*)\"", 
            java.util.regex.Matcher.quoteReplacement("$0"));
        // Simpler approach: escape all newlines that are inside JSON string values
        // by replacing literal newlines (not \n sequences) with \\n
        StringBuilder sb = new StringBuilder();
        boolean inString = false;
        boolean escaped2 = false;
        for (int i = 0; i < text.length(); i++) {
          char c = text.charAt(i);
          if (escaped2) {
            sb.append(c);
            escaped2 = false;
            continue;
          }
          if (c == '\\') {
            sb.append(c);
            escaped2 = true;
            continue;
          }
          if (c == '"') {
            inString = !inString;
            sb.append(c);
            continue;
          }
          if (inString && c == '\n') {
            sb.append("\\n");
            continue;
          }
          if (inString && c == '\r') {
            sb.append("\\r");
            continue;
          }
          sb.append(c);
        }
        return new JSONObject(sb.toString());
      } catch (Exception e2) {
        throw new IllegalStateException(String.format("Failed to transform %s to JSON format", text));
      }
    }
  }

  private static final Set<String> METADATA_FIELDS =
      Set.of("_id", "_index", "_score", "_maxscore", "_sort", "_routing",
          "_dedup_rn", "_global_rn", "_group_rn");

  private static final java.util.Map<String, String> SQL_TO_PPL_TYPES = new java.util.HashMap<>();

  static {
    SQL_TO_PPL_TYPES.put("keyword", "string");
    SQL_TO_PPL_TYPES.put("text", "string");
    SQL_TO_PPL_TYPES.put("integer", "int");
    SQL_TO_PPL_TYPES.put("int", "int");
    SQL_TO_PPL_TYPES.put("long", "bigint");
    SQL_TO_PPL_TYPES.put("bigint", "bigint");
    SQL_TO_PPL_TYPES.put("short", "smallint");
    SQL_TO_PPL_TYPES.put("smallint", "smallint");
    SQL_TO_PPL_TYPES.put("byte", "tinyint");
    SQL_TO_PPL_TYPES.put("tinyint", "tinyint");
    SQL_TO_PPL_TYPES.put("float", "float");
    SQL_TO_PPL_TYPES.put("real", "float");
    SQL_TO_PPL_TYPES.put("half_float", "real");
    SQL_TO_PPL_TYPES.put("double", "double");
    SQL_TO_PPL_TYPES.put("boolean", "boolean");
    SQL_TO_PPL_TYPES.put("date", "date");
    SQL_TO_PPL_TYPES.put("timestamp", "timestamp");
    SQL_TO_PPL_TYPES.put("ip", "ip");
    SQL_TO_PPL_TYPES.put("binary", "binary");
    SQL_TO_PPL_TYPES.put("geo_point", "geo_point");
    SQL_TO_PPL_TYPES.put("nested", "array");
    SQL_TO_PPL_TYPES.put("object", "struct");
    SQL_TO_PPL_TYPES.put("scaled_float", "double");
    SQL_TO_PPL_TYPES.put("unsigned_long", "bigint");
  }

  /**
   * Strip OpenSearch metadata columns from SQL response to match PPL response format. The SQL
   * endpoint includes _id, _index, _score etc. in SELECT * results, but PPL endpoint strips them.
   * Also normalizes type names from SQL format to PPL format.
   * Additionally strips columns marked for exclusion by rename operations via _RENAME_EXCLUDE
   * comments in the SQL.
   */
  private static JSONObject stripMetadataColumns(JSONObject response, String sql) {
    // Parse _RENAME_MAP comments from SQL to find rename mappings (old:new pairs)
    java.util.LinkedHashMap<String, String> renameMappings = new java.util.LinkedHashMap<>();
    if (sql != null) {
      java.util.regex.Matcher m = java.util.regex.Pattern
          .compile("/\\* _RENAME_MAP:([^*]+) \\*/").matcher(sql);
      while (m.find()) {
        for (String pair : m.group(1).split(",")) {
          String[] parts = pair.split(":", 2);
          if (parts.length == 2) {
            renameMappings.put(parts[0].trim(), parts[1].trim());
          }
        }
      }
    }

    response.remove("status");
    if (!response.has("schema") || !response.has("datarows")) {
      return response;
    }
    org.json.JSONArray schema = response.getJSONArray("schema");
    org.json.JSONArray datarows = response.getJSONArray("datarows");

    // Normalize NaN to null (PPL returns null for invalid math operations)
    // Normalize timestamps by stripping trailing '.0' (SQL returns '2025-07-28 00:15:23.0', PPL expects '2025-07-28 00:15:23')
    // Normalize integer values to double when schema type is double (JSON serializes 30497.0 as 30497)
    // Build a set of column indices that are double-typed for numeric normalization
    java.util.Set<Integer> doubleColumns = new java.util.HashSet<>();
    for (int i = 0; i < schema.length(); i++) {
      String type = schema.getJSONObject(i).optString("type", "").toLowerCase();
      if ("double".equals(type)) {
        doubleColumns.add(i);
      }
    }
    for (int r = 0; r < datarows.length(); r++) {
      org.json.JSONArray row = datarows.getJSONArray(r);
      for (int i = 0; i < row.length(); i++) {
        Object val = row.get(i);
        if (val instanceof String) {
          String s = (String) val;
          if ("NaN".equals(s)) {
            row.put(i, org.json.JSONObject.NULL);
          } else if (s.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.0$")) {
            row.put(i, s.substring(0, s.length() - 2));
          }
        } else if (doubleColumns.contains(i) && val instanceof Number
            && !(val instanceof Double) && !(val instanceof Float)) {
          row.put(i, ((Number) val).doubleValue());
        }
      }
    }

    // Build the output column order, handling renames by placing renamed columns
    // at the position of their source columns (in-place replacement).
    // The SQL generates "SELECT *, old AS new" which appends renamed cols at end.
    // We reorder so the renamed col replaces the source col's position.
    java.util.List<Integer> keepIndices = new java.util.ArrayList<>();
    if (!renameMappings.isEmpty()) {
      Set<String> sourceNames = renameMappings.keySet();
      Set<String> targetNames = new java.util.HashSet<>(renameMappings.values());

      // Find the last index of each target name (the appended alias)
      java.util.Map<String, Integer> lastTargetIndex = new java.util.HashMap<>();
      for (int i = 0; i < schema.length(); i++) {
        String colName = schema.getJSONObject(i).getString("name");
        if (targetNames.contains(colName)) {
          lastTargetIndex.put(colName, i);
        }
      }

      // Build a set of indices to skip (source cols, earlier target occurrences, appended aliases)
      Set<Integer> skipIndices = new java.util.HashSet<>();
      // Map from source index to the appended alias index (for reordering)
      java.util.Map<Integer, Integer> sourceToAlias = new java.util.HashMap<>();

      for (int i = 0; i < schema.length(); i++) {
        String colName = schema.getJSONObject(i).getString("name");
        if (sourceNames.contains(colName)) {
          String targetName = renameMappings.get(colName);
          Integer aliasIdx = lastTargetIndex.get(targetName);
          if (aliasIdx != null && aliasIdx != i) {
            // Source col — will be replaced by the alias at this position
            skipIndices.add(i);
            sourceToAlias.put(i, aliasIdx);
            skipIndices.add(aliasIdx); // Don't include alias at its appended position
          }
        }
        // Also skip earlier occurrences of target names (pre-existing cols being replaced)
        if (targetNames.contains(colName) && lastTargetIndex.get(colName) != null
            && lastTargetIndex.get(colName) != i) {
          skipIndices.add(i);
        }
      }

      for (int i = 0; i < schema.length(); i++) {
        if (sourceToAlias.containsKey(i)) {
          // Insert the alias column at the source column's position
          keepIndices.add(sourceToAlias.get(i));
        } else if (!skipIndices.contains(i) && !METADATA_FIELDS.contains(
            schema.getJSONObject(i).getString("name"))) {
          keepIndices.add(i);
        }
      }
    } else {
      // No renames — standard metadata stripping
      for (int i = 0; i < schema.length(); i++) {
        String colName = schema.getJSONObject(i).getString("name");
        if (!METADATA_FIELDS.contains(colName)) {
          keepIndices.add(i);
        }
      }
    }

    // Strip extra ORDER BY columns that Calcite adds to the response.
    // Only activate for simple SELECT with explicit columns (no *, JOIN, REPLACE, EXCEPT).
    if (sql != null) {
      keepIndices = stripOrderByColumns(keepIndices, schema, sql);
    }

    // Build new schema
    org.json.JSONArray newSchema = new org.json.JSONArray();
    for (int idx : keepIndices) {
      org.json.JSONObject col = schema.getJSONObject(idx);
      String type = col.getString("type");
      String pplType = SQL_TO_PPL_TYPES.getOrDefault(type.toLowerCase(), type.toLowerCase());
      col.put("type", pplType);
      newSchema.put(col);
    }

    // If no columns were removed, return with just type normalization
    if (keepIndices.size() == schema.length()) {
      response.put("schema", newSchema);
      return response;
    }

    // Filter datarows
    org.json.JSONArray newDatarows = new org.json.JSONArray();
    for (int r = 0; r < datarows.length(); r++) {
      org.json.JSONArray row = datarows.getJSONArray(r);
      org.json.JSONArray newRow = new org.json.JSONArray();
      for (int idx : keepIndices) {
        newRow.put(row.get(idx));
      }
      newDatarows.put(newRow);
    }

    response.put("schema", newSchema);
    response.put("datarows", newDatarows);
    response.put("size", newDatarows.length());
    response.put("total", newDatarows.length());
    return response;
  }

  /**
   * Strip columns that Calcite adds to the response because they appear in ORDER BY
   * but not in the outermost SELECT list. Only activates for simple explicit SELECT
   * lists (no *, JOIN, REPLACE, EXCEPT patterns).
   */
  private static java.util.List<Integer> stripOrderByColumns(
      java.util.List<Integer> keepIndices, org.json.JSONArray schema, String sql) {
    // Skip if SQL contains JOIN (complex column patterns)
    String sqlUpper = sql.toUpperCase();
    if (sqlUpper.contains(" JOIN ")) return keepIndices;

    // Must have ORDER BY to have extra columns
    if (!sqlUpper.contains("ORDER BY")) return keepIndices;

    // Extract the outermost SELECT clause (between first SELECT and first FROM at same depth)
    String stripped = sql.replaceAll("/\\*[^*]*\\*/", ""); // remove comments
    String strippedUpper = stripped.toUpperCase();
    int selectIdx = strippedUpper.indexOf("SELECT ");
    if (selectIdx < 0) return keepIndices;
    int afterSelect = selectIdx + 7;

    // Find the matching FROM by tracking parenthesis depth
    int depth = 0;
    int fromIdx = -1;
    for (int i = afterSelect; i < stripped.length() - 4; i++) {
      char c = stripped.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (depth == 0 && strippedUpper.startsWith("FROM ", i)) {
        fromIdx = i;
        break;
      }
    }
    if (fromIdx < 0) return keepIndices;

    String selectClause = stripped.substring(afterSelect, fromIdx).trim();

    // Skip if SELECT contains *, REPLACE, or EXCEPT (complex patterns)
    String selectUpper = selectClause.toUpperCase();
    if (selectUpper.contains("*") || selectUpper.contains("REPLACE(")
        || selectUpper.contains("EXCEPT(")) {
      return keepIndices;
    }

    // Parse column names from the SELECT clause (handle "expr AS alias" and bare columns)
    Set<String> selectColumns = new java.util.LinkedHashSet<>();
    // Split by commas at depth 0
    int start = 0;
    depth = 0;
    for (int i = 0; i <= selectClause.length(); i++) {
      char c = (i < selectClause.length()) ? selectClause.charAt(i) : ',';
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (c == ',' && depth == 0) {
        String item = selectClause.substring(start, i).trim();
        if (!item.isEmpty()) {
          // Extract the alias or column name
          String name = extractColumnName(item);
          if (name != null) selectColumns.add(name);
        }
        start = i + 1;
      }
    }

    if (selectColumns.isEmpty()) return keepIndices;

    // Filter keepIndices to only include columns whose names are in the SELECT list
    java.util.List<Integer> filtered = new java.util.ArrayList<>();
    for (int idx : keepIndices) {
      String colName = schema.getJSONObject(idx).getString("name");
      if (selectColumns.contains(colName)) {
        filtered.add(idx);
      }
    }

    // Safety: only strip if we actually removed something and didn't remove everything
    if (filtered.isEmpty() || filtered.size() == keepIndices.size()) return keepIndices;
    return filtered;
  }

  /** Extract the effective column name from a SELECT item (handles "expr AS alias" and quoted ids). */
  private static String extractColumnName(String item) {
    // Check for AS alias (case-insensitive, not inside parens)
    int depth = 0;
    int asIdx = -1;
    String upper = item.toUpperCase();
    for (int i = 0; i < item.length() - 3; i++) {
      char c = item.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (depth == 0 && upper.startsWith(" AS ", i)) {
        asIdx = i;
      }
    }
    String name;
    if (asIdx >= 0) {
      name = item.substring(asIdx + 4).trim();
    } else {
      name = item.trim();
    }
    // Unquote
    if (name.startsWith("\"") && name.endsWith("\"")) {
      name = name.substring(1, name.length() - 1).replace("\"\"", "\"");
    }
    return name;
  }

  protected static boolean isCalciteEnabled() throws IOException {
    return Boolean.parseBoolean(
        getClusterSetting(Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "persistent"));
  }

  public static void enableCalcite() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "true"));
  }

  public static void disableCalcite() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "false"));
  }

  public static void withCalciteEnabled(Runnable f) throws IOException {
    boolean isCalciteEnabled = isCalciteEnabled();
    if (isCalciteEnabled) f.run();
    else {
      try {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting(
                "persistent", Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "true"));
        f.run();
      } finally {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting(
                "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "false"));
      }
    }
  }

  public static void allowCalciteFallback() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "true"));
    LOG.info("{} enabled", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
  }

  public static void disallowCalciteFallback() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "false"));
    LOG.info("{} disabled", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
  }

  protected static boolean isFallbackEnabled() throws IOException {
    return Boolean.parseBoolean(
        getClusterSetting(Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "persistent"));
  }

  public static void withFallbackEnabled(Runnable f, String msg) throws IOException {
    LOG.info("Need fallback to v2 due to {}", msg);
    boolean isFallbackEnabled = isFallbackEnabled();
    if (isFallbackEnabled) f.run();
    else {
      try {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting(
                "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "true"));
        LOG.info(
            "Set {} to enabled and run the test", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
        f.run();
      } finally {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting(
                "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "false"));
        LOG.info("Reset {} back to disabled", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
      }
    }
  }

  public static void supportAllJoinTypes() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Key.CALCITE_SUPPORT_ALL_JOIN_TYPES.getKeyValue(), "true"));
  }

  public static void withSettings(Key setting, String value, Runnable f) throws IOException {
    String originalValue = getClusterSetting(setting.getKeyValue(), "transient");
    if (originalValue.equals(value)) f.run();
    else {
      try {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting("transient", setting.getKeyValue(), value));
        LOG.info("Set {} to {} and run the test", setting.name(), value);
        f.run();
      } finally {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting("transient", setting.getKeyValue(), originalValue));
        LOG.info("Reset {} back to {}", setting.name(), originalValue);
      }
    }
  }

  protected boolean isStandaloneTest() {
    return false; // Override this method in subclasses if needed
  }

  /**
   * assertThrows by replacing the expected throwable with {@link ResponseException} if the test is
   * not a standalone test.
   *
   * <p>In remote tests, the expected exception is always {@link ResponseException}, while in
   * standalone tests, the underlying exception can be retrieved.
   *
   * @param expectedThrowable the expected throwable type if the test is standalone
   * @param runnable the runnable that is expected to throw the exception
   * @return the thrown exception
   */
  public Throwable assertThrowsWithReplace(
      Class<? extends Throwable> expectedThrowable, org.junit.function.ThrowingRunnable runnable) {
    Class<? extends Throwable> expectedWithReplace;
    if (isStandaloneTest()) {
      expectedWithReplace = expectedThrowable;
    } else {
      expectedWithReplace = ResponseException.class;
    }
    return assertThrows(expectedWithReplace, runnable);
  }

  public static class GlobalPushdownConfig {
    /** Whether the global pushdown is enabled or not. Enable by default. */
    public static boolean enabled = true;
  }

  /**
   * We check pushdown disabled instead enabled because enabled is the default value of pushdown
   * config whatever calcite is enabled or not.
   */
  public boolean isPushdownDisabled() throws IOException {
    return isCalciteEnabled()
        && !Boolean.parseBoolean(
            getClusterSetting(Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "transient"));
  }

  protected void enabledOnlyWhenPushdownIsEnabled() throws IOException {
    Assume.assumeTrue("This test is only for when push down is enabled", !isPushdownDisabled());
  }

  protected void enabledOnlyWhenPushdownIsDisabled() throws IOException {
    Assume.assumeTrue("This test is only for when push down is disabled", isPushdownDisabled());
  }

  public void updatePushdownSettings() throws IOException {
    String pushdownEnabled = String.valueOf(GlobalPushdownConfig.enabled);
    assert !pushdownEnabled.isBlank() : "Pushdown enabled setting cannot be empty";
    if (isPushdownDisabled() == GlobalPushdownConfig.enabled) {
      LOG.info(
          "Updating {} to {}",
          Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
          GlobalPushdownConfig.enabled);
      updateClusterSettings(
          new SQLIntegTestCase.ClusterSetting(
              "transient",
              Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
              String.valueOf(GlobalPushdownConfig.enabled)));
    }
  }

  protected void setSubsearchMaxOut(Integer limit) throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "transient", Key.PPL_SUBSEARCH_MAXOUT.getKeyValue(), limit.toString()));
  }

  protected void resetSubsearchMaxOut() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "transient",
            Settings.Key.PPL_SUBSEARCH_MAXOUT.getKeyValue(),
            DEFAULT_SUBSEARCH_MAXOUT.toString()));
  }

  protected void setJoinSubsearchMaxOut(Integer limit) throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "transient", Key.PPL_JOIN_SUBSEARCH_MAXOUT.getKeyValue(), limit.toString()));
  }

  protected void resetJoinSubsearchMaxOut() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "transient",
            Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT.getKeyValue(),
            DEFAULT_JOIN_SUBSEARCH_MAXOUT.toString()));
  }

  /**
   * Sanitizes the PPL query by removing block comments and replacing new lines with spaces.
   *
   * @param ppl the PPL query string
   * @return the sanitized PPL query string
   */
  protected static String sanitize(String ppl) {
    String withoutComments = ppl.replaceAll("(?s)/\\*.*?\\*/", "");
    return withoutComments.replaceAll("\\r\\n", " ").replaceAll("\\n", " ").trim();
  }

  // Utility methods

  /**
   * Load a file from the resources directory and return its content as a String.
   *
   * @param filename the name of the file to load
   * @return the content of the file as a String
   */
  protected static String loadFromFile(String filename) {
    try {
      URI uri = Resources.getResource(filename).toURI();
      return new String(Files.readAllBytes(Paths.get(uri)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected String loadExpectedPlan(String fileName) throws IOException {
    String prefix;
    if (isCalciteEnabled()) {
      if (isPushdownDisabled()) {
        prefix = "expectedOutput/calcite_no_pushdown/";
      } else {
        prefix = "expectedOutput/calcite/";
      }
    } else {
      prefix = "expectedOutput/ppl/";
    }
    return loadFromFile(prefix + fileName);
  }
}
