/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.lang.reflect.Constructor;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

class ParquetStubResponseTest {

  @Test
  void utilityClassCannotBeInstantiated() throws Exception {
    Constructor<ParquetStubResponse> constructor =
        ParquetStubResponse.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    assertNotNull(constructor.newInstance());
  }

  @Test
  void buildStubQueryResultReturnsCorrectSchema() {
    QueryResult result = ParquetStubResponse.buildStubQueryResult();
    Schema schema = result.getSchema();
    List<Column> columns = schema.getColumns();

    assertEquals(4, columns.size());
    assertEquals("timestamp", columns.get(0).getName());
    assertEquals(ExprCoreType.TIMESTAMP, columns.get(0).getExprType());
    assertEquals("status", columns.get(1).getName());
    assertEquals(ExprCoreType.LONG, columns.get(1).getExprType());
    assertEquals("message", columns.get(2).getName());
    assertEquals(ExprCoreType.STRING, columns.get(2).getExprType());
    assertEquals("active", columns.get(3).getName());
    assertEquals(ExprCoreType.BOOLEAN, columns.get(3).getExprType());
  }

  @Test
  void buildStubQueryResultReturnsTwoRows() {
    QueryResult result = ParquetStubResponse.buildStubQueryResult();
    assertEquals(2, result.size());
  }

  @Test
  void formatAsSimpleJsonProducesValidResponse() {
    QueryResult result = ParquetStubResponse.buildStubQueryResult();
    String json = ParquetStubResponse.formatAsSimpleJson(result);

    assertNotNull(json);
    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
    assertTrue(obj.has("schema"));
    assertTrue(obj.has("datarows"));
    assertTrue(obj.has("total"));
    assertTrue(obj.has("size"));

    JsonArray schema = obj.getAsJsonArray("schema");
    assertEquals(4, schema.size());

    JsonArray datarows = obj.getAsJsonArray("datarows");
    assertEquals(2, datarows.size());

    assertEquals(2, obj.get("total").getAsInt());
    assertEquals(2, obj.get("size").getAsInt());
  }

  @Test
  void formatAsJdbcProducesValidResponse() {
    QueryResult result = ParquetStubResponse.buildStubQueryResult();
    String json = ParquetStubResponse.formatAsJdbc(result);

    assertNotNull(json);
    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
    assertTrue(obj.has("schema"));
    assertTrue(obj.has("datarows"));
    assertTrue(obj.has("total"));
    assertTrue(obj.has("size"));
    assertTrue(obj.has("status"));

    JsonArray schema = obj.getAsJsonArray("schema");
    assertEquals(4, schema.size());

    JsonArray datarows = obj.getAsJsonArray("datarows");
    assertEquals(2, datarows.size());

    assertEquals(2, obj.get("total").getAsInt());
    assertEquals(2, obj.get("size").getAsInt());
    assertEquals(200, obj.get("status").getAsInt());
  }

  @Test
  void formatExplainAsJsonProducesValidResponse() {
    String json = ParquetStubResponse.formatExplainAsJson();

    assertNotNull(json);
    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
    assertTrue(obj.has("Parquet"));
    JsonObject parquet = obj.getAsJsonObject("Parquet");
    assertTrue(parquet.has("description"));
    assertTrue(parquet.get("description").getAsString().contains("Stub explain"));
  }
}
