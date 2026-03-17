/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.*;
import static org.junit.Assert.*;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Demonstrates the RelNode object tree via Calcite's RelJsonWriter JSON dump. Each test asserts the
 * full JSON to clearly show what's in the RelNode for PoC demo purposes.
 */
public class UnifiedQueryRelNodeDemoTest extends UnifiedQueryTestBase {

  @Override
  @Before
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "parquet_index",
                SimpleTable.builder()
                    .col("ts", TIMESTAMP)
                    .col("dt", DATE)
                    .col("tm", TIME)
                    .col("data", VARBINARY)
                    .col("ip_addr", VARCHAR)
                    .col("status", INTEGER)
                    .col("message", VARCHAR)
                    .build());
          }
        };
    context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog(DEFAULT_CATALOG, testSchema)
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  /**
   * LogicalTableScan is a standard Calcite node referencing ["catalog", "parquet_index"]. No custom
   * OpenSearch scan node — the Analytics engine will replace this with its own physical scan.
   */
  @Test
  public void demoTableScanIsStandardCalcite() {
    RelNode plan = planner.plan("source = catalog.parquet_index | fields status");
    assertEquals(
        """
        {
          "rels": [
            {
              "id": "0",
              "relOp": "LogicalTableScan",
              "table": [
                "catalog",
                "parquet_index"
              ],
              "inputs": []
            },
            {
              "id": "1",
              "relOp": "LogicalProject",
              "fields": [
                "status"
              ],
              "exprs": [
                {
                  "input": 5,
                  "name": "$5"
                }
              ]
            }
          ]
        }\
        """,
        toRelJson(plan));
  }

  /**
   * HOUR is registered as a PPL UDF (UserDefinedFunctionBuilder) but produces standard Calcite
   * INTEGER type — no UDT. The Analytics engine sees a standard function call it can evaluate.
   */
  @Test
  public void demoDatetimeFunctionProducesStandardTypes() {
    RelNode plan =
        planner.plan("source = catalog.parquet_index | eval hour = hour(ts) | fields hour, status");
    assertEquals(
        """
        {
          "rels": [
            {
              "id": "0",
              "relOp": "LogicalTableScan",
              "table": [
                "catalog",
                "parquet_index"
              ],
              "inputs": []
            },
            {
              "id": "1",
              "relOp": "LogicalProject",
              "fields": [
                "hour",
                "status"
              ],
              "exprs": [
                {
                  "op": {
                    "name": "HOUR",
                    "kind": "OTHER_FUNCTION",
                    "syntax": "FUNCTION"
                  },
                  "operands": [
                    {
                      "input": 0,
                      "name": "$0"
                    }
                  ],
                  "class": "org.opensearch.sql.expression.function.UserDefinedFunctionBuilder$1",
                  "type": {
                    "type": "INTEGER",
                    "nullable": true
                  },
                  "deterministic": true,
                  "dynamic": false
                },
                {
                  "input": 5,
                  "name": "$5"
                }
              ]
            }
          ]
        }\
        """,
        toRelJson(plan));
  }

  /**
   * match() is a PPL full-text search UDF with MAP-typed arguments (field name and query string).
   * The Analytics engine must handle or reject this — it's not a standard Calcite/SQL function.
   */
  @Test
  public void demoMatchFunctionIsPplUdf() {
    RelNode plan =
        planner.plan(
            "source = catalog.parquet_index | where match(message, 'error') | fields message");
    assertEquals(
        """
        {
          "rels": [
            {
              "id": "0",
              "relOp": "LogicalTableScan",
              "table": [
                "catalog",
                "parquet_index"
              ],
              "inputs": []
            },
            {
              "id": "1",
              "relOp": "LogicalFilter",
              "condition": {
                "op": {
                  "name": "match",
                  "kind": "OTHER_FUNCTION",
                  "syntax": "FUNCTION"
                },
                "operands": [
                  {
                    "op": {
                      "name": "MAP",
                      "kind": "MAP_VALUE_CONSTRUCTOR",
                      "syntax": "SPECIAL"
                    },
                    "operands": [
                      {
                        "literal": "field",
                        "type": {
                          "type": "CHAR",
                          "nullable": false,
                          "precision": 5
                        }
                      },
                      {
                        "input": 6,
                        "name": "$6"
                      }
                    ]
                  },
                  {
                    "op": {
                      "name": "MAP",
                      "kind": "MAP_VALUE_CONSTRUCTOR",
                      "syntax": "SPECIAL"
                    },
                    "operands": [
                      {
                        "literal": "query",
                        "type": {
                          "type": "CHAR",
                          "nullable": false,
                          "precision": 5
                        }
                      },
                      {
                        "literal": "error",
                        "type": {
                          "type": "VARCHAR",
                          "nullable": false,
                          "precision": -1
                        }
                      }
                    ]
                  }
                ],
                "class": "org.opensearch.sql.expression.function.UserDefinedFunctionBuilder$1",
                "type": {
                  "type": "BOOLEAN",
                  "nullable": false
                },
                "deterministic": true,
                "dynamic": false
              }
            },
            {
              "id": "2",
              "relOp": "LogicalProject",
              "fields": [
                "message"
              ],
              "exprs": [
                {
                  "input": 6,
                  "name": "$6"
                }
              ]
            }
          ]
        }\
        """,
        toRelJson(plan));
  }

  private String toRelJson(RelNode plan) {
    RelJsonWriter writer = new RelJsonWriter();
    plan.explain(writer);
    return writer.asString();
  }
}
