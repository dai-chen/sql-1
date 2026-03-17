# PoC Result: Unified Query Pipeline (Option B)

## Summary

This PoC validates the end-to-end flow of routing SQL and PPL queries against non-Lucene indices (e.g., Parquet-backed) through the SQL/PPL plugin to the Analytics engine for execution. The SQL plugin parses queries using either Calcite's native SQL parser or the existing PPL V3 Calcite path, generates a logical `RelNode` plan against a schema built from cluster state index mappings, and hands the plan off to the Analytics engine's `QueryPlanExecutor` for execution. The integration uses OpenSearch's `extendedPlugins` mechanism — the SQL plugin declares `analytics-engine` as an extended plugin, which shares the Calcite classloader between both plugins, enabling direct in-process `RelNode` passing without serialization. Seven integration tests verify the full round-trip: REST request → query routing → RelNode generation → Analytics engine handoff → JDBC-formatted response, with the real analytics-engine plugin loaded in the test cluster.

## Key Area 1: UDT/UDF Impact on RelNode Processing

PPL V3's custom datetime UDTs (`EXPR_TIMESTAMP`, `EXPR_DATE`, `EXPR_TIME`, backed by VARCHAR) do **not** contaminate the RelNode when the schema uses standard Calcite types. The `OpenSearchSchemaBuilder` from the Analytics engine maps index fields to standard `SqlTypeName` (e.g., `date` → `TIMESTAMP`), bypassing `OpenSearchTypeFactory` entirely. Datetime functions like `hour()` and `day()` resolve correctly via `PPLTypeChecker` and return standard `INTEGER` type. The UDT system only activates when schemas are built through `OpenSearchTypeFactory` — which the Analytics engine path does not use.

## Key Area 2: RelNode Handoff Mechanism

Serialization is **not needed**. Each OpenSearch plugin gets its own `URLClassLoader`, so `RelNode` from one plugin is a different class than in another — unless one plugin extends the other. By declaring `extendedPlugins = ['analytics-engine']`, the SQL plugin inherits the Analytics engine's classloader as parent, sharing the same Calcite classes. The `QueryPlanExecutor.execute(RelNode, context)` call passes the object directly in-process. Calcite's JSON serializer (`RelJsonWriter`/`RelJsonReader`) remains a viable fallback if cross-node execution is needed in the future.

## Key Area 3: Optimization Boundary

The RelNode produced by the SQL plugin is a **pure logical plan** with standard Calcite nodes (e.g., `LogicalProject`, `LogicalFilter`, `LogicalTableScan`). Optimization is split into two layers with a clean boundary:

**SQL plugin (language-specific logical rewrites)**: The SQL plugin applies engine-agnostic HepPlanner rules before handoff. These simplify PPL/SQL-generated patterns without knowledge of the execution engine:
- `PPLAggregateConvertRule` — rewrite `SUM(field+N)` → `SUM(field)+N*COUNT()`
- `PPLAggGroupMergeRule` — merge redundant group-by fields
- `PPLSimplifyDedupRule` — simplify dedup pattern into `LogicalDedup`
- `FilterMergeRule` — merge adjacent filters

Currently `UnifiedQueryPlanner.plan()` does NOT apply these rules. The fix is to call `CalciteToolsHelper.optimize()` before returning the RelNode.

**Analytics engine (engine-specific physical optimization)**: The Analytics engine receives the optimized logical plan and runs its own VolcanoPlanner to produce a physical plan. It replaces `LogicalTableScan` with its own physical scan node (e.g., `DataFusionTableScan`) via a conversion rule — the same pattern as `CalciteLogicalIndexScan` replaces `LogicalTableScan` for the Lucene path. Pushdown rules (filter, project, aggregate, sort) are registered via the scan node's `register(RelOptPlanner)` method.

**Fail-fast for unsupported operations**: The Analytics engine's `QueryPlanExecutor.execute()` throws a clear exception for unsupported operations (e.g., `match()` on Parquet). The SQL plugin catches and returns an error response. This is simpler and more robust than a pre-validation pass, since the engine knows best what it supports.

## Responsibilities

### SQL/PPL Plugin

1. **Query routing**: Detect non-Lucene indices (e.g., by index settings) and route to the unified query pipeline instead of the existing V2/V3 engine
2. **Parsing**: Parse SQL via Calcite's native `SqlParser` → `SqlValidator` → `SqlToRelConverter`, or PPL via ANTLR → `CalciteRelNodeVisitor`, producing a logical `RelNode`
3. **Logical optimization**: Apply engine-agnostic HepPlanner rules (FilterMerge, PPL-specific rewrites) to simplify the logical plan before handoff
4. **Handoff**: Pass the optimized logical `RelNode` to the Analytics engine's `QueryPlanExecutor.execute()`
5. **Response formatting**: Convert `Iterable<Object[]>` results from the Analytics engine into JDBC/CSV/RAW response using existing `JdbcResponseFormatter`
6. **Error handling**: Catch exceptions from the Analytics engine (unsupported operations, execution failures) and return appropriate error responses

### Analytics Engine Plugin

1. **Schema provisioning**: Provide `OpenSearchSchemaBuilder` to build Calcite `SchemaPlus` from cluster state index mappings with standard Calcite types (no UDTs)
2. **Physical optimization**: Replace `LogicalTableScan` with engine-specific physical scan node, register pushdown rules (filter, project, aggregate, sort, limit) via `TableScan.register()`
3. **Execution**: Execute the physical plan via back-end engines (DataFusion, etc.) and return `Iterable<Object[]>` result rows
4. **Fail-fast**: Throw clear exceptions for unsupported operations (e.g., full-text search functions on Parquet)
5. **Classloader hosting**: Bundle Calcite and all transitive runtime dependencies as the classloader parent for extending plugins

## API Contract

### Query Routing Rule

```
if index storage type is non-Lucene (e.g., Parquet):
    → UnifiedQueryPlanner → RelNode → AnalyticsExecutionEngine
else:
    → existing V2/V3 engine (Lucene path, unchanged)
```

Currently detected by naming convention (`parquet_` prefix). In production, this would check index settings or metadata to determine the storage engine.

### SchemaBuilder (Analytics Engine → SQL Plugin)

```java
// Analytics engine provides schema from cluster state
SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterService.state());

// Type mapping: OpenSearch field type → standard Calcite SqlTypeName
//   keyword/text/ip → VARCHAR
//   long → BIGINT, integer → INTEGER, short → SMALLINT, byte → TINYINT
//   double → DOUBLE, float → FLOAT, boolean → BOOLEAN
//   date → TIMESTAMP
//   nested/object → skipped
```

### QueryPlanExecutor (Analytics Engine → SQL Plugin)

```java
// SQL plugin generates RelNode, Analytics engine executes it
QueryPlanExecutor<RelNode, Iterable<Object[]>> executor;
Iterable<Object[]> results = executor.execute(relNode, context);

// Each Object[] is one row, column order matches relNode.getRowType().getFieldList()
```

### Query Result Format (SQL Plugin → Client)

```json
{
  "schema": [
    {"name": "status", "type": "integer"},
    {"name": "message", "type": "keyword"}
  ],
  "datarows": [
    [200, "OK"],
    [500, "Error"]
  ],
  "total": 2,
  "size": 2,
  "status": 200
}
```

Schema column types derived from `RelNode.getRowType()` via `OpenSearchTypeFactory.convertRelDataTypeToExprType()`. Result rows converted from `Iterable<Object[]>` to `List<ExprValue>` (TODO in current PoC — stub returns empty results). Formatted by existing `JdbcResponseFormatter`.
