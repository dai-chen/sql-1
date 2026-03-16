# PoC Plan: Option B (Unified Query Pipeline)

**Goal**: Validate the end-to-end flow of PPL and SQL queries against a Parquet-backed index through the SQL/PPL plugin, with RelNode handoff to the analytics-plugin.

## Purpose

This PoC validates the feasibility of integrating the SQL/PPL plugin with the Analytics engine for Parquet-backed index queries. Beyond the end-to-end flow, it focuses on two key investigation areas:

### Key Area 1: UDT/UDF Impact on RelNode Processing in Analytics Engine

PPL V3 introduces custom User-Defined Types (UDTs) — notably `EXPR_TIMESTAMP` from `OpenSearchTypeFactory` — and User-Defined Functions (UDFs) that depend on these UDTs. These override Calcite's standard datetime handling. The PoC must determine whether these UDT/UDF overrides interfere with RelNode processing in the Analytics engine, which expects standard Calcite types. Specifically:
- Does the Analytics engine's `OpenSearchSchemaBuilder` produce schemas compatible with PPL V3's UDT-based type system?
- Can RelNode trees containing UDT-typed expressions be correctly processed by the Analytics engine's `QueryPlanExecutor`?
- Do UDF implementations that depend on UDTs (e.g., datetime functions) produce valid RelNode subtrees that survive optimization and execution in the Analytics engine?

### Key Area 2: RelNode (De-)Serialization via Calcite JSON Serializer

The handoff between the SQL/PPL plugin and the Analytics engine requires serializing `RelNode` across a transport action boundary. The PoC must determine whether Calcite's built-in JSON serializer (`RelJsonWriter` / `RelJsonReader`) can handle this reliably:
- Can the full range of RelNode types produced by PPL V3 and ANSI SQL paths be round-tripped through JSON serialization?
- Are custom operators, UDFs, and UDTs preserved correctly through serialization/deserialization?
- What are the limitations (e.g., custom `RexNode` types, non-standard `RelTraitSet` entries) and do they require custom serialization extensions?

## References

- **Analytics Engine (OpenSearch repo)**: https://github.com/opensearch-project/OpenSearch/tree/main/sandbox/plugins
  - `analytics-engine/` — Hub plugin implementing `ExtensiblePlugin`, discovers and wires extensions via Guice
  - `analytics-backend-datafusion/` — DataFusion native execution backend (stub)
  - `sandbox/libs/analytics-framework/` — Shared SPI library with interfaces: `QueryPlanExecutor`, `SchemaProvider`, `EngineBridge`, `AnalyticsBackEndPlugin`, `AnalyticsFrontEndPlugin`
- **ANSI SQL on Calcite PoC**: https://github.com/dai-chen/sql-1/tree/poc/unified-sql-support
  - Adds `ANSI_SQL` query type using Calcite's native `SqlParser` → `SqlValidator` → `SqlToRelConverter` → `RelNode`
  - Registers `match_phrase` as custom `SqlBasicFunction` in Calcite operator table
  - Known issue: `EXPR_TIMESTAMP` UDT from `OpenSearchTypeFactory` is incompatible with Calcite's `SqlTypeFactoryImpl` — directly relevant to Key Area 1
  - Only `match_phrase` registered; other search functions (`match`, `query_string`, etc.) not yet handled
  - Useful reference for REST handler integration: `RestUnifiedSQLQueryAction` wiring, thread pool, response formatting

## Assumptions

- No real DataFusion execution needed — mock/stub returning hardcoded rows is sufficient.
- No real Parquet data needed — mock schema with hardcoded results.
- RelNode serialization format for transport action is TBD — investigate Calcite JSON serializer first, fall back to Java serialization if needed.
- The PoC targets PPL V3 Calcite path only (not V2 fallback).
- The Analytics engine code in `sandbox/plugins` of the OpenSearch repo is available. The PoC will attempt to depend on it directly via published artifacts or Gradle composite build; if not feasible, copy the required SPI interfaces from `analytics-framework`.

## Module Placement

| New code | Module | Rationale |
|---|---|---|
| Routing logic + REST handler | `plugin/` or `legacy/` (where `RestSqlAction` lives) | Extends existing REST handlers. Copied from `RestUnifiedSQLQueryAction` and refactored to reuse existing default response formatter. |
| Analytics engine SPI interfaces | Depend on `analytics-framework` directly, or copy into new `analytics-engine-stub/` module | Prefer direct dependency. Copy only if cross-repo dependency is not feasible for PoC. |
| Mock Schema adapter, EngineCapabilities, absorption rules | New `analytics-engine-stub/` module (if copying) or test fixtures | Clear boundary — these are external dependencies. |
| Stub analytics-plugin transport action | Test plugin in `integ-test/` | Only needed for IT, not shipped |
| Integration tests | `integ-test/` | End-to-end REST round-trip tests |
| Unit tests | `core/`, `api/`, per module | Schema, EngineCapabilities, RelNode generation |

## Test Strategy

- **Unit tests**: In each module (`core/`, `api/`) for Schema, EngineCapabilities, RelNode generation, absorption rules.
- **Integration tests**: In `integ-test/` for end-to-end REST round-trips. All test queries in the table below should be covered as ITs.
- **Phase 2 establishes the IT scaffold first** (ANSI SQL RelNode + explain), then subsequent phases add PPL and routing. ITs should pass at every phase.

## Prerequisites: Analytics Engine Dependencies

The Analytics engine is already available in the OpenSearch repo under `sandbox/plugins/`. The SPI interfaces live in `sandbox/libs/analytics-framework/` (Calcite 1.41.0).

**Preferred approach**: Depend on `analytics-framework` directly as a Gradle dependency (if published as a Maven artifact or via composite build).

**Fallback approach**: Copy the SPI interfaces into a local `analytics-engine-stub/` module. Required interfaces from `analytics-framework`:
- `EngineCapabilities.java` — operator/function support checker
- `QueryPlanExecutor.java` — execution entry point (takes RelNode, returns rows)
- `SchemaProvider.java` — builds Calcite SchemaPlus from cluster state
- `EngineBridge.java` — JNI boundary interface for native execution
- `AnalyticsBackEndPlugin.java` — backend plugin SPI

From `analytics-engine` plugin (for reference/mock):
- `OpenSearchSchemaBuilder.java` — ClusterState → Calcite SchemaPlus with type mappings
- `AnalyticsPlugin.java` — hub plugin that discovers extensions via Guice

## Phases

### Phase 1: ANSI SQL RelNode Generation (U-2, U-3, U-4)

**Work**:
- Copy `UnifiedQueryPlanner.java` from the [ANSI SQL PoC](https://github.com/dai-chen/sql-1/blob/poc/unified-sql-support/api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java) into the `api/` module.
- Validate ANSI SQL → RelNode generation using Calcite's native SQL parser (`SqlParser` → `SqlValidator` → `SqlToRelConverter` → `RelNode`).
- Wire in a mock Schema (hardcoded Parquet index with representative column types).
- Verify full-text search functions (`match`, `match_phrase`, `query_string`) are handled correctly in absorption rules.
- Verify datetime functions work correctly with standard Calcite types (Key Area 1: UDT concern).
- Apply `AGGREGATE_CASE_TO_FILTER` HepPlanner rule for filter aggregation pushdown.

**Done when**: SQL queries generate correct RelNode. Search functions are absorbed or fail-fast as expected. Unit tests validate RelNode generation.

### Phase 2: PPL RelNode Generation (B-1, U-1, U-3, U-4)

**Work**:
- Use the existing PPL V3 Calcite path to generate `RelNode` from PPL queries.
- Wire in the same mock Schema from Phase 1.
- Apply logical optimization with absorption rules.
- Verify that unsupported operations are rejected based on `EngineCapabilities` (fail-fast behavior).
- Verify that PPL V3's custom datetime UDT overrides do not interfere when the schema uses standard Calcite types (Key Area 1).

**Done when**: PPL queries generate correct RelNode with absorption rules applied. Unsupported operations fail fast. Datetime functions work without UDT interference. Unit tests pass.

### Phase 3: Query Routing & REST Handler (A-1, A-2, A-4, A-5, M-6)

**Work**:
- Copy `RestUnifiedSQLQueryAction.java` from the [ANSI SQL PoC](https://github.com/dai-chen/sql-1/blob/poc/unified-sql-support/legacy/src/main/java/org/opensearch/sql/legacy/plugin/RestUnifiedSQLQueryAction.java) into `plugin/` or `legacy/`.
- Refactor to reuse the existing default response formatter instead of the custom `formatAsJdbc` method — use the existing JDBC response format infrastructure in the `protocol/` module.
- Add routing logic to extract the target index name and resolve Lucene vs Parquet (hardcoded flag or mock index setting).
- For Lucene indices, fall through to existing pipeline. For Parquet, enter the new path using `UnifiedQueryPlanner`.
- Wire the Parquet path end-to-end with a stub that returns hardcoded schema + rows.
- Support basic explain API (`_explain` endpoint) for the Parquet path.
- Add integration tests in `integ-test/` covering the full REST round-trip (query, explain, error cases).

**Done when**: `POST _plugins/_ppl` and `POST _plugins/_sql` return a valid JDBC-formatted response for a Parquet index query using the existing response formatter. `_explain` returns a plan. Lucene queries are unaffected. ITs pass.

### Phase 4: Analytics Engine Integration (M-3, M-4)

**Work**:
- Determine dependency approach: depend on `analytics-framework` directly from the OpenSearch repo, or copy SPI interfaces into `analytics-engine-stub/`.
- If direct dependency: add `analytics-framework` as a Gradle dependency and implement `AnalyticsFrontEndPlugin` SPI.
- If copy: copy SPI interfaces and implement mock `SchemaProvider` using `OpenSearchSchemaBuilder` as reference.
- Implement `EngineCapabilities` declaring supported operators/functions.
- Implement absorption rules (filter, project, sort, aggregate pushdown) gated by `EngineCapabilities`.
- Replace mock Schema from Phase 1/2 with the Analytics engine's schema infrastructure.

**Done when**: Schema can be registered in a Calcite `FrameworkConfig` and used to plan queries. Unit tests validate type mapping and EngineCapabilities. Phase 3 ITs updated to use real schema and still pass.

### Phase 5: RelNode Serialization Investigation (Key Area 2)

**Work**:
- Test Calcite's built-in JSON serializer (`RelJsonWriter` / `RelJsonReader`) with RelNode trees from Phase 1 and 2.
- Verify round-trip serialization for: simple filter/project, aggregations, sort/limit, UDF calls, UDT-typed expressions.
- Document which RelNode types and operators serialize correctly and which require custom handling.
- If Calcite JSON serializer is insufficient, prototype a minimal custom serialization extension or fall back to Java serialization.

**Done when**: Serialization feasibility documented. Round-trip tests for representative RelNode trees pass or limitations are clearly identified.

### Phase 6: RelNode Handoff & Execution (B-2, B-3, M-5)

**Work**:
- Implement a transport action in a stub analytics-plugin (test plugin in `integ-test/`) that accepts a serialized `RelNode`.
- SQL/PPL plugin hands off the optimized `RelNode` via this transport action.
- Use the serialization approach validated in Phase 5.
- Stub analytics-plugin deserializes and executes (mock execution returning hardcoded rows, or integrate with DataFusion if available).

**Done when**: Full round-trip works: REST → routing → RelNode generation → transport action → stub execution → response formatting. All ITs pass through the transport action path.

### Phase 7 (Optional): Cross-Cutting Concerns (TBD-1, TBD-2, TBD-3)

**Work**:
- **Resource management**: Verify `querySizeLimit` is enforced on the Parquet path. Identify where circuit breaker and timeout should be added (document findings, not full implementation).
- **Monitoring**: Verify existing request/failure metrics capture Parquet queries. Identify gaps.
- **Security**: Verify index-level auth during routing. Document the DLS/FLS gap.

**Done when**: Findings documented. No implementation required — this phase produces a gap analysis for the LLD.

## Test Queries

All test queries should be covered as integration tests in `integ-test/`.

| Query | What it validates |
|---|---|
| `source = parquet_index \| where status = 200 \| fields timestamp, status, message` | Filter + project pushdown |
| `source = parquet_index \| stats count() by status` | Aggregate pushdown |
| `source = parquet_index \| sort timestamp \| head 100` | Sort + limit pushdown |
| `source = parquet_index \| where match(message, 'error')` | Search function — should fail-fast on Parquet |
| `source = parquet_index \| eval new_field = upper(message)` | Function not in EngineCapabilities — test fail-fast vs post-process |
| `source = parquet_index \| eval hour = hour(timestamp) \| where timestamp > '2024-01-01'` | Datetime functions with standard Calcite TIMESTAMP type (no UDT) |
| `source = parquet_index \| stats count() by span(timestamp, 1h)` | Time span aggregation against native Calcite timestamp |
| `SELECT timestamp, status FROM parquet_index WHERE status = 200` | SQL filter + project |
| `SELECT count(*) FROM parquet_index GROUP BY status` | SQL aggregate |
| `SELECT * FROM parquet_index WHERE match(message, 'error')` | SQL search function handling |

## Success Criteria

- PPL and SQL queries against a Parquet index return correct results through `_plugins/_ppl` and `_plugins/_sql` endpoints.
- Lucene queries are unaffected (no regression).
- Response format matches existing Lucene query responses (reuses existing default response formatter).
- Unsupported operations produce clear error messages (fail-fast validated).
- Explain API returns meaningful output for Parquet queries.
- Datetime functions work correctly with standard Calcite types (Key Area 1: no UDT interference confirmed).
- RelNode serialization feasibility determined (Key Area 2: Calcite JSON serializer tested).
- All test queries pass as ITs in `integ-test/`.

## Dependencies on Analytics Engine Team

- Transport action interface definition (B-3): what serialization format for RelNode?
- `EngineCapabilities` baseline: which operators/functions are supported in the initial release?
- `analytics-framework` artifact availability: is it published to Maven for cross-repo dependency, or do we need composite build / copy?
