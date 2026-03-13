# PoC Plan: Option B (Unified Query Pipeline)

**Goal**: Validate the end-to-end flow of PPL and SQL queries against a Parquet-backed index through the SQL/PPL plugin, with RelNode handoff to the analytics-plugin.

## References

- **Analytics Engine PoC**: https://github.com/mch2/OpenSearch/tree/planning-poc/sandbox
- **ANSI SQL on Calcite PoC**: https://github.com/dai-chen/sql-1/tree/poc/unified-sql-support
  - Adds `ANSI_SQL` query type using Calcite's native `SqlParser` → `SqlValidator` → `SqlToRelConverter` → `RelNode`
  - Registers `match_phrase` as custom `SqlBasicFunction` in Calcite operator table
  - Known issue: `EXPR_TIMESTAMP` UDT from `OpenSearchTypeFactory` is incompatible with Calcite's `SqlTypeFactoryImpl` — directly relevant to Phase 4/5 datetime UDT concern
  - Only `match_phrase` registered; other search functions (`match`, `query_string`, etc.) not yet handled
  - Useful reference for Phase 1: shows how to integrate the unified query API with the existing SQL/PPL REST endpoints (`RestUnifiedSQLQueryAction` wiring, thread pool, response formatting)

## Assumptions

- No real DataFusion execution needed — mock/stub returning hardcoded rows is sufficient.
- No real Parquet data needed — mock schema with hardcoded results.
- RelNode serialization format for transport action is TBD — use Java serialization or JSON for PoC.
- The PoC targets PPL V3 Calcite path only (not V2 fallback).

## Module Placement

| New code | Module | Rationale |
|---|---|---|
| Routing logic | `plugin/` or `legacy/` (where `RestSqlAction` lives) | Extends existing REST handlers |
| Analytics engine SPI interfaces (copied from PoC) | New `analytics-engine-stub/` module | Clear boundary — these are external dependencies, not SQL/PPL plugin code. Will be replaced by real analytics-plugin dependency later. |
| Mock Schema adapter, EngineCapabilities, absorption rules | New `analytics-engine-stub/` module | Mock implementations live alongside the SPI interfaces they implement. Keeps `api/` clean. |
| Stub analytics-plugin transport action | Test plugin in `integ-test/` | Only needed for IT, not shipped |
| Integration tests | `integ-test/` | End-to-end REST round-trip tests |
| Unit tests | `core/`, `api/`, `analytics-engine-stub/` per module | Schema, EngineCapabilities, RelNode generation |

## Test Strategy

- **Unit tests**: In each module (`core/`, `api/`, `analytics-engine-stub/`) for Schema, EngineCapabilities, RelNode generation, absorption rules.
- **Integration tests**: In `integ-test/` for end-to-end REST round-trips. All test queries in the table below should be covered as ITs.
- **Phase 2 establishes the IT scaffold first** (routing + stub response + explain), then subsequent phases replace stubs with real logic. ITs should pass at every phase.

## Prerequisites: Analytics Engine PoC Dependencies

The PoC requires SPI interfaces and schema code from the analytics engine PoC branch. These must be copied into a new `analytics-engine-stub/` Gradle submodule in this repository. This module serves as a clear boundary — everything in it represents external dependencies from the analytics-plugin side and will be replaced by the real analytics-plugin dependency when available.

**Source**: https://github.com/mch2/OpenSearch/tree/planning-poc/sandbox

**From `libs/analytics-framework`** (SPI interfaces):
- `EngineCapabilities.java` — operator/function support checker
- `QueryPlanExecutor.java` — execution entry point (takes RelNode, returns rows)
- `SchemaProvider.java` — builds Calcite SchemaPlus from cluster state
- `EngineBridge.java` — JNI boundary interface for native execution
- `AnalyticsBackEndPlugin.java` — backend plugin SPI

**From `plugins/analytics-engine`** (hub plugin):
- `DefaultPlanExecutor.java` — stub executor (returns empty results, to be replaced)
- `OpenSearchSchemaBuilder.java` — ClusterState → Calcite SchemaPlus with type mappings
- `AnalyticsPlugin.java` — hub plugin that discovers extensions via Guice

> **Note**: All files verified present on `planning-poc` branch as of 2026-03-13. `DefaultPlanExecutor` is still a stub (`returns new ArrayList<>()`). `DataFusionBridge` is also a stub. The PoC will need to provide mock implementations for execution.

## Phases

### Phase 1: Query Routing (A-1, A-2, A-4)

**Work**:
- Add routing logic in the PPL/SQL REST handlers to extract the target index name.
- Resolve the index setting to determine Lucene vs Parquet. (Use a hardcoded flag or mock index setting if test data not yet available.)
- For Lucene indices, fall through to existing pipeline. For Parquet, enter the new path.

**Done when**: A PPL/SQL query targeting a Parquet index is routed to the new path (returns a placeholder error or empty response). Lucene queries are unaffected.

### Phase 2: Response Formatting & Explain (A-5, M-6)

**Work**:
- Wire the Parquet path end-to-end with a stub that returns hardcoded schema + rows.
- Convert the response using the existing response formatter (JDBC format as default).
- Support basic explain API (`_explain` endpoint) for the Parquet path.
- Add integration tests in `integ-test/` covering the full REST round-trip (query, explain, error cases). This establishes the IT scaffold that subsequent phases build on.

**Done when**: `POST _plugins/_ppl` and `POST _plugins/_sql` return a valid JDBC-formatted response for a Parquet index query. `_explain` returns a placeholder plan. ITs pass.

### Phase 3: Mock Parquet Schema Adapter (M-3, M-4)

**Work**:
- In the `analytics-engine-stub/` module, implement a mock Calcite `Schema` for a test Parquet index with representative column types (string, long, timestamp, boolean).
- Validate Parquet-to-SQL type mapping through the schema — use standard Calcite `SqlTypeName` (e.g., `TIMESTAMP`) without OpenSearch UDTs.
- Implement `EngineCapabilities` declaring supported operators/functions.
- Implement absorption rules (filter, project, sort, aggregate pushdown) gated by `EngineCapabilities`.

**Done when**: Mock Schema can be registered in a Calcite `FrameworkConfig` and used to plan a simple query. Unit tests validate type mapping and EngineCapabilities.

### Phase 4: PPL RelNode Generation (B-1, U-1, U-3, U-4)

**Work**:
- Use the existing unified query API to generate a Calcite `RelNode` from PPL V3 queries.
- Wire in the mock Schema from Phase 3.
- Apply logical optimization with absorption rules.
- Verify that unsupported operations are rejected based on `EngineCapabilities` (fail-fast behavior).
- Verify that PPL V3's custom datetime UDT overrides do not interfere when the schema uses standard Calcite types (PPL V3 overwrites all datetime functions with UDT-based implementations; Parquet schema must work with native Calcite types).

**Done when**: PPL queries generate correct RelNode with absorption rules applied. Unsupported operations fail fast. Datetime functions work without UDT interference. Phase 2 ITs updated to use real RelNode generation and still pass.

### Phase 5: SQL RelNode Generation (U-2, U-3, U-4)

**Work**:
- Validate ANSI SQL → RelNode generation using Calcite's SQL parser.
- Reuse the same Schema and EngineCapabilities from Phase 3.
- Verify full-text search functions (`match`, `match_phrase`, `query_string`) are handled correctly in absorption rules.
- Verify datetime functions work correctly with standard Calcite types (same UDT concern as Phase 4).

**Done when**: SQL queries generate correct RelNode. Search functions are absorbed or fail-fast as expected. SQL ITs added to `integ-test/` and pass.

### Phase 6: RelNode Handoff & Execution (B-2, B-3, M-5)

**Work**:
- Implement a transport action in a stub analytics-plugin (test plugin in `integ-test/`) that accepts a serialized `RelNode`.
- SQL/PPL plugin hands off the optimized `RelNode` via this transport action.
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
- Response format matches existing Lucene query responses.
- Unsupported operations produce clear error messages (fail-fast validated).
- Explain API returns meaningful output for Parquet queries.
- Datetime functions work correctly with standard Calcite types (no UDT interference).
- All test queries pass as ITs in `integ-test/`.

## Dependencies on Analytics Engine Team

- Mock or real Parquet-backed index with test data (or agreement on hardcoded mock).
- Transport action interface definition (B-3): what serialization format for RelNode?
- `EngineCapabilities` baseline: which operators/functions are supported in the initial release?
