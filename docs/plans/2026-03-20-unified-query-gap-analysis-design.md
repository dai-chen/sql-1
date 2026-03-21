# Unified Query API Gap Analysis — Design

## Goal

Verify that every query executed by SQL and PPL integration tests in `integ-test/` can be
planned, compiled, and executed through the `UnifiedQueryPlanner` + `UnifiedQueryCompiler`
pipeline (api module). Surface all failures as a categorized gap report grouped by error message.

## Approach

Intercept `executeQuery` in `PPLIntegTestCase` and `executeJdbcRequest`/`executeQuery` in
`SQLIntegTestCase`. After the normal REST call succeeds, replay the same query through the
unified pipeline. Log results without failing the original test.

## Components

### 1. `UnifiedQueryGapAnalyzer` (new)

Location: `integ-test/src/test/java/org/opensearch/sql/util/UnifiedQueryGapAnalyzer.java`

Responsibilities:
- Lazily initialize `UnifiedQueryContext` with `OpenSearchIndex`-backed schema
- Provide `tryUnifiedExecution(query, queryType)` → `GapResult`
- Collect results across all test methods
- Print categorized gap report on shutdown (grouped by error message, split by phase)

### 2. `PPLIntegTestCase` modification

In `executeQuery(String)`: after REST call succeeds, call gap analyzer with `QueryType.PPL`.
Transform query to add catalog prefix (`source=idx` → `source=opensearch.idx`).

### 3. `SQLIntegTestCase` modification

In `executeJdbcRequest(String)` and `executeQuery(String sqlQuery)`: after REST call succeeds,
call gap analyzer with `QueryType.SQL`. Use `defaultNamespace("opensearch")` so unqualified
table names resolve.

### 4. Query transformation

- PPL: regex rewrite `source\s*=\s*` table references to add `opensearch.` prefix
- SQL: rely on `defaultNamespace` in `UnifiedQueryContext`

### 5. Gap report format

```
=== UNIFIED QUERY GAP ANALYSIS (SQL) ===
Total: N | Success: N (%) | Failed: N (%)

--- FAILURES BY ERROR ---

[PLAN] ErrorClass: message (N queries)
  - TestClass.method: query text (truncated)
  ...

[COMPILE] ErrorClass: message (N queries)
  ...

[EXECUTE] ErrorClass: message (N queries)
  ...
```

### 6. Toggle

System property: `-Dunified.gap.analysis=true` (default: false)

## Scope

- All SQL IT tests in `integ-test/src/test/java/org/opensearch/sql/sql/`
- All PPL IT tests in `integ-test/src/test/java/org/opensearch/sql/ppl/`
- Does NOT compare results between REST and unified paths
- Does NOT fail tests when unified path fails
- Does NOT handle explain/cursor/CSV-only queries
