# Unified Query API Gap Analysis — Design

## Goal

Verify that every query executed by SQL and PPL integration tests in `integ-test/` can be
planned, compiled, and executed through the `UnifiedQueryPlanner` + `UnifiedQueryCompiler`
pipeline (api module). Surface all failures as a categorized gap report grouped by error message.

## Approach

Intercept `executeQuery` in `PPLIntegTestCase` and `executeJdbcRequest`/`executeQuery` in
`SQLIntegTestCase`. After the normal REST call succeeds, replay the same query through the
unified pipeline in **shadow mode**. Log results without failing the original test.

## Components

### 1. `UnifiedQueryGapAnalyzer` (new)

Location: `integ-test/src/test/java/org/opensearch/sql/util/UnifiedQueryGapAnalyzer.java`

Responsibilities:
- Lazily initialize `UnifiedQueryContext` with `OpenSearchIndex`-backed schema
- Provide `tryUnifiedExecution(RestClient, query, queryType)` → `GapResult`
- Track failure phase: PLAN / COMPILE / EXECUTE
- Unwrap root cause from exception chain for accurate error reporting

### 2. `GapReportCollector` (new)

Location: `integ-test/src/test/java/org/opensearch/sql/util/GapReportCollector.java`

Responsibilities:
- Thread-safe collection of gap results across all test methods (`ConcurrentLinkedQueue`)
- Group failures by phase + error message
- Print categorized report on JVM shutdown (stderr + file)
- Separate PPL and SQL sections with success/failure counts and percentages

### 3. `PPLIntegTestCase` modification

In `executeQuery(String)`: after REST call succeeds, call gap analyzer with `QueryType.PPL`.
Transform query to add catalog prefix (`source=idx` → `source=opensearch.idx`).

### 4. `SQLIntegTestCase` modification

In `executeJdbcRequest(String)` and `executeQuery(String sqlQuery)`: after REST call succeeds,
call gap analyzer with `QueryType.SQL`. Use `defaultNamespace("opensearch")` so unqualified
table names resolve.

### 5. Query transformations

Before replaying through the unified pipeline, queries are transformed:

- **PPL:** Regex rewrite `source=<index>` to `source=opensearch.<index>` (also handles `lookup` and `join` index references)
- **SQL:** Quote hyphenated table names in FROM/JOIN clauses (e.g., `FROM opensearch-sql_test_index_account` → `FROM "opensearch-sql_test_index_account"`) so Calcite doesn't parse `-` as minus
- **Both:** Unescape JSON encoding — test queries are pre-escaped for the REST `buildRequest()` JSON template (`\"` → `"`), but the unified pipeline bypasses the JSON round-trip

### 6. Gap report format

```
================================================================================
  UNIFIED QUERY GAP ANALYSIS REPORT
================================================================================

--- PPL: N total, N success (N%), N failed (N%) ---

  [PLAN] error message (N occurrences)
  Exception: exception.class.name
    - TestClass.testMethod: query text
    ...

--- SQL: N total, N success (N%), N failed (N%) ---

  [PLAN] error message (N occurrences)
  ...

  [EXECUTE] error message (N occurrences)
  ...

================================================================================
```

### 7. Toggle

System property: `-Dunified.gap.analysis=true` (default: false)

Must be forwarded from Gradle to the test JVM in `integ-test/build.gradle`:
```groovy
if (System.getProperty('unified.gap.analysis') != null) {
  systemProperty 'unified.gap.analysis', System.getProperty('unified.gap.analysis')
}
```

## Scope

- All PPL IT tests in `integ-test/src/test/java/org/opensearch/sql/ppl/`
- All SQL V2 IT tests in `integ-test/src/test/java/org/opensearch/sql/sql/`
- All Legacy SQL IT tests in `integ-test/src/test/java/org/opensearch/sql/legacy/`
- Does NOT compare results between REST and unified paths
- Does NOT fail tests when unified path fails
- Does NOT handle explain/cursor/CSV-only queries

## Results

- **PPL:** 1593 queries, 98.3% success → [PPL Gap Analysis Report](2026-03-24-ppl-gap-analysis-report.md)
- **SQL:** 807 queries, 71.3% success → [SQL Gap Analysis Report](2026-03-24-sql-gap-analysis-report.md)
