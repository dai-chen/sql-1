# Unified Query API — PPL Gap Analysis Report

**Date:** 2026-03-24  
**Branch:** `poc/gap-analysis-on-formal` (based on `feature/unified-calcite-sql-support`)  
**Scope:** All PPL IT tests (`org.opensearch.sql.ppl.*IT`) — 1593 queries  
**Pipeline:** `UnifiedQueryPlanner` → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`

---

## Approach

The gap analyzer intercepts every query in the PPL integration test base class (`PPLIntegTestCase.executeQuery()`) and replays it through the unified query pipeline in shadow mode — after the normal REST call succeeds, the same query is planned, compiled, and executed via `UnifiedQueryPlanner` + `UnifiedQueryCompiler`. Failures are collected without affecting the original test. Results are categorized by failure phase (PLAN/COMPILE/EXECUTE) and grouped by error message.

Toggle: `-Dunified.gap.analysis=true`

---

## Summary

| Metric | Value |
|--------|-------|
| Total Queries | 1593 |
| Success | 1566 (98.3%) |
| Failed | 27 (1.7%) |
| Failure Phase | 100% PLAN |

---

## Category 1: Special Index Names with Dots/Hyphens (2 queries)

**Error:** `SyntaxCheckException: [logs-2021.01.11] is not a valid term`  
**Root Cause:** Index names containing dots and hyphens are not recognized as valid identifiers by the unified PPL parser.

| Test Method | Sample Query |
|-------------|-------------|
| `testSearchCommandWithSpecialIndexName` | `search source=logs-2021.01.11` |
| `testSearchCommandWithSpecialIndexName` | `search source=logs-7.10.0-2021.01.11` |

---

## Category 2: Newline in Multi-Line Commands (2 queries)

**Error:** `SyntaxCheckException: [n] is not a valid term`  
**Root Cause:** Test queries contain literal `\n` characters (JSON-escaped newlines) that the parser receives as `\n` instead of actual newlines.

| Test Method | Sample Query |
|-------------|-------------|
| `CommentIT.testMultipleLinesCommand` | `source=...account \n\| fields firstname \n\| where firstname='Amber'` |
| `CommentIT.testMultipleLinesCommand` | Similar multi-line query |

---

## Category 3: Unsupported Calcite Features (3 queries)

**Error:** Various `CalciteUnsupportedException`  
**Root Cause:** Features explicitly guarded as not yet implemented.

| Feature | Test | Sample Query |
|---------|------|--------------|
| Table function | `InformationSchemaCommandIT` | `source=my_prometheus.information_schema.tables` |
| SHOW DATASOURCES | `InformationSchemaCommandIT` | `SHOW DATASOURCES` |
| Consecutive dedup | `DedupCommandIT` | `... \| dedup 1 ... \| dedup 1 ...` |

---

## Category 4: Unregistered Function (1 query)

**Error:** `Cannot resolve function: GEOIP`

| Test Method | Sample Query |
|-------------|-------------|
| `GeoIpCommandIT` | `... \| geoip ip_address` |

---

## Category 5: Prometheus/External Datasource (5 queries)

**Error:** `Table 'my_prometheus...' not found`  
**Root Cause:** Prometheus datasource tables not available in the gap analyzer's schema (only OpenSearch indices are registered).

| Test | Sample Query |
|------|--------------|
| `InformationSchemaCommandIT` | `source=my_prometheus.prometheus_http_requests_total` |
| `PrometheusDataSourceCommandsIT` | Various prometheus queries |

---

## Category 6: Index Not Found (3 queries)

**Error:** `index_not_found_exception: no such index [logs*]`  
**Root Cause:** Wildcard index patterns or indices that don't exist in the test cluster.

| Test | Sample Query |
|------|--------------|
| `SearchCommandIT` | `search source=logs-2021.01.11` (after catalog prefix) |
| Various | Queries referencing non-existent indices |

---

## Category 7: Null Statement (1 query)

**Error:** `Cannot invoke "Object.getClass()" because "statement" is null`  
**Root Cause:** Query that produces a null compiled statement — needs investigation.

---

## Priority Matrix

| Priority | Category | Queries | Fix Complexity |
|----------|----------|---------|----------------|
| **P1** | Special index names | 2 | Low — grammar fix |
| **P2** | Newline escaping | 2 | Low — gap analyzer fix |
| **P2** | Unsupported Calcite features | 3 | Medium |
| **P3** | GEOIP function | 1 | Low — register function |
| **N/A** | Prometheus datasource | 5 | N/A — external datasource |
| **N/A** | Index not found | 3 | N/A — test data issue |
| **P2** | Null statement | 1 | Needs investigation |

---

## How to Run

```bash
# All PPL IT tests
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.ppl.*IT' -Dunified.gap.analysis=true

# Specific test class
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.ppl.SearchCommandIT' -Dunified.gap.analysis=true

# Report is printed to stderr at JVM shutdown
```
