# Unified Query API — SQL Gap Analysis Report

**Date:** 2026-03-25
**Branch:** `poc/gap-analysis-on-formal` (based on `feature/unified-calcite-sql-support`)
**Scope:** All SQL IT tests — V2 (`org.opensearch.sql.sql.*IT`, 50 classes) + Legacy (`org.opensearch.sql.legacy.*IT`, 39 classes) — 807 queries
**Pipeline:** `UnifiedQueryPlanner` (Calcite native SQL parser) → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`
**Previous Report:** [2026-03-24](./2026-03-24-sql-gap-analysis-report.md)

---

## What Changed Since Mar 24

> The Mar 24 report only detected **exception-based failures** (PLAN/COMPILE/EXECUTE phases).
> Queries that executed successfully in both V2 and Calcite but returned **different data** were
> counted as "success". This report adds a new failure category — **RESULT_MISMATCH** — that
> compares actual query results between the two pipelines.

| Metric | Mar 24 | Mar 25 | Delta |
|--------|--------|--------|-------|
| Total Queries | 807 | 807 | — |
| Success | 575 (71.3%) | 548 (67.9%) | **−27** |
| Failed (Exception) | 232 (28.7%) | 232 (28.7%) | — |
| Failed (Different Result) | *not measured* | 27 (3.3%) | **+27 (NEW)** |
| Total Failed | 232 (28.7%) | 259 (32.1%) | +27 |

**Key insight:** 27 queries that were previously counted as "success" actually return different results between V2 and Calcite. The exception-based failure categories (Categories 1–9) are unchanged.

### Implementation Change

The gap analyzer now captures the V2 REST response and compares it against the Calcite `ResultSet` after successful execution:
- Parses V2 JDBC `datarows` array
- Extracts Calcite ResultSet into comparable row lists
- Normalizes values (float→2 decimal places, integer→long, null→"NULL")
- Sorts rows for non-ORDER BY queries (order-independent comparison)
- Reports mismatches as `RESULT_MISMATCH` phase with diff description

Modified files:
- `UnifiedQueryGapAnalyzer.java` — added result comparison logic, new `RESULT_MISMATCH` phase
- `GapReportCollector.java` — updated report formatting for result mismatch entries
- `SQLIntegTestCase.java` — passes V2 response to replay method
- `PPLIntegTestCase.java` — passes V2 response to replay method

---

## Summary

| Metric | Value |
|--------|-------|
| Total Queries | 807 |
| Success | 548 (67.9%) |
| Failed (Exception) | 232 (28.7%) |
| Failed (Different Result) | 27 (3.3%) |
| Total Failed | 259 (32.1%) |

---

## Categories 1–9: Exception-Based Failures (232 queries) — UNCHANGED

These categories are identical to the [Mar 24 report](./2026-03-24-sql-gap-analysis-report.md). Summary:

| # | Category | Queries | Phase |
|---|----------|---------|-------|
| 1 | Missing Calcite Functions | 86 | PLAN |
| 2 | EXPR_TIMESTAMP Type Conversion | 21 | PLAN |
| 3 | Comma-Join / Nested Table Syntax | 21 | PLAN |
| 4 | String Literal Treated as Identifier | 16 | PLAN |
| 5 | Backtick Identifiers | 8 | PLAN |
| 6 | GROUP BY Expression Issues | 7 | PLAN |
| 7 | Unknown Field * / Wildcard Issues | 6 | PLAN |
| 8 | Index Not Found | 5 | PLAN |
| 9 | Other | 62 | PLAN |

---

## Category 10: Different Query Results — NEW (27 queries)

**Phase:** `RESULT_MISMATCH`
**Root Cause:** Both V2 and Calcite execute the query successfully, but return different data.

### 10a. SELECT * Returns Extra Metadata Columns (9 queries)

**Diff Pattern:** Calcite returns additional columns that V2 strips from `SELECT *` results.
**Root Cause:** Calcite's schema includes OpenSearch metadata fields (`_id`, `_index`, `_routing`, `_score`, `_seq_no`, `_primary_term`); V2 filters them out.

| Sample Query | Diff |
|-------------|------|
| `SELECT * FROM opensearch-sql_test_index_account` | V2=11 cols, Calcite=17 cols |
| `SELECT * FROM opensearch-sql_test_index_wildcard WHERE TextBody LIKE 'test%'` | Same pattern |

**Affected Tests:** PrettyFormatResponseIT.selectAll, PaginationBlackboxIT (×4), PaginationFilterIT (×2), LikeQueryIT (×2)

### 10b. JOIN Row Count Explosion (3 queries)

**Diff Pattern:** Calcite returns significantly more rows than V2 for JOIN queries.
**Root Cause:** V2 applies a default `LIMIT 200` to JOIN results; Calcite returns the full result set.

| Sample Query | V2 Rows | Calcite Rows |
|-------------|---------|-------------|
| `SELECT b1.balance, b1.age, b2.firstname FROM ... b1 JOIN ... b2 ON b1.age = b2.age` | 200 | 48,626 |
| `SELECT b1.age FROM ... b1 JOIN ... b2 ON b1.firstname = b2.firstname` | 200 | 1,014 |

### 10c. V2 Column Truncation in JDBC Format (6 queries)

**Diff Pattern:** V2 returns fewer columns than Calcite for queries with `ORDER BY` on columns not in SELECT.
**Root Cause:** V2's JDBC formatter strips the ORDER BY column from the result when it's not in the SELECT list. Calcite includes it.

| Sample Query | V2 Row | Calcite Row |
|-------------|--------|------------|
| `SELECT LOWER(firstname) FROM ... ORDER BY firstname LIMIT 2` | `[abbott]` | `[abbott, Abbott]` |
| `SELECT (balance + 5) AS balance_add_five FROM ... ORDER BY firstname LIMIT 2` | `[11020]` | `[11020, Abbott]` |
| `SELECT ABS(age) FROM ... ORDER BY age LIMIT 5` | `[20]` | `[20, 20]` |
| `SELECT radians(balance) FROM ... ORDER BY firstname LIMIT 2` | `[192]` | `[192.25, Abbott]` |
| `SELECT sin(balance) FROM ... ORDER BY firstname LIMIT 2` | `[0]` | `[0.54, Abbott]` |
| `SELECT CEIL(balance) FROM ... ORDER BY balance LIMIT 5` | `[1011]` | `[1011, 1011]` |

### 10d. Numeric Precision Differences (1 query)

**Diff Pattern:** V2 truncates decimal values; Calcite preserves full precision.
**Root Cause:** V2's JDBC response rounds `PI()` to integer `3`; Calcite returns `3.14`.

| Sample Query | V2 | Calcite |
|-------------|----|---------| 
| `SELECT PI() FROM ... LIMIT 1` | `3` | `3.14` |

### 10e. Geo-Point Format Differences (2 queries)

**Diff Pattern:** V2 returns geo-points as JSON objects; Calcite returns WKT format.
**Root Cause:** Different serialization of `geo_point` type.

| Sample Query | V2 | Calcite |
|-------------|----|---------| 
| `SELECT point FROM opensearch-sql_test_index_geopoint LIMIT 5` | `{"lon":74,"lat":40.71}` | `POINT (74 40.71)` |

### 10f. LIKE Escape Behavior Differences (6 queries)

**Diff Pattern:** V2 and Calcite handle escaped wildcards (`\%`, `\_`) differently in LIKE expressions.
**Root Cause:** V2 treats `\%` and `\_` as literal `%` and `_` characters. Calcite's LIKE doesn't recognize backslash escape by default (requires explicit `ESCAPE` clause).

| Sample Query | V2 | Calcite |
|-------------|----|---------| 
| `WHERE KeywordBody LIKE '\%test wildcard%'` | 1 row | 0 rows |
| `WHERE KeywordBody LIKE '\_test wildcard%'` | 1 row | 0 rows |
| `SELECT KeywordBody, KeywordBody LIKE '\%test wildcard%'` | `true` | `false` |
| `SELECT KeywordBody, KeywordBody LIKE '\_test wildcard%'` | `true` | `false` |

---

## Progress Tracking

| Fix Applied | Before | After | Improvement |
|------------|--------|-------|-------------|
| Baseline (Calcite native parser) | 3 / 807 (0.4%) | — | — |
| + Quote hyphenated table names | — | 575 / 807 (71.3%) | +572 queries |
| + Result comparison (this iteration) | 575 / 807 (71.3%) | 548 / 807 (67.9%) | −27 queries reclassified |

---

## Updated Priority Matrix

> Items marked with **△** are new or changed from the Mar 24 report.

| Priority | Category | Queries | Fix |
|----------|----------|---------|-----|
| **P0** | Backtick identifiers | 8 | Configure `Quoting.BACK_TICK` |
| **P0 △** | SELECT * extra metadata columns | 9 | Filter metadata fields from Calcite schema or result |
| **P1** | Missing relevance functions | 67 | Register in Calcite |
| **P1** | EXPR_TIMESTAMP conversion | 21 | Implement type converter |
| **P1** | Missing datetime functions | 32 | Register convert_tz, DATETIME |
| **P1 △** | LIKE escape behavior | 6 | Configure default ESCAPE character or transform LIKE patterns |
| **P2** | Comma-join / nested table | 21 | Implement nested table resolution |
| **P2** | String literal vs identifier | 16 | Parser configuration or transform |
| **P2** | GROUP BY strictness | 7 | Relax validation or transform |
| **P2** | COUNT(*) / wildcard | 6 | Fix wildcard resolution |
| **P2 △** | JOIN row count explosion | 3 | Apply default LIMIT to Calcite JOINs or document as intentional |
| **P2 △** | V2 column truncation (ORDER BY) | 6 | Align column projection behavior |
| **P2 △** | Geo-point format | 2 | Align geo_point serialization |
| **P2 △** | Numeric precision (PI) | 1 | Align numeric formatting |
| **P3** | SHOW/DESCRIBE, reserved words, etc. | 62 | Various |

---

## How to Run

```bash
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.*IT' --tests 'org.opensearch.sql.legacy.*IT' -Dunified.gap.analysis=true
```
