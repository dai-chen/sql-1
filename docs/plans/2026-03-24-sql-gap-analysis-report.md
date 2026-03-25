# Unified Query API — SQL Gap Analysis Report

**Date:** 2026-03-25  
**Branch:** `poc/gap-analysis-on-formal` (based on `feature/unified-calcite-sql-support`)  
**Scope:** All SQL IT tests — V2 (`org.opensearch.sql.sql.*IT`, 50 classes) + Legacy (`org.opensearch.sql.legacy.*IT`, 39 classes) — 807 queries  
**Pipeline:** `UnifiedQueryPlanner` (Calcite native SQL parser) → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`

---

## Approach

The gap analyzer intercepts every query in the SQL integration test base classes (`SQLIntegTestCase.executeJdbcRequest()` and `executeQuery()`) and replays it through the unified query pipeline in shadow mode — after the normal V2/legacy REST call succeeds, the same query is planned, compiled, and executed via `UnifiedQueryPlanner` + `UnifiedQueryCompiler`. Failures are collected without affecting the original test.

The gap analyzer applies two transformations before replay:
1. **JSON unescape** — test queries are pre-escaped for the REST JSON template; the gap analyzer strips this
2. **Quote hyphenated table names** — wraps names like `opensearch-sql_test_index_account` in double quotes so Calcite doesn't parse `-` as minus

Toggle: `-Dunified.gap.analysis=true`

---

## Summary

| Metric | Value |
|--------|-------|
| Total Queries | 807 |
| Success | 575 (71.3%) |
| Failed | 232 (28.7%) |
| Failure Phase | ~100% PLAN |

---

## Category 1: Missing Calcite Functions (86 queries)

**Error:** `No match found for function signature <name>(...)`  
**Root Cause:** OpenSearch-specific functions not registered in Calcite's function registry.

| Sub-Category | Count | Sample Query |
|-------------|-------|--------------|
| Relevance: `match()` | 4 | `WHERE match(lastname, 'Bates')` |
| Relevance: `match_query()` / `matchquery()` | 10 | `WHERE match_query(lastname, 'Bates')` |
| Relevance: `match_phrase()` / `matchphrase()` | 10 | `WHERE match_phrase(phrase, 'quick fox')` |
| Relevance: `matchphrasequery()` | 2 | `WHERE matchphrasequery(phrase, 'quick fox')` |
| Relevance: `match_phrase_prefix()` | 8 | `WHERE match_phrase_prefix(Title, 'champagne be')` |
| Relevance: `multi_match()` / `multimatch()` | 3 | `WHERE multi_match([Title, Body], 'IPA')` |
| Relevance: `query()` | 8 | `WHERE query('Tags:taste')` |
| Relevance: `wildcard_query()` / `wildcardquery()` | 22 | `WHERE wildcard_query(KeywordBody, 'test*')` |
| `convert_tz()` | 17 | `SELECT convert_tz('2021-05-12','+10:00','+11:00')` |
| `DATETIME()` | 15 | `SELECT DATETIME('2008-01-01 02:00:00+10:00', '-10:00')` |
| `Log()` (case-sensitive) | 1 | `SELECT Log(MAX(age) + MIN(age))` |
| `percentiles()` | 2 | `SELECT percentiles(age, 25.0, 50.0, 75.0, 99.9)` |
| `ISNULL()` | 1 | `SELECT ISNULL(lastname)` |
| `nested()` | 22 | `SELECT nested(message.info)` |

---

## Category 2: EXPR_TIMESTAMP Type Conversion (21 queries)

**Error:** `class java.lang.String: need to implement EXPR_TIMESTAMP`  
**Root Cause:** The unified pipeline encounters OpenSearch's internal `EXPR_TIMESTAMP` type during execution but doesn't have a converter for it.

| Sample Query |
|-------------|
| `SELECT login_time FROM opensearch-sql_test_index_datetime` |
| `SELECT DATE_FORMAT(birthdate, 'YYYY-MM-dd') FROM opensearch-sql_test_index_bank` |

---

## Category 3: Comma-Join / Nested Table Syntax (21 queries)

**Error:** `Table '<name>' not found`  
**Root Cause:** Legacy comma-join syntax (`FROM t1 e, e.message m`) creates table references that Calcite can't resolve. The `e.message` is treated as a table name rather than a nested field path.

| Sample Query |
|-------------|
| `SELECT * FROM opensearch-sql_test_index_nested_type e, e.message m` |
| `SELECT m.* FROM opensearch-sql_test_index_nested_type e, e.message m` |

---

## Category 4: String Literal Treated as Identifier (16 queries)

**Error:** `Column 'Hello' not found in any table`  
**Root Cause:** Calcite treats double-quoted strings as identifiers per ANSI SQL standard. `select "Hello"` looks for a column named `Hello`. OpenSearch SQL treats double-quoted strings as string literals.

| Sample Query | Error |
|-------------|-------|
| `select "Hello"` | `Column 'Hello' not found` |
| `SELECT highlight('Tags')` | `Column 'Tags' not found` |

---

## Category 5: Backtick Identifiers (8 queries)

**Error:** `Lexical error: Encountered "\`" (96)`  
**Root Cause:** Calcite's SQL parser does not recognize backtick as an identifier quote character by default.

| Sample Query |
|-------------|
| `` SELECT AVG(age) as `avg` FROM ... `` |
| `` SELECT MAX(age) + 1 as `add` FROM ... `` |

**Fix:** Configure `Quoting.BACK_TICK` in Calcite's `SqlParser.Config`.

---

## Category 6: GROUP BY Expression Issues (7 queries)

**Error:** `Expression 'lastname' is not being grouped`  
**Root Cause:** Calcite enforces strict GROUP BY validation. Queries referencing non-aggregated columns without GROUP BY fail.

| Sample Query |
|-------------|
| `SELECT IFNULL(lastname, 'unknown') AS name FROM ... GROUP BY name` |
| `SELECT gender, Log(age+10), max(balance) FROM ... GROUP BY gender, age` |

---

## Category 7: Unknown Field * / Wildcard Issues (6 queries)

**Error:** `Unknown field '*'`  
**Root Cause:** `SELECT COUNT(*)` and similar wildcard patterns not properly resolved in certain contexts.

| Sample Query |
|-------------|
| `SELECT COUNT(*) FROM ... GROUP BY age` |
| `SELECT COUNT(*), AVG(age) FROM ... GROUP BY age` |

---

## Category 8: Index Not Found (5 queries)

**Error:** `index_not_found_exception`  
**Root Cause:** Queries reference indices that don't exist in the test cluster (e.g., alias references like `tab`, `e`).

---

## Category 9: Other (62 queries)

Remaining failures include: SHOW/DESCRIBE syntax (4), reserved word conflicts (3), trailing semicolons, quote escaping, cast errors, type mismatches, nested type access, and multi-line Calcite parser errors.

---

## Progress Tracking

| Fix Applied | Before | After | Improvement |
|------------|--------|-------|-------------|
| Baseline (Calcite native parser) | 3 / 807 (0.4%) | — | — |
| + Quote hyphenated table names | — | 575 / 807 (71.3%) | +572 queries |

---

## Priority Matrix

| Priority | Category | Queries | Fix |
|----------|----------|---------|-----|
| **P0** | Backtick identifiers | 8 | Configure `Quoting.BACK_TICK` |
| **P1** | Missing relevance functions | 67 | Register in Calcite |
| **P1** | EXPR_TIMESTAMP conversion | 21 | Implement type converter |
| **P1** | Missing datetime functions | 32 | Register convert_tz, DATETIME |
| **P2** | Comma-join / nested table | 21 | Implement nested table resolution |
| **P2** | String literal vs identifier | 16 | Parser configuration or transform |
| **P2** | GROUP BY strictness | 7 | Relax validation or transform |
| **P2** | COUNT(*) / wildcard | 6 | Fix wildcard resolution |
| **P3** | SHOW/DESCRIBE, reserved words, etc. | 62 | Various |

---

## Note on PPL vs SQL Relevance Function Behavior

PPL's `match()` works (98.3% success) while SQL's `match()` fails. This is because:
- **PPL path:** Relevance function arguments are represented as `UnresolvedArgument` AST nodes, resolved directly during planning — `PredicateAnalyzer` can convert them to native OpenSearch queries
- **SQL path (Calcite native):** The function isn't registered in Calcite's function registry at all, so it fails at parse/validate time with "No match found for function signature"

---

## How to Run

```bash
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.*IT' --tests 'org.opensearch.sql.legacy.*IT' -Dunified.gap.analysis=true
```
