# Unified Query API — SQL Gap Analysis Report

**Date:** 2026-03-24  
**Branch:** `poc/gap-analysis-on-formal` (based on `feature/unified-calcite-sql-support`)  
**Scope:** All SQL IT tests — V2 (`org.opensearch.sql.sql.*IT`, 50 classes) + Legacy (`org.opensearch.sql.legacy.*IT`, 39 classes) — 807 queries  
**Pipeline:** `UnifiedQueryPlanner` (Calcite native SQL parser) → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`

---

## Approach

The gap analyzer intercepts every query in the SQL integration test base classes (`SQLIntegTestCase.executeJdbcRequest()` and `executeQuery()`) and replays it through the unified query pipeline in shadow mode — after the normal V2/legacy REST call succeeds, the same query is planned, compiled, and executed via `UnifiedQueryPlanner` + `UnifiedQueryCompiler`. Failures are collected without affecting the original test. Results are categorized by failure phase (PLAN/COMPILE/EXECUTE) and grouped by error message.

On this branch, the unified SQL path uses **Calcite's native SQL parser** (not the ANTLR-based OpenSearch SQL parser). This means queries go through Calcite's `SqlParser` with `SqlConformanceEnum.LENIENT`, which does not understand OpenSearch-specific SQL extensions.

Toggle: `-Dunified.gap.analysis=true`

---

## Summary

| Metric | Value |
|--------|-------|
| Total Queries | 807 |
| Success | 3 (0.4%) |
| Failed | 804 (99.6%) |
| Failure Phase | 100% PLAN |

The low success rate is expected — Calcite's native SQL parser is strict ANSI SQL and does not support OpenSearch SQL extensions (hyphenated index names, backtick identifiers, relevance functions, etc.).

---

## Category 1: Hyphenated Index Names (264+ queries)

**Error:** `Encountered "-" at line 1, column N`  
**Root Cause:** Calcite's SQL parser treats `-` as a minus operator. Index names like `opensearch-sql_test_index_account` are parsed as `opensearch` minus `sql_test_index_account`. This is the dominant failure category.

| Sample Query | Error |
|-------------|-------|
| `SELECT * FROM opensearch-sql_test_index_account` | `Encountered "-" at line 1, column 25` |
| `SELECT COUNT(age) AS cnt FROM opensearch-sql_test_index_account` | `Encountered "-" at line 1, column 55` |

**Fix:** Quote index names with double quotes (`"opensearch-sql_test_index_account"`) or use Calcite's bracket quoting. Alternatively, configure the gap analyzer to auto-quote hyphenated table names.

---

## Category 2: Backtick Identifiers (8 queries)

**Error:** `Lexical error: Encountered "\`" (96)`  
**Root Cause:** Calcite's SQL parser does not recognize backtick (\`) as an identifier quote character. OpenSearch SQL uses backticks for identifiers like `` `avg` ``, `` `add` ``.

| Sample Query | Error |
|-------------|-------|
| `` SELECT AVG(age) as `avg` FROM ... `` | `Lexical error at line 1, column 20. Encountered: "\`"` |
| `` SELECT MAX(age) + 1 as `add` FROM ... `` | `Lexical error at line 1, column 28. Encountered: "\`"` |

**Fix:** Configure Calcite's `SqlParser` with `Quoting.BACK_TICK` or `Quoting.BACK_TICK_BACKSLASH`.

---

## Category 3: Missing Calcite Functions (32 queries)

**Error:** `No match found for function signature <name>(...)`  
**Root Cause:** OpenSearch-specific functions not registered in Calcite's function registry.

| Function | Count | Sample Query |
|----------|-------|--------------|
| `convert_tz()` | 17 | `SELECT convert_tz('2021-05-12','..','...')` |
| `DATETIME()` (2-arg) | 10 | `SELECT DATETIME('2008-01-01 02:00:00+10:00', '-10:00')` |
| `DATETIME()` (1-arg) | 5 | `SELECT DATETIME('2008-01-01 02:00:00')` |

---

## Category 4: SHOW/DESCRIBE Syntax (4 queries)

**Error:** `Incorrect syntax near the keyword 'SHOW'` / `Encountered "show"`  
**Root Cause:** `SHOW TABLES LIKE` and `DESCRIBE TABLES LIKE` are not standard SQL — Calcite's parser doesn't support them.

| Sample Query |
|-------------|
| `SHOW TABLES LIKE opensearch-sql_test_index_account` |
| `DESCRIBE TABLES LIKE opensearch-sql_test_index_account` |

---

## Category 5: Reserved Word Conflicts (3 queries)

**Error:** `Encountered "max"` / `Encountered "one"`  
**Root Cause:** `max` and `one` are reserved words in Calcite's SQL parser. Using them as aliases or identifiers without quoting fails.

| Sample Query |
|-------------|
| `SELECT ... max(balance) as max FROM ...` |
| `SELECT one.login_time, two.login_time FROM ... AS one JOIN ...` |

---

## Category 6: String Literals and Quoting (4 queries)

**Error:** `Column 'Hello' not found` / `Encountered "'"`  
**Root Cause:** Calcite treats single-quoted strings as string literals (correct) but double-quoted strings as identifiers. `select "Hello"` looks for a column named `Hello`. Backslash-escaped quotes (`\'`) are also not handled.

| Sample Query | Error |
|-------------|-------|
| `select "Hello"` | `Column 'Hello' not found in any table` |
| `select "I""m"` | `Column 'I"m' not found in any table` |
| `select 'I\'m'` | `Encountered "'" at line 1, column 13` |
| `SELECT ... INTERVAL 2 DAY;` | `Encountered ";" at line 1, column 112` |

---

## Category 7: TYPEOF Type Mismatch (1 query)

**Error:** `Cannot apply 'TYPEOF' to arguments of type 'TYPEOF(<TIMESTAMP(0)>)'`  
**Root Cause:** `TYPEOF()` is registered but only accepts `VARIANT` type, not `TIMESTAMP`.

| Sample Query |
|-------------|
| `SELECT typeof(CAST('1961-04-12 09:07:00' AS TIMESTAMP))` |

---

## Priority Matrix

| Priority | Category | Queries | Fix Complexity |
|----------|----------|---------|----------------|
| **P0** | Hyphenated index names | 264+ | Low — auto-quote in gap analyzer or configure parser |
| **P0** | Backtick identifiers | 8 | Low — configure `Quoting.BACK_TICK` |
| **P1** | Missing functions (convert_tz, DATETIME) | 32 | Medium — register in Calcite |
| **P1** | SHOW/DESCRIBE syntax | 4 | Medium — add custom parser extension |
| **P2** | Reserved word conflicts | 3 | Low — quote identifiers |
| **P2** | String literal/quoting | 4 | Low — parser configuration |
| **P3** | TYPEOF type mismatch | 1 | Low — widen accepted types |

---

## How to Run

```bash
# All SQL V2 + Legacy tests
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.*IT' --tests 'org.opensearch.sql.legacy.*IT' -Dunified.gap.analysis=true

# Specific test class
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.MatchIT' -Dunified.gap.analysis=true

# Report is printed to stderr at JVM shutdown
```

---

## Note on PPL vs SQL Relevance Function Behavior

On the previous PoC branch (which used the ANTLR-based OpenSearch SQL parser), `match()`, `match_phrase()`, and `match_phrase_prefix()` planned successfully but failed at EXECUTE with "Relevance search query functions are only supported when they are pushed down". This was because the SQL AST wraps relevance function arguments in `MAP_VALUE_CONSTRUCTOR` RexCalls, which `PredicateAnalyzer` cannot convert to native OpenSearch queries — it falls back to a script query that the script engine can't evaluate.

The PPL path does NOT have this issue because PPL's AST represents relevance function arguments differently (as `UnresolvedArgument` nodes that are resolved directly, not wrapped in `MAP_VALUE_CONSTRUCTOR`).

On this branch (Calcite native SQL parser), this issue doesn't surface because queries fail earlier at the PLAN phase due to hyphenated index names and other Calcite parser limitations.
