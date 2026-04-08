# Unified Query API — SQL Gap Analysis Report (Reclassified)

**Date:** 2026-04-07
**Branch:** https://github.com/dai-chen/sql-1/tree/poc/gap-analysis-on-formal
**Scope:** All SQL IT tests — V2 (`org.opensearch.sql.sql.*IT`, 50 classes) + Legacy (`org.opensearch.sql.legacy.*IT`, 39 classes) — 807 queries
**Pipeline:** `UnifiedQueryPlanner` (Calcite native SQL parser) → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`

> Reclassifies the [Mar 30 baseline](https://github.com/opensearch-project/sql/issues/5248#issuecomment-4158344192) based on root-cause audit. Numbers unchanged (259 failures). Categories reorganized to reflect actual root causes — merged overlapping categories, split misclassified ones, promoted large sub-categories.

## Summary

| Metric | Count | % |
|--------|------:|--:|
| Total Queries | 807 | 100% |
| ✅ Success | 548 | 67.9% |
| ❌ Failed (Exception) | 232 | 28.7% |
| ❌ Failed (Result Mismatch) | 27 | 3.3% |
| **Total Failed** | **259** | **32.1%** |

## All Failure Categories

| # | Category | Count | Phase | Difficulty | Type |
|---|----------|------:|-------|------------|------|
| 1 | Missing Calcite Functions | 86 | PLAN | Mixed | Function registration |
| 2 | EXPR_TIMESTAMP/TIME Type Conversion | 22 | EXECUTE | Medium | Type system |
| 3 | Comma-Join / Nested Table Syntax | 26 | PLAN/EXECUTE | Hard | Semantic gap |
| 4 | Relevance Function Named Parameters | 13 | PLAN | Medium | Function parameter syntax |
| 5 | String Literal vs Identifier | 2 | PLAN | Easy | Parser config |
| 6 | Backtick Identifiers | 8 | PLAN | Easy | Parser config |
| 7 | GROUP BY Expression Issues | 8 | PLAN | Medium | Validation strictness |
| 8 | Wildcard `*` in nested() | 6 | PLAN | Medium | Schema resolution |
| 9 | Square Bracket `[field]` Syntax | 17 | PLAN | Medium | Parser grammar |
| 10 | `match()` Reserved Word | 16 | PLAN | Medium | Parser config |
| 11 | Other | 28 | PLAN | Mixed | Various |
| 12 | Result Mismatch | 27 | RESULT | Mixed | Semantic differences |
| | **Total** | **259** | | | |

> **Reclassification notes vs Mar 30 baseline:**
> - Old Cat 8 (Index Not Found, 5) merged into Cat 3 — same comma-join root cause, alias passed to OpenSearch as literal index name
> - Old Cat 4 (16) split: 13 are relevance function named-parameter syntax (new Cat 4), 2 remain as string literal issues (Cat 5), 1 moved to Cat 7 (GROUP BY alias)
> - Old Cat 9 sub-categories 9A (17) and 9B (16) promoted to top-level (Cat 9, Cat 10) — both larger than several existing categories
> - Old Cat 9 sub-category 9J (EXPR_TIME, 1) merged into Cat 2 — same code path as EXPR_TIMESTAMP

## Category 1: Missing Calcite Functions (86 queries)
**Error:** `No match found for function signature <name>(...)`

| Function Group | Count | Examples |
|---------------|------:|---------|
| Relevance: `wildcard_query()` / `wildcardquery()` | 22 | `WHERE wildcard_query(KeywordBody, 'test*')` |
| Relevance: `convert_tz()` | 17 | `SELECT convert_tz('2021-05-12','+10:00','+11:00')` |
| Relevance: `DATETIME()` | 15 | `SELECT DATETIME('2008-01-01 02:00:00+10:00', '-10:00')` |
| Relevance: `match_query()` / `matchquery()` | 10 | `WHERE match_query(lastname, 'Bates')` |
| Relevance: `match_phrase()` / `matchphrase()` | 10 | `WHERE match_phrase(phrase, 'quick fox')` |
| Relevance: `match_phrase_prefix()` | 8 | `WHERE match_phrase_prefix(Title, 'champagne be')` |
| Relevance: `query()` | 8 | `WHERE query('Tags:taste')` |
| Relevance: `multi_match()` / `multimatch()` | 3 | `WHERE multi_match([Title, Body], 'IPA')` |
| Relevance: `matchphrasequery()` | 2 | `WHERE matchphrasequery(phrase, 'quick fox')` |
| `nested()` | 1 | `SELECT nested(message.info)` |
| `percentiles()` | 2 | `SELECT percentiles(age, 25.0, 50.0, 75.0, 99.9)` |
| `ISNULL()` | 1 | `SELECT ISNULL(lastname)` |
| `Log()` (case-sensitive) | 1 | `SELECT Log(MAX(age) + MIN(age))` |

**Note:** Core relevance functions (`match`, `match_phrase`, `match_bool_prefix`, `match_phrase_prefix`, `multi_match`, `simple_query_string`, `query_string`) are registered in PPL's Calcite path via `PPLBuiltinOperators`. The gap is that the SQL path doesn't chain the same operator table. `wildcard_query` and `query` are not implemented as Calcite operators at all.

## Category 2: EXPR_TIMESTAMP/TIME Type Conversion (22 queries)
**Error:** `class java.lang.String: need to implement EXPR_TIMESTAMP` / `EXPR_TIME`
OpenSearch returns `EXPR_TIMESTAMP`/`EXPR_TIME` typed values that the Calcite pipeline doesn't have a converter for. Affects datetime columns and functions like `DATE_FORMAT()`. Includes 1 EXPR_TIME query (same root cause).

## Category 3: Comma-Join / Nested Table Syntax (26 queries)
**Error:** `Table '<name>' not found` / `no such index [<alias>]`
Legacy syntax `FROM t1 e, e.message m` treats `e.message` as a nested field path. Calcite parses it as a table reference. This is an OpenSearch-specific semantic with no ANSI SQL equivalent. Includes 5 queries (previously "Index Not Found") where the table alias (`e`, `tab`) passes through to OpenSearch as a literal index name.

## Category 4: Relevance Function Named Parameters (13 queries)
**Error:** `Column 'slop' not found` / `Column 'boost' not found` / `Column 'analyzer' not found` / `Column 'query' not found`
Relevance functions use `key=value` named parameter syntax (e.g., `match_phrase(phrase, 'quick fox', slop=2)`). Calcite doesn't understand this convention and treats `slop`, `boost`, `analyzer`, `query`, `max_expansions` as column references.

## Category 5: String Literal vs Identifier (2 queries)
**Error:** `Column 'Hello' not found in any table`
Calcite follows ANSI SQL: double-quoted strings are identifiers. V2 treats them as string literals (`SELECT "Hello"`).
**Fix:** Configure `SqlParser.Config` quoting behavior, or accept ANSI behavior and document the migration.

## Category 6: Backtick Identifiers (8 queries)
**Error:** `Lexical error: Encountered "\`" (96)`
**Fix:** Configure `Quoting.BACK_TICK` in `SqlParser.Config`.

## Category 7: GROUP BY Expression Issues (8 queries)
**Error:** `Expression 'lastname' is not being grouped`
Calcite enforces strict GROUP BY validation per ANSI SQL. V2 is more lenient. Includes 1 query where a SELECT alias used in GROUP BY is not resolved.

## Category 8: Wildcard `*` in nested() (6 queries)
**Error:** `Unknown field '*'`
`nested(message.*)` wildcard expansion fails during schema resolution. Distinct from Cat 3 — the `nested()` function parses fine, but `*` expansion inside function arguments is unsupported.

## Category 9: Square Bracket `[field]` Syntax (17 queries)
**Error:** `Encountered "[" at line 1, column N`
Calcite's parser does not recognize `[field1, field2]` array literal syntax used by `multi_match`, `query_string`, and `highlight` functions.

## Category 10: `match()` Reserved Word (16 queries)
**Error:** `Encountered "match" at line 1, column N`
`MATCH` is a SQL reserved keyword in Calcite (used for `MATCH_RECOGNIZE`). OpenSearch uses `match()` as a full-text search function in WHERE/HAVING clauses.

## Category 11: Other (28 queries)

| Sub-Cat | Name | Count | Difficulty |
|---------|------|------:|------------|
| 11a | SHOW/DESCRIBE Syntax | 11 | Medium |
| 11b | Trailing Semicolons | 6 | Easy |
| 11c | Reserved Word as Alias (`max`, `one`, `escape`) | 3 | Easy-Medium |
| 11d | CAST BOOLEAN→INT | 1 | Medium |
| 11e | Slash in Index Name | 1 | Easy |
| 11f | Nested Function + AND keyword | 1 | Hard |
| 11g | Nested Type Access on Array | 1 | Hard |
| 11h | Quote Escaping (backslash) | 1 | Easy |
| 11i | TYPEOF Type Restriction | 1 | Easy |
| 11j | Runtime Type Cast Error | 1 | Medium |

## Category 12: Result Mismatch (27 queries)

Both pipelines execute successfully but return different data.

| Sub-Cat | Count | Issue | Example |
|---------|------:|-------|---------|
| 12a. Extra metadata columns | 9 | Calcite `SELECT *` includes `_id`, `_index`, `_routing`, `_score`, `_seq_no`, `_primary_term` | V2: 11 cols → Calcite: 17 cols |
| 12b. JOIN row explosion | 3 | V2 applies default `LIMIT 200` on JOINs; Calcite returns full result | V2: 200 rows → Calcite: 48,626 rows |
| 12c. Column truncation | 6 | V2 strips ORDER BY column not in SELECT list | `ORDER BY firstname` → V2: `[abbott]`, Calcite: `[abbott, Abbott]` |
| 12d. Numeric precision | 1 | V2 truncates decimals | `PI()` → V2: `3`, Calcite: `3.14` |
| 12e. Geo-point format | 2 | Different serialization | V2: `{"lon":74,"lat":40.71}`, Calcite: `POINT (74 40.71)` |
| 12f. LIKE escape | 6 | V2 recognizes `\%`/`\_`; Calcite requires explicit `ESCAPE` clause | `LIKE '\%test%'` → V2: 1 row, Calcite: 0 rows |
