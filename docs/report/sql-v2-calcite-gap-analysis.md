# SQL V2 → CalciteRelNodeVisitor: Complete Gap Analysis

**Date**: 2026-05-12  
**Pass Rate**: 572 / 964 (59.3%) with all fallback paths disabled  
**Fixes Applied**: Alias in project list, Cursor.None, REST fallback disabled  
**Branch**: https://github.com/dai-chen/sql-1/tree/sql-v2-calcite-gap-analysis

## Background

This analysis evaluates the feasibility of connecting the SQL V2 parser/AST builder to the existing PPL Calcite planner (`CalciteRelNodeVisitor`) — bypassing the V2 Analyzer → Planner → PhysicalPlan path entirely. All fallback paths (Calcite → V2, V2 → legacy) were disabled to expose the complete set of gaps.

The motivation is to provide data for the **Mustang SQL** design decision: integrating the SQL plugin with the Analytics Engine in OpenSearch core on Parquet index **without any fallback path to Lucene index**. One option under consideration is to reuse the V2 parser and AST builder for maximum SQL compatibility, routing the resulting AST through CalciteRelNodeVisitor (which already powers PPL V3) to produce Calcite RelNode plans. This report identifies exactly what works, what doesn't, and what effort is required to close each gap — informing whether this reuse strategy is viable or whether alternative approaches (e.g., Calcite's native SQL parser) are preferable for Mustang.

### Alternative Path Evaluated: Calcite Native Parser + UnifiedQueryCompiler

For comparison, we also tested routing all SQL through `RestUnifiedQueryAction` using Calcite's native `SqlParser` → `SqlToRelConverter` → `UnifiedQueryCompiler` (direct Calcite Bindable execution). Result: **11 / 1007 (1.1%) pass rate**. The test cluster crashed mid-run due to an unhandled `ExceptionInInitializerError` (fatal JVM crash from `log(0)` in Calcite Enumerable execution). Before the crash, 198 queries failed at planning (Calcite parser can't handle OpenSearch-specific functions/syntax) and 143 failed at compilation (RelRunner execution failures). This path is fundamentally not ready — it lacks error resilience, doesn't leverage OpenSearch push-down, and has far worse compatibility than the V2+CalciteRelNodeVisitor approach.

---

## Category A: V2 Grammar / Parser Gaps

These queries **cannot be parsed** by the V2 ANTLR grammar (`OpenSearchSQLParser.g4`). They fail with `SyntaxCheckException` before reaching CalciteRelNodeVisitor.

| # | Gap | Failures | Example Query | Effort | Solution |
|---|-----|----------|---------------|--------|----------|
| A1 | **JOIN** (INNER, LEFT, cross) | 19 | `SELECT a.firstname FROM bank a INNER JOIN order b ON a.name = b.name` | L | Extend V2 grammar with JOIN clause + AstBuilder visitJoin. `Join` AST node and `CalciteRelNodeVisitor.visitJoin` already exist — only grammar + AstBuilder missing |
| A2 | **SHOW/DESCRIBE with unquoted hyphenated identifiers** | 11 | `SHOW TABLES LIKE opensearch-sql_test_index_account` | S | V2 grammar supports SHOW/DESCRIBE but `tableFilter` expects `STRING_LITERAL`. Tests pass unquoted identifiers. Fix: extend `tableFilter` to accept ID |
| A3 | **Nested FROM syntax** (PartiQL comma-join) | 7 | `SELECT * FROM nested_type e, e.message m` | M | Extend grammar FROM clause to allow comma-separated relations + nested path expressions |
| A4 | **PERCENTILES / STATS / EXTENDED_STATS** | 7 | `SELECT PERCENTILES(age, 25.0, 50.0) FROM account` | M | Tokens exist in lexer but unused in parser. Add parser rules. Note: `PERCENTILE` (singular) IS supported |
| A5 | **Legacy method syntax** (`field=FUNC('arg')`) | 5 | `SELECT * FROM account WHERE address=REGEXP_QUERY('.*')` | M | Only MATCH_QUERY/MATCHPHRASE have `altSingleFieldRelevanceFunction` rule. REGEXP_QUERY does not. Add to grammar or use standard function-call syntax |
| A6 | **Index names with dots** | 3 | `SELECT * FROM test-logs-2025.01.01` | S | Grammar splits on dots into `qualifiedName` parts. `01` fails as ident (starts with digit). Require backtick quoting (already works with backticks) |
| A7 | **IN / EXISTS / NOT EXISTS subquery** | 0 (excluded) | `WHERE age IN (SELECT age FROM bank2 WHERE age > 30)` | L | `IN` predicate only accepts expression list, not subquery. `EXISTS` token defined in lexer but unused in parser. Add `IN '(' querySpecification ')'` and `EXISTS '(' querySpecification ')'` rules |
| A8 | **Qualified wildcard** (`alias.*`) | 2 | `SELECT m.* FROM nested_type e, e.message m` | S | Add qualifiedStar rule to selectElement in grammar |
| A9 | **Multi-table FROM** (comma-join, non-nested) | 1 | `SELECT * FROM dog_index, dogs2_index` | S | Allow comma-separated table list in FROM clause |
| A10 | **date_histogram** | 1 | `SELECT count(*) FROM online GROUP BY date_histogram('field'='insert_time','fixed_interval'='4d')` | S | Add date_histogram to function name tokens in parser |
| A11 | **Index/type syntax** (`/type`) | 1 | `SELECT * FROM account/wrongType` | S | Add optional `/type` suffix to table reference rule |
| A12 | **UNION / INTERSECT / MINUS** | 0 (excluded) | `SELECT name FROM bank UNION ALL SELECT dog_name FROM dog` | L | Add set-operation rules to grammar + AST Append/Union nodes |
| A13 | **Correlated subquery** | 0 (excluded) | `WHERE EXISTS (SELECT * FROM dept d WHERE d.id = e.dept_id)` | L | Requires A7 first + decorrelation support in planner |

**Total Category A failures: ~57 (active) + 3 hidden gaps with 0 active tests (A7, A12, A13)**

**Note on subqueries**: V2 grammar supports derived tables (`FROM (SELECT ...) AS alias`) via the `subqueryAsRelation` rule. CalciteRelNodeVisitor has `visitSubqueryAlias` for this. However, `IN (SELECT ...)`, `EXISTS (SELECT ...)`, and correlated subqueries are not in the V2 grammar — the `EXISTS` token is defined in the lexer but unused in any parser rule, and `IN` only accepts expression lists, not nested SELECTs. The legacy `SubqueryIT` (18 tests) is entirely excluded from the build.

---

## Category B: CalciteRelNodeVisitor Execution Gaps

These queries parse successfully into AST but fail during Calcite visitor traversal or execution.

| # | Gap | Failures | Example Query | Effort | Solution |
|---|-----|----------|---------------|--------|----------|
| B1 | **Aggregation ArgumentMap NPE** (BUCKET_NULLABLE) | 82 | `SELECT MAX(datetime1), MIN(datetime1) FROM calcs` | S | Null-check `statsArgs.get(Argument.BUCKET_NULLABLE)` at CalciteRelNodeVisitor.java:1612. SQL's `Aggregation` has empty `argExprList` (PPL sets BUCKET_NULLABLE). Default to `false`. |
| B2 | **AggregateFunction → Function cast** | 29 | `SELECT AVG(num3) OVER(PARTITION BY datetime1) FROM calcs` | S | Add `instanceof AggregateFunction` branch at CalciteRexNodeVisitor.java:564. `AggregateFunction` and `Function` are sibling classes (both extend `UnresolvedExpression`), not parent-child. |
| B3 | **Pagination unsupported** (schema mismatch + cursor) | 35 | `SELECT * FROM bank` (with fetchSize) | L | `visitPaginate()` and `visitFetchCursor()` both throw `CalciteUnsupportedException`. Non-paged goes through Calcite; paged falls back to V2 → different schemas. Implement pagination in Calcite or ensure schema consistency. |
| B4 | **Table function unsupported** | 26 | `SELECT v._id FROM vectorSearch(table='t', field='f', vector='[1.0]', option='k=5') AS v` | L | `visitTableFunction()` is a complete stub (single throw). Implement KNN scan translation. |
| B5 | **NESTED function** | 33 | `SELECT nested(message.author.name) FROM multi_nested` | L | NESTED not registered in `PPLFuncImpTable.java`. Requires special handling — not a simple function, needs unnest/correlate pattern for OpenSearch nested field access. |
| B6 | **WILDCARD_QUERY** | 18 | `SELECT body FROM wildcard WHERE wildcard_query(KeywordBody, 't*')` | M | Not registered in `PPLFuncImpTable.java`. Register and map to OpenSearch wildcard query DSL. |
| B7 | **Relevance function aliases** (MATCH_QUERY, MATCHPHRASE, MATCHQUERY) | 14 | `SELECT firstname FROM accounts WHERE match_query(lastname, 'Bates')` | S | `PPLFuncImpTable` registers MATCH, MATCH_PHRASE, MULTI_MATCH but NOT the aliases (MATCH_QUERY, MATCHQUERY, MATCHPHRASE). Add alias registrations pointing to same operators. |
| B8 | **LIMIT not applied** (visitLimit missing) | 9 | `SELECT last_day(date0) FROM calcs LIMIT 3` → returns 17 rows | S | `CalciteRelNodeVisitor` does NOT override `visitLimit()`. Default `visitChildren()` silently skips it. Add `visitLimit(Limit node, ctx)` calling `context.relBuilder.limit(offset, count)`. |
| B9 | **Window functions** (ROW_NUMBER, RANK, DENSE_RANK) | 8 | `SELECT age, ROW_NUMBER() OVER(ORDER BY age DESC) FROM bank` | S | `WINDOW_FUNC_MAPPING` in `BuiltinFunctionName.java` is missing ranking functions. `PlanUtils.makeOver()` already has `case ROW_NUMBER` handler. Just add to mapping. |
| B10 | **Table name resolution** (dot = schema separator) | 6 | `SELECT __age FROM test.twounderscores` → `Table 'test.twounderscores' not found` | S | `visitRelation` passes `QualifiedName.getParts()` (split on dots) to `relBuilder.scan()`. Calcite interprets `["test","twounderscores"]` as schema.table. Fix: pass resolved name as single-element list. |
| B11 | **QUERY function** | 5 | `SELECT Id FROM beer WHERE query('Tags:taste OR Body:taste')` | M | Not registered in `PPLFuncImpTable.java`. Register and map to OpenSearch query_string DSL. |
| B12 | **Schema type mismatch** (keyword vs text) | 5 | `SELECT phrase FROM phrase WHERE match_bool_prefix(phrase, 'quick')` — schema says `keyword` not `text` | S | Calcite schema resolves field type from mapping incorrectly. Fix type resolution in OpenSearchSchema. |
| B13 | **Column name = alias instead of expression** | 6 | `SELECT NULLIF(lastname, 'unknown') AS name` → schema name is `name` not `NULLIF(...)` | S | Calcite uses the AS alias as column label. V2 uses expression text. Align behavior in response formatter. |
| B14 | **ISNULL / IFNULL unresolved** | 5 | `SELECT ISNULL(lastname) FROM accounts` | S | ISNULL (1-arg boolean) not registered. IFNULL fails with NULL literal (UNDEFINED type). Register + fix type resolution. |
| B15 | **Explain format** | 3 | `EXPLAIN SELECT age FROM account WHERE age IS NOT NULL` | S | Calcite explain outputs different JSON structure. Test regex doesn't match. Fix test or add V2-compatible explain format. |
| B16 | **ArgumentMap NPE** (systemic — PI, GROUP BY, HAVING) | 4 | `SELECT PI()` / `SELECT col FROM t GROUP BY col HAVING COUNT(1) > 0` | S | Same root cause as B1 — ArgumentMap access without null-check in multiple visitor methods. Fix alongside B1. |
| B17 | **Pagination fallback tests** | 5 | `SELECT * FROM bank LIMIT 5` (with fetchSize) | M | Tests verify fallback behavior. With fallback disabled, they fail. Test-infrastructure issue. |
| B18 | **Type name mismatch** (SQL standard vs OpenSearch) | 1 | `SELECT typeof(age)` → returns `BIGINT` instead of `LONG` | S | Calcite uses SQL standard type names. Map back to OpenSearch names in typeof() implementation. |
| B19 | **LIMIT off-by-one** | 1 | `SELECT point FROM geopoints LIMIT 5` → returns 6 rows | S | Off-by-one in SystemLimit or LIMIT application. Related to B8 (visitLimit missing). |

**Total Category B failures: ~295**

---

## Summary Statistics

| Category | Active Failures | Hidden Gaps (0 tests) | Key Blocker |
|----------|----------------|----------------------|-------------|
| A (Grammar) | ~57 | 3 (IN/EXISTS, UNION, correlated) | JOINs (19) — visitor ready, grammar missing |
| B (Visitor) | ~295 | — | ArgumentMap NPE (86), NESTED (33), AggFunc cast (29) |

## Effort Legend

- **S** (Small): < 1 day. Null-check, register function alias, fix config.
- **M** (Medium): 1–3 days. New visitor method, function implementation with DSL mapping.
- **L** (Large): 3+ days. Grammar extension, new execution path, architectural change.

## Quick Wins (S effort, high impact)

| Fix | Effort | Tests Unlocked | File |
|-----|--------|----------------|------|
| B1+B16: Null-check BUCKET_NULLABLE in visitAggregation | S | ~86 | CalciteRelNodeVisitor.java:1612 |
| B2: instanceof AggregateFunction in visitWindowFunction | S | ~29 | CalciteRexNodeVisitor.java:564 |
| B8+B19: Add visitLimit() override | S | ~10 | CalciteRelNodeVisitor.java (new method) |
| B9: Add ROW_NUMBER/RANK/DENSE_RANK to WINDOW_FUNC_MAPPING | S | ~8 | BuiltinFunctionName.java |
| B7: Register MATCH_QUERY/MATCHQUERY/MATCHPHRASE aliases | S | ~14 | PPLFuncImpTable.java |
| B10: Pass resolved name as single-element list to scan() | S | ~6 | CalciteRelNodeVisitor.java visitRelation |
| B14: Register ISNULL + fix IFNULL null-type | S | ~5 | PPLFuncImpTable.java |
| **Total quick wins** | **~2 days** | **~158 tests** | |

## Architectural Notes

1. **JOIN path is ready**: `Join` AST node exists, `CalciteRelNodeVisitor.visitJoin()` is implemented. Only the V2 SQL grammar rule + AstBuilder visitor method are needed to connect SQL JOINs to the existing Calcite path.

2. **Pagination is the biggest L-effort gap**: B3 (35 failures) requires implementing cursor-based pagination in the Calcite execution engine — a fundamentally new capability.

3. **Function registration pattern**: Most unresolved function gaps (B6, B7, B11, B14) follow the same pattern — register in `PPLFuncImpTable.java` and map to appropriate Calcite SqlOperator or custom implementation.

---

## Appendix: Fixes Applied for This Analysis

The following minimal fixes were applied to enable the gap analysis. They are intentionally temporary and non-production-quality.

### Fix 1: Route SQL through CalciteRelNodeVisitor

**File**: `core/src/main/java/org/opensearch/sql/executor/QueryService.java`

```java
// Before:
private boolean shouldUseCalcite(QueryType queryType) {
    return isCalciteEnabled(settings) && queryType == QueryType.PPL;
}

// After:
private boolean shouldUseCalcite(QueryType queryType) {
    return isCalciteEnabled(settings)
        && (queryType == QueryType.PPL || queryType == QueryType.SQL);
}
```

### Fix 2: Enable Calcite + disable fallback in IT tests

**File**: `integ-test/src/test/java/org/opensearch/sql/legacy/SQLIntegTestCase.java`

```java
// Before:
protected void init() throws Exception {
    disableCalcite();
    increaseMaxCompilationsRate();
}

// After:
protected void init() throws Exception {
    enableCalcite();
    disallowCalciteFallback();
    increaseMaxCompilationsRate();
}
```

### Fix 3: Handle Alias expression in project list

**File**: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`

Added `case Alias` to the switch in `expandProjectFields()`:

```java
case Alias alias -> {
    expandedFields.add(rexVisitor.analyze(alias, context));
}
```

This dispatches to the existing `CalciteRexNodeVisitor.visitAlias()` which correctly unwraps the delegated expression and applies the alias name.

### Fix 4: Return Cursor.None instead of null

**File**: `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java`

```java
// Before:
QueryResponse response = new QueryResponse(schema, values, null);

// After:
QueryResponse response = new QueryResponse(schema, values, Cursor.None);
```

### Fix 5: Disable REST-level V2 → legacy fallback

**File**: `legacy/src/main/java/org/opensearch/sql/legacy/plugin/RestSQLQueryAction.java`

Disabled the `SyntaxCheckException` catch in `fallBackListener.onFailure()` to surface all grammar-level failures instead of silently routing to legacy engine.

---

## Appendix B: Function Registration Status

**278 total functions** in `BuiltinFunctionName`. Summary:

| Status | Count | Details |
|--------|-------|---------|
| PPLFuncImpTable (registered in Calcite) | 237 | All math, string, date, arithmetic, boolean, aggregate, collection, JSON, crypto |
| visitCast (handled by cast visitor) | 14 | All CAST_TO_* type conversions |
| makeOver (window function utility) | 2 | ROW_NUMBER, NTH_VALUE |
| Special visitor method | 2 | HIGHLIGHT, SCORE (throw unsupported) |
| External (OpenSearchExecutionEngine) | 2 | GEOIP, DISTINCT_COUNT_APPROX |
| GAP IN REPORT (missing, identified) | 13 | See below |
| NOT REGISTERED (missing, not in report) | 8 | See below |

### Functions Identified as Gaps (13)

| Function | SQL Name | Gap ID |
|----------|----------|--------|
| NESTED | `nested` | B5 |
| ISNULL | `isnull` | B14 |
| RANK | `rank` | B9 |
| DENSE_RANK | `dense_rank` | B9 |
| MATCHPHRASE | `matchphrase` | B7 |
| MATCHPHRASEQUERY | `matchphrasequery` | B7 |
| QUERY | `query` | B11 |
| MATCH_QUERY | `match_query` | B7 |
| MATCHQUERY | `matchquery` | B7 |
| MULTIMATCH | `multimatch` | B7 |
| MULTIMATCHQUERY | `multimatchquery` | B7 |
| WILDCARDQUERY | `wildcardquery` | B6 |
| WILDCARD_QUERY | `wildcard_query` | B6 |

### Functions NOT REGISTERED (8) — Not in Gap Report

| Function | SQL Name | Impact | Notes |
|----------|----------|--------|-------|
| TAN | `tan` | Low | `SqlStdOperatorTable.TAN` exists, trivial to add |
| NOT_LIKE | `not like` | Negligible | Handled by `NOT` + `LIKE` composition at query level |
| SCOREQUERY | `scorequery` | Low | Legacy alias for SCORE (which throws unsupported) |
| SCORE_QUERY | `score_query` | Low | Legacy alias for SCORE |
| BRAIN | `brain` | None for SQL | Internal PPL patterns command function |
| INTERVAL | `interval` | None | Handled implicitly by date arithmetic literals |
| NONE | `none` | None for SQL | PPL convert command no-op |
| INTERNAL_UNCOLLECT_PATTERNS | `uncollect_patterns` | None | Defined but never referenced anywhere |

### Registered Functions by Category (237 in PPLFuncImpTable)

| Category | Count | Examples |
|----------|-------|---------|
| Math | 26 | ABS, CEIL, FLOOR, ROUND, SQRT, LOG, EXP, POW, PI, RAND |
| Trigonometric | 11 | ACOS, ASIN, ATAN, ATAN2, COS, COSH, COT, SIN, SINH, DEGREES, RADIANS |
| Arithmetic | 11 | +, -, *, /, %, MOD, ADD, SUBTRACT, MULTIPLY, DIVIDE |
| Boolean/Comparison | 12 | AND, OR, NOT, XOR, =, !=, <, <=, >, >=, LIKE, ILIKE |
| String | 23 | CONCAT, SUBSTRING, TRIM, UPPER, LOWER, LENGTH, REPLACE, REVERSE, LOCATE |
| Date/Time | 63 | NOW, CURDATE, DATE_FORMAT, DATEDIFF, TIMESTAMPADD, EXTRACT, YEAR, MONTH, DAY |
| Aggregate | 18 | AVG, SUM, COUNT, MIN, MAX, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, PERCENTILE_APPROX |
| Conditional | 8 | IF, IFNULL, NULLIF, COALESCE, IS_NULL, IS_NOT_NULL, IS_PRESENT, IS_EMPTY |
| Collection | 20 | ARRAY, SPLIT, MVAPPEND, MVJOIN, MVINDEX, JSON_EXTRACT, JSON_KEYS |
| JSON | 12 | JSON_VALID, JSON_OBJECT, JSON_ARRAY, JSON_SET, JSON_DELETE |
| Binning | 4 | SPAN_BUCKET, WIDTH_BUCKET, MINSPAN_BUCKET, RANGE_BUCKET |
| Crypto | 3 | MD5, SHA1, SHA2 |
| Relevance | 7 | MATCH, MATCH_PHRASE, MULTI_MATCH, SIMPLE_QUERY_STRING, QUERY_STRING, MATCH_BOOL_PREFIX, MATCH_PHRASE_PREFIX |
| Convert/Type | 10 | AUTO, NUM, CTIME, MKTIME, TYPEOF, TOSTRING, TONUMBER |
| Internal | 9 | ITEM, PATTERN_PARSER, GROK, PARSE, REGEXP_REPLACE |
| Other | 2 | CIDRMATCH, SPAN |
