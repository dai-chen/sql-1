# SQL V2 + CalciteRelNodeVisitor: Complete Gap Table

**Date**: 2026-05-12  
**Pass Rate**: 650 / 964 (67.4%) with S-effort fixes + grammar extensions applied  
**Branch**: https://github.com/dai-chen/sql-1/tree/sql-v2-calcite-gap-analysis

## Complete SQL Gap Table

| # | Gap | Failures | Example Query | SQL-Only? | Origin | Status |
|---|-----|----------|---------------|-----------|--------|--------|
| **Grammar Gaps (cannot parse)** | | | | | | |
| 1 | SHOW/DESCRIBE with unquoted identifiers | 11 | `SHOW TABLES LIKE opensearch-sql_test_index_account` | ✅ SQL-only | SQL Plugin | Grammar expects STRING_LITERAL |
| 2 | Comma-join / nested FROM (PartiQL) | 5 | `SELECT * FROM nested_type e, e.message m` | ✅ SQL-only | SQL Plugin | Grammar gap |
| 3 | PERCENTILES / STATS / EXTENDED_STATS | 5 | `SELECT PERCENTILES(age, 25.0, 50.0) FROM account` | ✅ SQL-only | SQL Plugin | Legacy functions not in grammar |
| 4 | Index names with dots | 3 | `SELECT * FROM test-logs-2025.01.01` | ✅ SQL-only | SQL Plugin | Use backticks |
| 5 | Legacy method syntax (REGEXP_QUERY, wildcardQuery) | 3 | `SELECT * FROM account WHERE address=REGEXP_QUERY('.*')` | ✅ SQL-only | SQL Plugin | Legacy syntax |
| 6 | Qualified wildcard `m.*` | 2 | `SELECT m.* FROM nested_type e, e.message m` | ✅ SQL-only | SQL Plugin | Grammar gap |
| 7 | Index/type syntax | 1 | `SELECT * FROM account/wrongType` | ✅ SQL-only | SQL Plugin | Legacy syntax |
| 8 | Subquery EOF parse | 1 | `SELECT col FROM (SELECT * FROM table)` with pagination | ✅ SQL-only | SQL Plugin | Edge case |
| 9 | Scalar subquery in SELECT | 0 (no test) | `SELECT (SELECT MAX(x) FROM t2) FROM t1` | ✅ SQL-only | SQL Plugin | Not in grammar; visitor ready |
| 10 | EXCEPT/MINUS | 0 (throws) | `SELECT a FROM t1 EXCEPT SELECT a FROM t2` | ✅ SQL-only | SQL Plugin | Grammar added, visitor throws |
| | | | | | | |
| **Visitor/Execution Gaps (parses but fails)** | | | | | | |
| 11 | Field [X] not found in aggregation Project | 36 | `SELECT MAX(balance), AVG(age) FROM account GROUP BY gender` | ✅ SQL-only | SQL Plugin | SQL wraps agg in Project; PPL doesn't |
| 12 | NESTED function not registered | 33 | `SELECT nested(message.author.name) FROM multi_nested` | ❌ Shared | SQL Plugin + AE | PPL also uses nested() |
| 13 | vectorSearch() table function | 30 | `SELECT * FROM vectorSearch(table='t', field='f', vector='[1]', option='k=5')` | ✅ SQL-only | AE | AE needs TableFunctionScan support |
| 14 | Pagination schema mismatch | 26 | `SELECT * FROM bank` (with fetchSize=5) | ❌ Shared | SQL Plugin | Calcite path has no pagination |
| 15 | RexNode NPE (nested wildcard) | 20 | `SELECT nested(message.*) FROM nested_type` | ❌ Shared | SQL Plugin | Part of NESTED gap |
| 16 | WILDCARD_QUERY not registered | 18 | `SELECT body FROM test WHERE wildcard_query(KeywordBody, 't*')` | ✅ SQL-only | SQL Plugin + AE | Needs function registration + AE DSL mapping |
| 17 | AVG on temporal types rejected | 13 | `SELECT AVG(datetime1) FROM calcs` | ❌ Shared | SQL Plugin | Type checker too strict |
| 18 | Schema type mismatch (keyword vs text) | 11 | `SELECT firstname FROM accounts WHERE match(firstname, 'Amber')` | ✅ SQL-only | SQL Plugin | Response metadata issue |
| 19 | Explain format assertions | 11 | `EXPLAIN SELECT * FROM bank WHERE age > 30` | ✅ SQL-only | SQL Plugin | Tests expect V2 explain format |
| 20 | JOIN explain alias mismatch | 11 | `EXPLAIN SELECT a.id, b.name FROM order a JOIN bank b ON a.id = b.id` | ✅ SQL-only | SQL Plugin | Alias not propagated in explain |
| 21 | Column name = alias instead of expression | 10 | `SELECT NULLIF(lastname, 'unknown') AS name FROM accounts` | ✅ SQL-only | SQL Plugin | Calcite uses alias as column label |
| 22 | Window: ROW_NUMBER still failing | 3 | `SELECT ROW_NUMBER() OVER(ORDER BY age) FROM bank` | ❌ Shared | SQL Plugin | Mapping added but not taking effect |
| 23 | Window: PERCENTILE over window | 3 | `SELECT PERCENTILE(balance, 50) OVER() FROM bank` | ❌ Shared | SQL Plugin | Not in WINDOW_FUNC_MAPPING |
| 24 | Pagination cursor unsupported | 5 | `SELECT * FROM bank LIMIT 5` (with fetchSize, cursor iteration) | ❌ Shared | SQL Plugin | Calcite has no cursor support |
| 25 | Field resolution misc | 3 | `SELECT birthdate FROM account` / concurrent queries | ✅ SQL-only | SQL Plugin | Same root cause as #11 |
| 26 | Error message format mismatch | 2 | Tests expect "IndexNotFoundException" but get "SyntaxCheckException" | ✅ SQL-only | SQL Plugin | Test assertion issue |
| 27 | Expected exception not thrown | 2 | Self-join without alias — Calcite handles it, test expects error | ✅ SQL-only | SQL Plugin | Calcite more lenient |
| 28 | Explain plan node IDs differ | 1 | `EXPLAIN SELECT ... ORDER BY ...` — non-deterministic rel#IDs | ✅ SQL-only | SQL Plugin | Brittle test |
| | | | | | | |
| **Code-level gaps (not surfaced by tests)** | | | | | | |
| 29 | NOT LIKE function | 0 | `SELECT * FROM t WHERE name NOT LIKE '%test%'` | ❌ Shared | SQL Plugin | Not registered in PPLFuncImpTable |
| 30 | INTERVAL compound units | 0 | `SELECT date + INTERVAL 1 DAY_HOUR FROM t` | ✅ SQL-only | SQL Plugin | PlanUtils throws UnsupportedOp |
| 31 | INTERVAL with non-literal expression | 0 | `SELECT date + INTERVAL col DAY FROM t` | ✅ SQL-only | SQL Plugin | NumberFormatException |
| 32 | `\|\|` string concatenation | 0 | `SELECT 'hello' \|\| ' world'` | ✅ SQL-only | SQL Plugin | Not in V2 grammar |
| 33 | RelationSubquery (derived table alias lost) | 0 | `SELECT * FROM (SELECT a, b FROM t) AS sub` | ✅ SQL-only | SQL Plugin | No visitRelationSubquery |

## Summary

| Category | Failures | SQL-Only | Shared with PPL |
|----------|----------|----------|-----------------|
| Grammar gaps | 31 | 31 | 0 |
| Visitor/execution gaps | 204 | 128 | 76 |
| Code-level (no test) | 0 | 4 | 1 |
| **Total** | **283** (+5 code) | **163** | **77** |

## For AE Team: SQL-Specific RelNode/Operator Support Needed (Beyond PPL)

| # | What AE Needs Beyond PPL | RelNode/Operator | Example |
|---|--------------------------|-----------------|---------|
| 1 | vectorSearch() table function | `TableFunctionScan` | `FROM vectorSearch(table='t', ...)` |
| 2 | WILDCARD_QUERY in filter | `RexCall(WILDCARD_QUERY, field, pattern)` | `WHERE wildcard_query(body, 'test*')` |
| 3 | Aggregation with expression Project | `LogicalProject` over `LogicalAggregate` with field refs | `SELECT MAX(balance+1) FROM t GROUP BY age` |
| 4 | INTERVAL compound units | `RexLiteral(INTERVAL DAY TO HOUR)` | `date + INTERVAL 1 DAY_HOUR` |

Everything else (JOIN, UNION, IN/EXISTS, filter, sort, window, basic aggregation) produces identical RelNode types as PPL — no additional AE work needed.
