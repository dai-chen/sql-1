# PPL V4 — Iterative Improvement Loop

## Overview

This document defines the Ralph loop protocol for incrementally building the PPL V4 transpiler
and raising the CalcitePPL*IT pass rate from 0% to ≥85%.

## Architecture Recap

```
PPL string → PPL Parser → AST → PPLToSqlTranspiler → SQL string → /_plugins/_sql → results
```

The transpiler lives at `api/src/main/java/org/opensearch/sql/api/PPLToSqlTranspiler.java`.
Test wiring lives in `PPLIntegTestCase.java` — when `ppl.engine.v4=true`, PPL queries are
transpiled to SQL and sent to the Calcite SQL endpoint instead of the PPL endpoint.

## Test Surface

| Category | Classes | ~Tests | Priority |
|----------|---------|--------|----------|
| Basic (source, where, fields, sort, head) | CalcitePPLBasicIT | 40 | P0 |
| Aggregation (stats, top, rare) | CalcitePPLAggregationIT | 99 | P0 |
| Eval + functions | CalcitePPLBuiltinFunctionIT, CalcitePPLCaseFunctionIT, CalcitePPLConditionBuiltinFunctionIT, CalcitePPLEvalMaxMinFunctionIT | 64 | P0 |
| Sort | CalcitePPLSortIT | 18 | P0 |
| Dedup | CalcitePPLDedupIT | 12 | P1 |
| Rename | CalcitePPLRenameIT | 22 | P1 |
| Eventstats | CalcitePPLEventstatsIT | 27 | P1 |
| Join | CalcitePPLJoinIT | 39 | P1 |
| String functions | CalcitePPLStringBuiltinFunctionIT | 27 | P1 |
| Null handling | CalcitePPLBuiltinFunctionsNullIT | 71 | P1 |
| Datetime invalid | CalcitePPLBuiltinDatetimeFunctionInvalidIT | 45 | P2 |
| Subqueries | CalcitePPLExistsSubqueryIT, CalcitePPLInSubqueryIT, CalcitePPLScalarSubqueryIT | 50 | P2 |
| Cast | CalcitePPLCastFunctionIT | 5 | P2 |
| Fillnull | CalcitePPLFillnullIT | 3 | P2 |
| Parse/Grok/Patterns | CalcitePPLParseIT, CalcitePPLGrokIT, CalcitePPLPatternsIT | 24 | P2 |
| Trendline | CalcitePPLTrendlineIT | 7 | P2 |
| Append | CalcitePPLAppendCommandIT, CalcitePPLAppendPipeCommandIT | 12 | P2 |
| Lookup | CalcitePPLLookupIT | 17 | P2 |
| JSON | CalcitePPLJsonBuiltinFunctionIT | 18 | P3 |
| Spath | CalcitePPLSpathCommandIT | 14 | P3 |
| Coalesce | CalcitePPLEnhancedCoalesceIT | 15 | P3 |
| Crypto | CalcitePPLCryptographicFunctionIT | 4 | P3 |
| IP | CalcitePPLIPFunctionIT | 1 | P3 |
| Explain | CalcitePPLExplainIT | 4 | P3 |
| GraphLookup | CalcitePPLGraphLookupIT | 22 | P3 |
| Appendcol | CalcitePPLAppendcolIT | 2 | P3 |
| Nested agg | CalcitePPLNestedAggregationIT | 9 | P3 |

## Recommended Iteration Order

Each iteration = one bounded task. Expected ~15-25 iterations to reach 85%.

### Phase 1: Foundation (~0% → ~5%)
1. **Scaffold transpiler + test wiring** — Create PPLToSqlTranspiler.java skeleton, wire PPLIntegTestCase V4 path
2. **source + fields + head** — `SELECT cols FROM table LIMIT N`

### Phase 2: Core Commands (~5% → ~30%)
3. **where** — WHERE clause with boolean expressions, comparisons, LIKE, IN, BETWEEN
4. **sort** — ORDER BY with ASC/DESC/NULLS FIRST/LAST
5. **eval** — SELECT *, expr AS alias with subquery wrapping
6. **stats (basic)** — GROUP BY + COUNT/SUM/AVG/MIN/MAX

### Phase 3: Functions (~30% → ~50%)
7. **Math functions** — abs, ceil, floor, round, sqrt, pow, ln, log10, exp, trig
8. **String functions** — upper, lower, length, substring, trim, replace, concat, left, right, reverse
9. **Condition functions** — if→CASE, ifnull→COALESCE, isnull→IS NULL, nullif, coalesce
10. **Cast functions** — CAST(x AS type)

### Phase 4: Intermediate Commands (~50% → ~65%)
11. **dedup** — ROW_NUMBER() OVER (PARTITION BY ...) + filter
12. **rename** — subquery wrap with alias
13. **top/rare** — GROUP BY + COUNT + ORDER BY + LIMIT
14. **eventstats** — window aggregation OVER (PARTITION BY ...)
15. **fillnull** — COALESCE wrapping

### Phase 5: Advanced (~65% → ~80%)
16. **join** — JOIN ... ON with alias handling
17. **subqueries** — EXISTS, IN, scalar subquery support
18. **append** — UNION ALL
19. **trendline** — window AVG with ROWS BETWEEN
20. **parse/grok** — REGEXP_EXTRACT UDF
21. **lookup** — LEFT JOIN + COALESCE

### Phase 6: Long Tail (~80% → ~85%+)
22. **Null handling edge cases** — fix null propagation across all functions
23. **Datetime functions** — date/time specific SQL mappings
24. **JSON functions** — JSON_VALUE, JSON_EXTRACT
25. **Remaining edge cases** — fix failures from earlier iterations

## Key Files

| File | Purpose |
|------|---------|
| `api/src/main/java/org/opensearch/sql/api/PPLToSqlTranspiler.java` | The transpiler (to create) |
| `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` | Test base class (to modify for V4 routing) |
| `scripts/ppl_it_rate.sh` | Test rate measurement |
| `progress.txt` | Iteration log |
| `docs/dev/ppl-v4-sqlnode-research.md` | Command inventory & translation patterns |
| `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java` | PPL parser (read-only reference) |
| `core/src/main/java/org/opensearch/sql/ast/tree/` | AST node classes (read-only reference) |
| `core/src/main/java/org/opensearch/sql/ast/expression/` | AST expression nodes (read-only reference) |
