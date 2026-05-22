# PPL scalarWindowFunctionName Grammar Orphan Analysis

## Grammar Location
`ppl/src/main/antlr/OpenSearchPPLParser.g4:832-844`

## Per-Function Wiring Table

| Function | Grammar token? | AstExpressionBuilder? | BuiltinFunctionName enum? | WINDOW_FUNC_MAPPING? | V2 runtime impl? | Calcite makeOver case? | Tests (eventstats/streamstats)? | Status |
|---|---|---|---|---|---|---|---|---|
| ROW_NUMBER | YES (Lexer:331) | YES generic (AstExpressionBuilder.java:882) | YES (BuiltinFunctionName.java:294) | NO | YES (WindowFunctions.java:42) | YES (PlanUtils.java:213) | NO | **ORPHAN** (not in WINDOW_FUNC_MAPPING) |
| RANK | YES (Lexer:332) | YES generic | YES (BuiltinFunctionName.java:296) | NO | YES (WindowFunctions.java:43) | NO (falls to default) | NO | **ORPHAN** (not in WINDOW_FUNC_MAPPING) |
| DENSE_RANK | YES (Lexer:333) | YES generic | YES (BuiltinFunctionName.java:297) | NO | YES (WindowFunctions.java:44) | NO (falls to default) | NO | **ORPHAN** (not in WINDOW_FUNC_MAPPING) |
| PERCENT_RANK | YES (Lexer:334) | YES generic | NO | NO | NO | NO | NO | **ORPHAN** (no enum, no mapping, no runtime) |
| CUME_DIST | YES (Lexer:335) | YES generic | NO | NO | NO | NO | NO | **ORPHAN** (no enum, no mapping, no runtime) |
| FIRST | YES (Lexer:336) | YES generic | YES (BuiltinFunctionName.java:236) | NO (only in AGGREGATION_FUNC_MAPPING:404) | NO (aggregator only) | NO | NO | **ORPHAN** (not in WINDOW_FUNC_MAPPING) |
| LAST | YES (Lexer:337) | YES generic | YES (BuiltinFunctionName.java:237) | NO (only in AGGREGATION_FUNC_MAPPING:405) | NO (aggregator only) | NO | NO | **ORPHAN** (not in WINDOW_FUNC_MAPPING) |
| NTH | YES (Lexer:338) | YES generic | NO (NTH_VALUE exists at :295 but maps to "nth_value" not "nth") | NO | NO | NO (makeOver has NTH_VALUE case but lookup fails before reaching it) | NO | **ORPHAN** (name mismatch: grammar says "NTH", enum says "nth_value") |
| NTILE | YES (Lexer:339) | YES generic | NO | NO | NO | NO | NO | **ORPHAN** (no enum, no mapping, no runtime) |
| DISTINCT_COUNT | YES (Lexer:295) | YES generic | NO (DISTINCT_COUNT_APPROX at :228) | YES → DISTINCT_COUNT_APPROX (BuiltinFunctionName.java:425) | YES (via aggregation) | YES (default case) | YES (CalcitePPLEventstatsIT.java:688) | **WIRED** |
| DC | YES (Lexer:328) | YES generic | NO (maps to DISTINCT_COUNT_APPROX) | YES → DISTINCT_COUNT_APPROX (BuiltinFunctionName.java:424) | YES (via aggregation) | YES (default case) | YES (CalcitePPLEventstatsIT.java:637) | **WIRED** |

## Critical Path Analysis

The Calcite path for eventstats/streamstats resolves window functions via:
1. `AstExpressionBuilder.visitWindowFunction` (ppl/.../AstExpressionBuilder.java:882) — generic, accepts ALL tokens
2. `CalciteRexNodeVisitor.visitWindowFunction` (core/.../CalciteRexNodeVisitor.java:565) — calls `BuiltinFunctionName.ofWindowFunction(name)`
3. `ofWindowFunction` (BuiltinFunctionName.java:440) — looks up in `WINDOW_FUNC_MAPPING`
4. If not found → `orElseThrow` → `UnsupportedOperationException: "Unexpected window function: <name>"`

## Failure Behavior

For the 9 orphaned functions, a user running e.g.:
```
source=index | eventstats row_number() as rn
```
Would get: `UnsupportedOperationException: Unexpected window function: ROW_NUMBER`

The parser accepts the query (grammar is valid), the AST is built, but the Calcite codegen fails at function resolution.

## Summary

- **WIRED (2/11)**: DISTINCT_COUNT, DC — both map to DISTINCT_COUNT_APPROX in WINDOW_FUNC_MAPPING
- **ORPHAN (9/11)**: ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, FIRST, LAST, NTH, NTILE

## Root Cause

The developer who added the `scalarWindowFunctionName` grammar rule (for eventstats/streamstats) defined tokens for all common SQL window functions but only wired up the aggregation-style ones (dc, distinct_count) via WINDOW_FUNC_MAPPING. The ranking/positional functions (ROW_NUMBER, RANK, etc.) have V2 runtime implementations registered in `WindowFunctions.java` for the older SQL window function path, but were never added to `WINDOW_FUNC_MAPPING` which is the gate for the Calcite-based eventstats/streamstats path.

This appears to be **intentional stubbing for future work** rather than a forgotten implementation — the grammar was designed to be forward-compatible, but the backend wiring was only completed for the aggregation functions that eventstats/streamstats actually needed at launch.
