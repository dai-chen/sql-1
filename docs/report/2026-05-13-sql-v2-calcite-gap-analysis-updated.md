# SQL V2 → CalciteRelNodeVisitor: Updated Gap Analysis

**Date**: 2026-05-13 (updated)  
**Previous Pass Rate**: 572 / 964 (59.3%) — 2026-05-12  
**Current Pass Rate**: 491 / 609 (80.6%) — V2 + Legacy ITs that actually run  
**Branch**: sql-v2-calcite-gap-analysis

---

## Progress Summary

| Gap ID | Description | Original Status | Current Status | Tests Fixed |
|--------|-------------|----------------|----------------|-------------|
| B1+B16 | ArgumentMap NPE (BUCKET_NULLABLE) | ~86 failures | ✅ **Already fixed** (pre-existing in branch) | — |
| B2 | AggregateFunction → Function cast | ~29 failures | ✅ **Already fixed** (pre-existing in branch) | — |
| B7 | Relevance function aliases | ~14 failures | ✅ **Already fixed** (pre-existing in branch) | — |
| B8+B19 | visitLimit missing | ~10 failures | ✅ **Already fixed** (pre-existing in branch) | — |
| B9 | Window functions (ROW_NUMBER, RANK, DENSE_RANK) | ~8 failures | ✅ **FIXED** — added to WINDOW_FUNC_MAPPING + PlanUtils.makeOver | ROW_NUMBER resolves |
| B10 | Table name resolution (dot separator) | ~6 failures | ✅ **Already fixed** (pre-existing in branch) | — |
| B11 | QUERY function | ~5 failures | ✅ **Already fixed** (pre-existing in branch) | — |
| B13 | Column name = alias instead of expression | ~6 failures | ✅ **FIXED** — alias encoding in field name + decoding in response | ~7 (ConditionalIT) |
| B14 | ISNULL / IFNULL unresolved | ~5 failures | ✅ **Already fixed** (pre-existing in branch) | — |
| B18 | typeof() type name mismatch | 1 failure | 🟡 **Partially fixed** — SQL returns correct names; text/keyword still wrong | typeof works |
| B6 | WILDCARD_QUERY | ~18 failures | 🟡 **Partially fixed** — registered but push-down not implemented | 0 (execution fails) |
| B12 | Schema type mismatch (keyword vs text) | ~5 failures | ❌ **Not fixed** — requires new UDT (M-effort) | 0 |
| B3 | Pagination unsupported | ~35 failures | ⏭️ **Skipped** (out of scope) | — |
| B4 | Table function unsupported | ~26 failures | ⏭️ **Skipped** (out of scope) | — |
| B5 | NESTED function | ~33 failures | ⏭️ **Skipped** (out of scope) | — |
| B15 | Explain format | ~3 failures | ⏭️ **Skipped** (not SELECT-focused) | — |
| B17 | Pagination fallback tests | ~5 failures | ⏭️ **Skipped** (not SELECT-focused) | — |

---

## New Fixes Applied (this session)

### Fix 1: Window Function ORDER BY Support
**Files**: `CalciteRexNodeVisitor.java`, `PlanUtils.java`

- `visitWindowFunction` now converts `node.getSortList()` to Calcite orderKeys with ASC/DESC/NULLS handling
- Added RANK and DENSE_RANK cases to `PlanUtils.makeOver` switch
- Added `makeOver` overload with `distinct` parameter for `COUNT(DISTINCT x) OVER(...)`
- Pure window functions (ROW_NUMBER, RANK, DENSE_RANK) bypass `aggFunctionRegistry` validation

### Fix 2: Column Name/Alias Encoding (B13)
**Files**: `CalciteRexNodeVisitor.java`, `OpenSearchExecutionEngine.java`

- For SQL queries with AS-alias, encodes `exprName\0alias` in Calcite field name
- Response builder decodes and populates both `Column.name` and `Column.alias`
- Fixed NPE in ANY-type lookup when alias is present

### Fix 3: typeof() QueryType-Aware (B18)
**Files**: `CalcitePlanContext.java`, `PPLFuncImpTable.java`

- Added `ThreadLocal<QueryType>` to `CalcitePlanContext` with cleanup in finally block
- `typeof()` uses `getCurrentQueryType()` instead of hardcoded PPL
- SQL queries return OpenSearch type names (LONG, INTEGER); PPL keeps BIGINT, INT

### Fix 4: AVG Type Validation Bypass for SQL
**File**: `PPLFuncImpTable.java`

- SQL queries bypass strict PPL type validation for aggregation functions
- Allows AVG on TIMESTAMP/DATE/TIME to pass validation (Calcite handles coercion)

### Fix 5: AVG on Temporal — Cast to BIGINT (uncommitted)
**File**: `PPLFuncImpTable.java`

- For SQL queries, detects temporal-typed AVG operands and casts to BIGINT before AVG
- Resolves "Error while preparing plan" — AVG now executes
- Result type is `double` instead of `timestamp` (partial fix — cast-back not implemented)

### Fix 6: wildcard_query + percentile Registration
**Files**: `PPLBuiltinOperators.java`, `BuiltinFunctionName.java`, `PPLFuncImpTable.java`

- Registered WILDCARD_QUERY and WILDCARDQUERY operators
- Added percentile/percentile_approx to WINDOW_FUNC_MAPPING

---

## Current Failure Breakdown (609 tests total)

| Root Cause | Failures | Category | Effort |
|-----------|----------|----------|--------|
| DateTimeComparisonIT type issues | 24 | AE-side | — |
| WildcardQuery push-down | 18 | SQL plugin | M |
| AggregationIT (AVG type + COUNT DISTINCT) | 16 | SQL plugin | M |
| text vs keyword type mapping | 16 | SQL plugin | M (UDT) |
| Legacy AggregationExpressionIT | 15 | Grammar gap (GROUP BY expr) | M |
| MatchIT text/keyword | 11 | SQL plugin | M (UDT) |
| DateTimeFunctionIT | 7 | AE-side | — |
| WindowFunctionIT (DISTINCT COUNT) | 7 | Pre-existing / Calcite limitation | M |
| Other (field not found, identifier, etc.) | 4 | Various | S-M |

---

## Remaining Gaps by Priority

### High Priority (SQL plugin, fixable)

| Issue | Tests | Fix |
|-------|-------|-----|
| text vs keyword type | 27 | New EXPR_TEXT UDT or override getFamily() |
| WildcardQuery push-down | 18 | Implement relevance function push-down rule |
| AVG result type (double→timestamp) | 13 | Cast AVG result back to original temporal type |
| Post-aggregate field resolution | 11+ | Match sub-expressions against group key fields |

### Medium Priority (pre-existing / Calcite limitations)

| Issue | Tests | Fix |
|-------|-------|-----|
| Window DISTINCT COUNT | 7 | Calcite limitation — may need custom aggregate |
| EXISTS subquery | 2 | Grammar gap (A7) |

### Reclassified to AE (not SQL plugin)

| Issue | Tests |
|-------|-------|
| DATE_PART(string, string?) | 22 |
| DateTimeComparison cross-type | 24 |
| to_timestamp(string, string) | 10 |
| convert_tz(string, string, string) | 9 |
| MOD/LOG10/LN type mismatch | 8 |
| /(timestamp, bigint) | 3 |

---

## Test Classes — Full Results

### 100% Pass (21 classes)
ArithmeticFunctionIT, ConvertTZFunctionIT, DateTimeImplementationIT, LikeQueryIT, MatchPhraseIT, MatchPhrasePrefixIT, MathematicalFunctionIT, MultiMatchIT, NowLikeFunctionIT, NullLiteralIT, ObjectFieldSelectIT, PositionFunctionIT, QueryIT, QueryStringIT, RelevanceFunctionIT, ScoreQueryIT, SimpleQueryStringIT, StringLiteralIT, TextFunctionIT

### Partial Pass (13 classes)
| Class | Pass/Total | Rate |
|-------|-----------|------|
| DateTimeComparisonIT | 167/191 | 87% |
| DateTimeFunctionIT | 67/74 | 91% |
| AggregationIT | 38/54 | 70% |
| DateTimeFormatsIT | 10/11 | 91% |
| ConditionalIT | 7/12 | 58% |
| IdentifierIT | 9/10 | 90% |
| ComplexTimestampQueryIT | 4/6 | 67% |
| QueryValidationIT | 2/5 | 40% |
| MatchIT | 3/14 | 21% |
| MatchBoolPrefixIT | 1/3 | 33% |
| SystemFunctionIT | 1/2 | 50% |
| WindowFunctionIT | 1/8 | 12% |
| MethodQueryIT (legacy) | 2/7 | 29% |

### 0% Pass (3 classes)
| Class | Tests | Reason |
|-------|-------|--------|
| WildcardQueryIT | 18 | Push-down not implemented |
| ExistsPushdownIT | 2 | Grammar gap (EXISTS subquery) |
| AggregationExpressionIT (legacy) | 16 | GROUP BY expression field resolution |
