# PR: Replace 5 Calcite Temp Patches With Clean Minimal Fixes

**Branch:** `fix/calcite-temp-fix-cleanup`
**Base:** `main` (`8113f69d3`)
**Commit:** `cee66a7c8`
**Stats:** 9 files changed, +242 / -12

## Summary

Replaces fragile temporary patches (originally landed in `0f40eadfc` and later reverted)
with production-quality fixes for the Calcite-based unified SQL/PPL query path. Each fix
is the smallest change that addresses the root cause without changing observable behavior
elsewhere.

## Changes

### Fix 1 — FROM-less SELECT returning 0 rows
**File:** `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`

Replace the empty-typed Values with Calcite's canonical `LogicalValues.createOneRow(cluster)`.
The downstream `LogicalProject` strips the dummy `ZERO` column.

**Before:** `SELECT 1` → `{"datarows":[], "total":0}`
**After:** `SELECT 1` → `{"datarows":[[1]], "total":1}`

### Fix 2 — Strip Calcite's NUL separator from response field names
**File:** `core/src/main/java/org/opensearch/sql/executor/analytics/AnalyticsExecutionEngine.java`

Calcite's planner may produce field names like `'ISNULL(lastname)\0name'` when
disambiguating projected fields. Strip everything up to and including the NUL so
the response schema shows only the user-facing alias.

**Before:** `SELECT ISNULL(lastname) AS name` → `{"name":"ISNULL(lastname)\0name"}`
**After:** `SELECT ISNULL(lastname) AS name` → `{"name":"name"}`

### Fix 3 — DatetimeUdtNormalizeRule no longer crashes on aggregates
**File:** `api/src/main/java/org/opensearch/sql/api/spec/datetime/DatetimeUdtNormalizeRule.java`

The previous shuttle normalized `RexCall` return types in aggregate inputs but never
updated the corresponding `AggregateCall.getType()`, causing
`Aggregate.<init>` assertion `aggCall type: VARCHAR != inferred type: TIMESTAMP(9)`.

**Approach:** Special-case `Aggregate` before `super.visit()`. Manually visit children,
then rebuild each `AggregateCall` via `AggregateCall.create(...)` with `null` type so
Calcite re-infers from the new input row type. A `toStdType` helper is extracted to
remove duplication. Uses `agg.copy(...)` which preserves traits and hints.

**Note:** The 13-arg `AggregateCall.create` overload is `@Deprecated` in Calcite 1.41;
it is the only overload that accepts `(groupCount, input, null type)` and triggers
re-inference. Using a non-deprecated overload would require us to pre-compute the type,
defeating the purpose. Documented inline.

### Fix 4 — HAVING / aggregate expression resolution
**Files:**
- `core/src/main/java/org/opensearch/sql/calcite/CalcitePlanContext.java`
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`

The previous patch resolved post-aggregate `AggregateFunction` AST references via
string matching against row-type field names — including a dangerous
case-insensitive **prefix** match that could resolve `SUM(a)` to `SUM(ab)`.

**Approach:** Structural registry.
- `CalcitePlanContext` gains
  `Map<AggregateFunction, Integer> aggregateOutputIndex`.
- `CalciteRelNodeVisitor.visitAggregation` populates it after schema reordering by
  looking up each registered `AggregateFunction.toString()` in the final field list.
- `CalciteRexNodeVisitor.visitAggregateFunction` first does an O(1) lookup keyed on
  the `AggregateFunction`'s **structural identity** (via `@EqualsAndHashCode`, which
  includes DISTINCT and conditions), then falls back to a case-insensitive *exact*
  name match (no prefix matching), and finally throws `IllegalStateException` with
  the available field list if neither matches.

**Fixes:**
- `SELECT abs(MAX(age)) FROM t`
- `SELECT MAX(age) + MIN(age) FROM t`
- `SELECT name FROM t GROUP BY name HAVING COUNT(*) > 1`
- Same aggregate referenced twice (e.g., `SELECT MAX(age), abs(MAX(age)) FROM t`)
- `DISTINCT` aggregates are resolved by structural equality (not by toString).

### Fix 5 — NullOrder NPE in window functions
**File:** `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`

Window functions with `ORDER BY` but no explicit `NULLS FIRST/LAST` produce a
`SortOption` with `nullOrder = null`, which NPE'd at the `switch`.

**Approach:** 4-line null check before the switch, defaulting to `nullsFirst`. This
matches the existing top-level `ORDER BY` default in `CalciteRelNodeVisitor.visitSort`
(where `null nullOrder` falls through to `nullsFirst`), so observable behavior is
consistent across window-function and top-level ordering. The redundant `default`
branch in the switch is removed.

**Why not fix at parser level?** Defaulting `nullOrder` based on `sortOrder`
(`ASC → NULLS FIRST`, `DESC → NULLS LAST`) in the parser cascades into the V1
`AstBuilder`, which conditionally adds an `Argument(argName=nullFirst, ...)` to
`Sort.Field` nodes whenever `nullOrder` is non-null. That broke 12 pre-existing
tests for top-level ORDER BY. Guarding only at the window-function call site avoids
the cascade and keeps the PR scope minimal.

**Fixes:** `SELECT ROW_NUMBER() OVER(ORDER BY age) FROM t`,
`SELECT RANK() OVER(ORDER BY age DESC) FROM t`.

## Tests

| Test class | Coverage |
| --- | --- |
| `CalciteRexNodeVisitorTest` | 3 new tests for `visitAggregateFunction`: registry hit, name-fallback hit, unresolvable throws |
| `AnalyticsExecutionEngineTest` | 2 new tests: NUL prefix stripped, clean names pass through |
| `DatetimeExtensionSqlTest` | 2 new tests: `MAX(datetime_col)`, `MIN(date_col)` plan-level smoke |
| `UnifiedQueryPlannerSqlV2Test` | 2 new tests: `SELECT 1`, `SELECT 1 + 1` plan-level |

Integration tests requiring a running OpenSearch cluster are intentionally not
included in this PR. Existing IT files (`PaginationIT`, `ArithmeticFunctionIT`,
`ConditionalIT`, `WindowFunctionIT`, `AggregationIT`) already cover the
end-to-end behavior and will benefit from these fixes.

## Verification

All targeted unit tests pass:

```
./gradlew :core:test --tests 'org.opensearch.sql.calcite.*' \
          :api:test  --tests 'org.opensearch.sql.api.*' \
          :sql:test
# BUILD SUCCESSFUL — no failures
```

## Risk

- **Fix 1**: Behavior change for FROM-less SELECT (`0 rows → 1 row`). This is
  the documented bug being fixed.
- **Fix 2**: User-visible column names for aliased aggregate/function expressions
  may change from `'ISNULL(x)\0alias'` to `'alias'`. This is the documented bug
  being fixed and aligns with what users specified.
- **Fix 3, 4, 5**: Restore previously-broken plans to working order; no behavior
  change for queries that already worked.

## What was *not* changed

- `ParserUtils.createSortOption` (Fix 5 alternative) — would have caused 12
  pre-existing tests to fail due to V1 AST builder cascading.
- The `@Ignore`'d `UnifiedSqlSpecTest.java` was not modified.
- `RestUnifiedQueryAction` AssertionError catching and "Failed to plan query"
  error-message improvements from the original temp commit are out of scope
  for this PR.
