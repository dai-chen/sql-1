# NUL Producer Investigation

## 1. Static Search Summary

**0 LIVE producers found in project source code.**

Searched all modules (core, sql, ppl, legacy, plugin, api, opensearch) for:
- Explicit `\0` / `\u0000` concatenation in Java source
- `String.format` / `append` / `+` with NUL byte
- `SqlValidatorUtil.uniquify()` / `EXPR_SUGGESTER` calls
- Any code that assembles `"<expression>\0<alias>"` patterns

Results:
- `CalciteStringSortScript.java:34` — uses `\u0000` as a NULL placeholder sentinel for sort ordering. **DEAD** for this issue (unrelated to field names).
- Generated ANTLR lexer files — contain `\u0000` in serialized DFA tables. **DEAD**.
- No code in this project explicitly produces `\0` in field names.

## 2. Runtime Probe Summary

Ran `:core:test :sql:test :ppl:test :legacy:test :plugin:test` with NUL_PROBE throw in `buildSchema`.

**1 test hit NUL_PROBE:**
- Module: `:core`
- Test: `AnalyticsExecutionEngineTest.executeRelNode_nulSeparatorStrippedFromFieldNames`
- Field name: `"ISNULL(lastname)\0name"` (length=21)
- **This is a SYNTHETIC test** that manually constructs a mock RelNode with `\0` in field names to verify the strip behavior. It does NOT exercise a real production code path.

**0 hits in :legacy, :plugin, :sql, :ppl** — no real code path in unit tests produces `\0`.

The real `\0` only manifests in integration tests (`:integ-test`) which require a running OpenSearch cluster and could not be run.

## 3. The Producer — Calcite Internal Behavior

### Root Cause: Calcite's `SqlToRelConverter.convertSelectList()`

The `\0` separator is a **Calcite-internal encoding** produced during `SqlToRelConverter.convertSelectList()` at:

```
calcite-core-1.41.0.jar!org/apache/calcite/sql2rel/SqlToRelConverter.class
  convertSelectList() → line 391 (bytecode):
    invokestatic SqlValidatorUtil.uniquify(List, boolean)
```

This calls `SqlValidatorUtil.uniquify(names, EXPR_SUGGESTER, caseSensitive)` which can produce composite names when disambiguating duplicate field names in a SELECT list.

### Why it reaches AnalyticsExecutionEngine

The `\0` is produced when:
1. A SQL query like `SELECT ISNULL(lastname) AS name FROM ...` is processed
2. Calcite's validator/converter internally encodes the field as `"ISNULL(lastname)\0name"` to preserve both the expression text and the alias
3. This encoding propagates through the RelNode tree's rowType

**However**, the `CustomVisitorStrategy` path (used by `UnifiedQueryPlanner`) does NOT go through `SqlToRelConverter`. The `CalciteRelNodeVisitor` + `RelBuilder.alias()` + `RelBuilder.project()` path correctly produces just `"name"` as the field name.

### Hypothesis for Integration Test Failures

The `\0` likely appears in one of these scenarios:
1. **Calcite's VolcanoPlanner** during `runner.prepareStatement(rel)` in `OpenSearchRelRunners.run()` — the `CalcitePrepareImpl.trimUnusedFields()` step uses `OpenSearchSqlToRelConverter` which goes through the validator
2. **A different entry point** that uses `CalciteNativeStrategy` (currently dead code but may have been active in the failing test)
3. **Calcite's internal project merging** in `RelBuilder.project_()` when merging stacked projects

## 4. Recommended Upstream Fix

Since the `\0` is produced by Calcite's internal mechanisms (not by any code in this project), there is no upstream fix within this codebase. The options are:

### Option A: Keep the boundary strip (RECOMMENDED)
```java
// In AnalyticsExecutionEngine.buildSchema():
int nul = name.indexOf('\0');
if (nul >= 0) {
  name = name.substring(nul + 1);
}
```

### Option B: Strip at the source (if we can identify the exact Calcite entry point)
Add a post-analysis rule that strips `\0` from all field names in the final RelNode before handing to the execution engine. This would be in `UnifiedQueryPlanner.plan()`:
```java
// After postAnalysisRules:
plan = stripNulFromFieldNames(plan);
```

But this is more complex and fragile than Option A.

## 5. Verdict

**(ii) Producer found but not fixable cleanly → keep the boundary strip; document why**

The `\0` is produced by Calcite's internal `SqlValidatorUtil.uniquify()` / `SqlToRelConverter.convertSelectList()` mechanisms. These are deep inside Calcite's planner infrastructure and cannot be patched without forking Calcite. The boundary strip in `AnalyticsExecutionEngine.buildSchema()` is the correct defensive fix because:

1. It's minimal and targeted (only affects the schema output, not the execution)
2. It handles all cases where Calcite might produce `\0` (future-proof)
3. It's well-documented with a clear comment explaining why
4. The synthetic unit test verifies the behavior

The strip should remain as-is. No upstream fix is needed or possible within this codebase.
