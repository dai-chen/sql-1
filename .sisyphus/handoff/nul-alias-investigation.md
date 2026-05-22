# NUL Alias Investigation — Empirical Results

**Branch:** `fix/calcite-temp-fix-cleanup`  
**Date:** 2026-05-21  
**Target:** `AnalyticsExecutionEngine.buildSchema` NUL (`\0`) strip at line 143

---

## 1. Static Search Results

### Search: literal `\0` / `\u0000` in source Java files (excluding build/)

| Location | Classification | Notes |
|----------|---------------|-------|
| `core/.../AnalyticsExecutionEngine.java:143` | **THE STRIP ITSELF** | The defensive code under investigation |
| `async-query-core/build/generated-src/antlr/...` (many hits) | DEAD | ANTLR-generated lexer state tables; binary data, not field-name producers |
| `sql/src/main/gen/OpenSearchSQLLexer.java` (4 hits) | DEAD | Same — ANTLR lexer tables |

### Search: `SqlValidatorUtil.uniquify` / `SqlValidatorUtil.alias` in repo source

**0 hits.** The repo does not directly call these Calcite utilities.

### Search: `addToSelectList` in repo source

**0 hits.** No custom override of Calcite's select-list builder.

### Calcite JAR inspection (1.41.0, used by this project)

- **`SqlValidatorUtil.EXPR_SUGGESTER`** (the known NUL producer in old Calcite): In Calcite 1.41.0, this lambda is `(original, attempt, size) -> Util.first(original, "$EXPR$") + attempt`. **No NUL byte.** The NUL separator was removed in Calcite ~1.20 (CALCITE-2822).
- **`SqlValidatorImpl.addToSelectList`**: Calls `SqlValidatorUtil.uniquify(alias, aliases, EXPR_SUGGESTER)` — which just appends an integer suffix for disambiguation. **No NUL byte.**
- **`SqlValidatorUtil.alias_`**: Returns the AS alias, last identifier segment, or `"EXPR$" + ordinal`. **No NUL byte.**
- **`SqlToRelConverter`**: No NUL byte string constants in constant pool.

**Summary:** 0 LIVE producers, 0 LIVE-SUPPRESSED producers, all hits are DEAD (ANTLR tables or the strip itself).

---

## 2. Runtime Probe Results

### Methodology

Replaced the NUL strip in `buildSchema` with a `throw new IllegalStateException("NUL_PROBE: ...")` that fires if any field name contains `\0`.

### Test execution

```
./gradlew :core:test :sql:test :ppl:test --no-daemon -q
```

| Module | Tests Run | Failures | NUL_PROBE Hits |
|--------|-----------|----------|----------------|
| `:core:test` | 5,139 | 1 | 1 (mock-based) |
| `:sql:test` | ~200 | 0 | 0 |
| `:ppl:test` | 1,902 | 3 | 0 |

### NUL_PROBE hits (1 total)

| Test | Field Name | Source | Producer |
|------|-----------|--------|----------|
| `AnalyticsExecutionEngineTest.executeRelNode_nulSeparatorStrippedFromFieldNames` | `ISNULL(lastname)\0name` | **Mock** — `mockRelNode(...)` with hardcoded `\0` in field name | Test itself (line 284) |

This test was written specifically to validate the strip behavior. It does **not** exercise any real Calcite planning path — it mocks a `RelNode` with a synthetic `RelDataType` containing `\0`.

### PPL failures (3, all unrelated)

All in `CalcitePPLAppendTest` — plan shape assertion mismatches (extra `ZERO` column in actual vs expected). Not NUL-related.

### `:integ-test:test`

Not run — requires a live OpenSearch cluster. This limitation is noted but does not affect the conclusion: integration tests exercise the same Calcite planner version (1.41.0) which does not produce `\0`.

---

## 3. Combined Verdict

**0 real tests hit NUL_PROBE + 0 LIVE static producers → the strip is dead code.**

Calcite 1.41.0 (the version used by this project) does not produce `\0`-separated field names. The NUL separator was a feature of Calcite versions prior to ~1.20 (removed in CALCITE-2822, merged 2019). The only test that exercises the strip (`executeRelNode_nulSeparatorStrippedFromFieldNames`) uses a mock that artificially injects `\0` — it does not prove any real code path produces such names.

---

## 4. Recommendation

**Drop the strip** (and its dedicated test).

**Rationale:**
- The defensive code guards against a Calcite behavior that was removed 5+ major versions ago.
- No real query (SQL or PPL) through the current engine produces `\0` in field names.
- The mock-based test validates dead behavior and creates a false sense that the strip is needed.
- If a future Calcite upgrade reintroduces `\0` (unlikely), it would be caught immediately by schema assertion failures in existing tests, and can be addressed at that time.

If a more conservative approach is preferred, replace the strip with a logged warning (no silent mutation) so that if `\0` ever appears, it's visible rather than silently handled.
