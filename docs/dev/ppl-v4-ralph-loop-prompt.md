# PPL V4 — Ralph Loop

This document is the single source of truth for the PPL V4 Ralph loop: iteration plan, test surface, agent rules, and prompt templates.

---

## Architecture

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
| `api/src/main/java/org/opensearch/sql/api/PPLToSqlTranspiler.java` | The transpiler (to create/modify) |
| `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` | Test base class (to modify for V4 routing) |
| `scripts/ppl_it_rate.sh` | Test rate measurement |
| `progress.txt` | Iteration log |
| `docs/dev/ppl-v4-sqlnode-research.md` | Command inventory & translation patterns |
| `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java` | PPL parser (read-only reference) |
| `core/src/main/java/org/opensearch/sql/ast/tree/` | AST node classes (read-only reference) |
| `core/src/main/java/org/opensearch/sql/ast/expression/` | AST expression nodes (read-only reference) |
| `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java` | Visitor base (read-only reference) |

---

## The Prompt

Copy-paste this prompt to kick off each iteration. The agent reads context, picks one task, implements it, measures, and logs progress.

```
Read these files first:
- docs/dev/ppl-v4-ralph-loop-prompt.md (iteration plan, test surface, rules, AND the "Base Engine Capabilities" section — critical for knowing what SQL features the engine supports)
- docs/dev/ppl-v4-sqlnode-research.md (command inventory and translation patterns)
- progress.txt (what's been done so far)

Goal: increase CalcitePPL*IT pass rate to ≥85% for the PPL V4 transpiler branch.

IMPORTANT: The SQL engine has been enhanced with a simplified OpenSearch schema (standard SQL types, no UDT) and SELECT * EXCEPT/REPLACE support. Read the "Base Engine Capabilities" section in the ralph-loop-prompt doc before implementing. Use EXCEPT/REPLACE instead of workarounds. Date/time functions should work on date fields now.

Rules:
1. Run `scripts/ppl_it_rate.sh` to get the current baseline rate. If this is the first iteration and no V4 code exists yet, note that the baseline is 0% (all tests use the existing PPL endpoint, V4 wiring doesn't exist yet).
2. Read progress.txt to see what's been done. Pick exactly ONE bounded, highest-impact unfinished task from the Recommended Iteration Order section above.
3. Implement ONLY that one task with minimal code changes. Follow these constraints:
   - PPLToSqlTranspiler.java is a pure function: String transpile(String ppl). No OpenSearch dependencies, no V3 imports.
   - Each PPL command visitor method: 5-20 lines. Total transpiler: under 500 lines.
   - Function mappings go in a single static map, not scattered switch statements.
   - SQL output must be human-readable.
   - When pipe stages conflict (WHERE after GROUP BY), auto-wrap as subquery.
   - No fallback to V3/V2. Unsupported commands fail explicitly with "unsupported in V4".
   - Do NOT modify any existing CalcitePPL*IT test file.
4. Run the relevant tests to verify your change works:
   - For targeted validation: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalcitePPL<RelevantClass>IT" -Dppl.engine.v4=true -Dtests.calcite.pushdown.enabled=false --continue`
   - Then run `scripts/ppl_it_rate.sh` for the overall rate.
5. Update progress.txt with a new iteration entry:
   ```
   ### Iteration N — <task name>
   - Date: <today>
   - Baseline rate: <rate before this iteration>
   - Task: <what you did>
   - Files changed: <list>
   - New rate: <rate after>
   - Tests fixed: <count and examples>
   - Blockers: <any issues hit>
   - Next recommended task: <what to do next iteration>
   ```
6. Commit the iteration: `git add -A && git commit -m "V4 Iteration N: <task name> — rate X% → Y%"` using the actual values from the progress entry you just wrote. Local commit only, do not push.
7. If the rate does NOT improve, explain why and propose the next better slice. Do not retry the same approach — pivot.
8. Do NOT broaden scope. Do NOT attempt multiple commands in one iteration. One bounded task per loop.

Now: read the files, determine baseline, pick one task, implement it, measure, log, commit. Go.
```

---

## Base Engine Capabilities (enhanced after Iteration 23, outside the Ralph loop)

After Iteration 23, the Calcite SQL engine behind `/_plugins/_sql` was enhanced in a separate side project (`poc/extend-calcite-sql-select` branch, now rebased under this branch). **This work is already done — do NOT attempt to modify the engine. The Ralph loop's job is purely transpiler translation work.** Just use these new capabilities when generating SQL:

### Simplified OpenSearch Schema (no UDT)
- OpenSearch date/time fields are now mapped to **standard SQL TIMESTAMP/DATE/TIME** — NOT UDT EXPR_TIMESTAMP/EXPR_DATE/EXPR_TIME
- `SELECT *` on indexes with date fields **works** — no more "need to implement EXPR_TIMESTAMP" errors
- Standard SQL functions like `EXTRACT(YEAR FROM field)`, `YEAR(field)`, `DAYOFWEEK(field)` work on date fields
- OpenSearch metadata fields (`_id`, `_index`, `_score`, `_routing`) are **excluded from schema** — `SELECT *` returns only user data columns

### SELECT * EXCEPT / REPLACE
- `SELECT * EXCEPT(col1, col2)` — exclude columns from wildcard expansion
- `SELECT * REPLACE(expr AS col)` — replace a column's value in wildcard expansion
- These enable clean transpilation of: `fields -`, eval column overriding, rename, fillnull all-fields, lookup REPLACE/APPEND

### What this means for the transpiler
- **Date/time functions should now work** — generate standard SQL (EXTRACT, YEAR, etc.) and trust the engine handles them
- **SELECT * is safe** — no metadata fields, no UDT expansion errors
- **Column exclusion/replacement** — generate EXCEPT/REPLACE in SQL output instead of workarounds (temp aliases, _RENAME_MAP comments, computedColumns maps)
- **Previous workarounds may need cleanup** — PPLIntegTestCase metadata stripping, type normalization may be partially redundant now
- **Re-measure baseline before next iteration** — many previously-blocked tests may now pass or have different failure modes

---

## Lessons Learned

Avoid these pitfalls:
- V3 UDFs (ImplementorUDF/PPLFuncImpTable) are incompatible — map PPL functions to SQL directly.
- Eval aliases need subquery wrapping so downstream pipes can reference them.
- Use COUNT(*) not COUNT() — Calcite rejects zero-argument COUNT.
- PPL count() AST uses Literal(1) not AllFields — check for both.
- The PPL parser always wraps plans in Project(AllFields) — treat as passthrough.
- Join aliases (left=l right=r) must appear in SQL output.
- Use `SELECT * EXCEPT(col)` for fields- exclusion instead of manual column enumeration.
- Use `SELECT * REPLACE(expr AS col)` for eval column overriding instead of temp aliases.

---

## Quick-Resume Prompt

If a session was interrupted, use this shorter version:

```
Read docs/dev/ppl-v4-ralph-loop-prompt.md and progress.txt. Resume the PPL V4 Ralph loop from where we left off. Run scripts/ppl_it_rate.sh for current rate, pick the next highest-impact task, implement it, measure, update progress.txt, commit. One task only.
```

---

## Targeted Fix Prompt

When you know exactly which test class to fix:

```
Read docs/dev/ppl-v4-ralph-loop-prompt.md and progress.txt. Current PPL V4 rate is <X>%. Focus on CalcitePPL<ClassName>IT — it has <N> tests and <M> are failing. Read the failing test methods, understand what PPL commands/functions they exercise, implement the missing transpiler support, run the tests, update progress.txt, commit.
```
