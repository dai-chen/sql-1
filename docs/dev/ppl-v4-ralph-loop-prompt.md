# PPL V4 Ralph Loop Prompt

Copy-paste this prompt to kick off each iteration. The agent reads context, picks one task, implements it, measures, and logs progress.

---

## The Prompt

```
Read these files first:
- docs/dev/ppl-v4-loop.md (iteration plan and test surface)
- docs/dev/ppl-v4-sqlnode-research.md (command inventory and translation patterns)
- progress.txt (what's been done so far)

Goal: increase CalcitePPL*IT pass rate to ≥85% for the PPL V4 transpiler branch.

Architecture:
  PPL string → PPL Parser → AST → PPLToSqlTranspiler.java → SQL string → /_plugins/_sql endpoint → results

Rules:
1. Run `scripts/ppl_it_rate.sh` to get the current baseline rate. If this is the first iteration and no V4 code exists yet, note that the baseline is 0% (all tests use the existing PPL endpoint, V4 wiring doesn't exist yet).
2. Read progress.txt to see what's been done. Pick exactly ONE bounded, highest-impact unfinished task from docs/dev/ppl-v4-loop.md's iteration order.
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
6. If the rate does NOT improve, explain why and propose the next better slice. Do not retry the same approach — pivot.
7. Do NOT broaden scope. Do NOT attempt multiple commands in one iteration. One bounded task per loop.

Key reference files (read-only, do not modify):
- ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java — PPL parser
- core/src/main/java/org/opensearch/sql/ast/tree/ — AST tree nodes
- core/src/main/java/org/opensearch/sql/ast/expression/ — AST expression nodes
- core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java — visitor base

Files you WILL create/modify:
- api/src/main/java/org/opensearch/sql/api/PPLToSqlTranspiler.java — the transpiler
- integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java — V4 test routing (add V4 path, don't break existing)
- progress.txt — iteration log

Lessons learned (avoid these pitfalls):
- UDT types (EXPR_TIMESTAMP etc.) are a dead end — transpile to SQL strings, the SQL endpoint handles types.
- V3 UDFs (ImplementorUDF/PPLFuncImpTable) are incompatible — map PPL functions to SQL directly.
- SELECT * includes metadata fields (_id, _index, _score etc.) — the SQL endpoint already excludes them.
- Eval aliases need subquery wrapping so downstream pipes can reference them.
- Use COUNT(*) not COUNT() — Calcite rejects zero-argument COUNT.
- PPL count() AST uses Literal(1) not AllFields — check for both.
- The PPL parser always wraps plans in Project(AllFields) — treat as passthrough.
- Join aliases (left=l right=r) must appear in SQL output.

Now: read the files, determine baseline, pick one task, implement it, measure, log. Go.
```

---

## Quick-Resume Prompt

If a session was interrupted, use this shorter version:

```
Read progress.txt and docs/dev/ppl-v4-loop.md. Resume the PPL V4 Ralph loop from where we left off. Run scripts/ppl_it_rate.sh for current rate, pick the next highest-impact task, implement it, measure, update progress.txt. One task only.
```

---

## Targeted Fix Prompt

When you know exactly which test class to fix:

```
Read progress.txt. Current PPL V4 rate is <X>%. Focus on CalcitePPL<ClassName>IT — it has <N> tests and <M> are failing. Read the failing test methods, understand what PPL commands/functions they exercise, implement the missing transpiler support, run the tests, update progress.txt.
```
