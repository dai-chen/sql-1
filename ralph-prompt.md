# Ralph Agent Instructions — PPL-to-SqlNode Converter Migration

You are an autonomous coding agent migrating the PPL-to-SQL translation from a string-based transpiler to a SqlNode-based converter.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** section FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt` — these describe changes made outside the loop that you must account for
4. Read the design doc at `docs/dev/2026-03-10-unified-sql-ppl-v4-design.md` for architecture context
5. Read the implementation plan at `docs/dev/2026-03-10-impl-plan-2.1-2.2.md` for task details
6. Check you're on the correct branch specified in `prd.json` field `branchName`
7. Pick the **highest priority** user story where `passes: false`
8. Implement that single user story
9. Run build: `./gradlew :api:compileJava :api:compileTestJava 2>&1 | tail -20`
10. Run tests: `./gradlew :api:test 2>&1 | tail -30`
11. If checks pass, commit ALL changes with message: `feat: [{story.id}] {story.title}`
12. Push the commit: `git push`
13. Update `prd.json` — set `passes: true` for the completed story
14. Append your progress to `progress.txt`
15. If you discover reusable patterns, consolidate them into the Codebase Patterns section at the top of `progress.txt`

## Key Architecture Context

**Two-tier converter:**
- `PPLToSqlNodeConverter` — base, no schema, handles static commands (where, sort, head, eval, stats, dedup, join, etc.)
- `DynamicPPLToSqlNodeConverter extends PPLToSqlNodeConverter` — adds schema-dependent commands (fields -, wildcard rename, fillnull all-fields)

**SqlNode DSL:** Use a thin utility class (`SqlNodeDSL.java`) with static factory methods wrapping Calcite SqlNode constructors. Each PPL command's translation should read as a declarative SQL query shape.

**Pipe chaining:** Each `visitXxx` returns a `SqlSelect`. The next command wraps it as a subquery: `SELECT ... FROM (previous_select) AS _tN`.

**PPLTypeCoercion:** Extends `TypeCoercionImpl`, registered via `TypeCoercionFactory` in `UnifiedQueryContext`. Uses `MYSQL_5` conformance as base. Only needs to add DATE+TIME→TIMESTAMP on top of Calcite defaults.

**Reference code:**
- `api/src/main/java/org/opensearch/sql/api/PPLToSqlTranspiler.java` — the string-based transpiler being replaced. Use as reference for FUNC_MAP, command semantics, and edge cases.
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java` — V3 RelNode-based visitor. Reference for command semantics but DO NOT copy its complexity.
- `core/src/main/java/org/opensearch/sql/expression/function/CoercionUtils.java` — PPL coercion rules reference.

**Regression baseline:** `docs/dev/baseline-string-transpiler-failures.txt` contains the tests that fail with the string transpiler. The new converter must not regress — every test that passed before must still pass.

## Key Files

| File | Purpose |
|------|---------|
| `api/src/main/java/org/opensearch/sql/calcite/utils/SqlNodeDSL.java` | SqlNode builder DSL (new) |
| `api/src/main/java/org/opensearch/sql/api/PPLToSqlNodeConverter.java` | Base converter (new) |
| `api/src/main/java/org/opensearch/sql/api/DynamicPPLToSqlNodeConverter.java` | Schema-aware subclass (new) |
| `core/src/main/java/org/opensearch/sql/calcite/type/PPLTypeCoercion.java` | Custom TypeCoercion (new) |
| `api/src/main/java/org/opensearch/sql/api/UnifiedQueryContext.java` | Register TypeCoercionFactory here |
| `api/src/main/java/org/opensearch/sql/api/PPLToSqlTranspiler.java` | Reference — the old transpiler |
| `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` | V4 test routing |
| `scripts/ppl_it_rate.sh` | Pass rate measurement script |

## Progress Report Format

APPEND to progress.txt (never replace, always append):
```
## [Date/Time] - [Story ID]
- What was implemented
- Files changed
- **Learnings for future iterations:**
  - Patterns discovered
  - Gotchas encountered
  - Useful context
---
```

## Human Intervention Notes

If you see a `## Human Intervention` section in `progress.txt`, a human made changes between rounds. Read it carefully — it explains what was changed and why. Account for these changes in your implementation. Do not undo or conflict with human changes.

## Quality Requirements

- ALL commits must pass build and tests
- Do NOT commit broken code
- Keep changes focused and minimal — write only the code needed
- Follow the SqlNode DSL pattern — declarative, not imperative
- Never delete or skip tests to make build pass
- Use PPLToSqlTranspiler as reference for command semantics and edge cases

## Failure Handling

If build or tests fail after 3 attempts at fixing:
1. Record the failure details in the story's `notes` field in prd.json
2. Leave `passes: false`
3. Append failure details to progress.txt with what was tried
4. Move on — another iteration will address it

## Stop Condition

After completing a user story, check if ALL stories have `passes: true`.
If ALL stories are complete and passing, output: **RALPH_COMPLETE**
If there are still stories with `passes: false`, end your response normally.

## Important

- Work on **ONE story per iteration**
- Commit frequently
- Keep CI green
- Read the Codebase Patterns section in progress.txt BEFORE starting
- Do NOT attempt multiple stories in one iteration
