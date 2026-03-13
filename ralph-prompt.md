# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** section FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt` — these describe changes made outside the loop that you must account for
4. Check you're on the correct branch from PRD `branchName`. If not, check it out or create from main.
5. Pick the **highest priority** user story where `passes: false`
6. Implement that single user story
7. Run build: `./gradlew build -x integTest`
8. Run tests: `./gradlew test`
9. If checks pass, commit ALL changes with message: `feat: [{story.id}] {story.title}`
10. Update `prd.json` — set `passes: true` for the completed story
11. Append your progress to `progress.txt`
12. If you discover reusable patterns, consolidate them into the Codebase Patterns section at the top of `progress.txt`

## Project Context

This PoC validates the end-to-end flow of PPL and SQL queries against a Parquet-backed index through the SQL/PPL plugin, with RelNode handoff to the analytics-plugin (Option B: Unified Query Pipeline).

**Key references:**
- Full PoC plan: `docs/plans/poc-option-b.md`
- Analytics Engine PoC (SPI interfaces to copy): https://github.com/mch2/OpenSearch/tree/planning-poc/sandbox
- ANSI SQL on Calcite PoC (endpoint wiring pattern): https://github.com/dai-chen/sql-1/tree/poc/unified-sql-support

**Key technical context:**
- This is a multi-module Gradle Java project (Java 21, Gradle 8.x)
- Uses Apache Calcite for query planning, ANTLR4 for parsing
- PPL V3 already generates RelNode via CalciteRelNodeVisitor
- ANSI SQL uses Calcite's native SqlParser → SqlToRelConverter → RelNode
- New code for analytics engine dependencies goes in `analytics-engine-stub/` module (new Gradle submodule)
- Integration tests go in `integ-test/` module
- Code formatting: Google Java Format enforced by Spotless (`./gradlew spotlessApply` to fix)
- CRITICAL: PPL V3 overwrites datetime functions with UDT-based implementations (EXPR_TIMESTAMP). Parquet schema must use standard Calcite SqlTypeName to avoid interference.

## Progress Report Format

APPEND to progress.txt (never replace, always append):
```
## [Date/Time] - [Story ID]
- What was implemented
- Files changed
- **Learnings for future iterations:**
  - Patterns discovered (e.g., "this codebase uses X for Y")
  - Gotchas encountered (e.g., "don't forget to update Z when changing W")
  - Useful context (e.g., "the evaluation panel is in component X")
---
```

The learnings section is critical — it helps future iterations avoid repeating mistakes and understand the codebase better.

## Consolidate Patterns

If you discover a **reusable pattern** that future iterations should know, add it to the `## Codebase Patterns` section at the TOP of progress.txt (create it if it doesn't exist). This section should consolidate the most important learnings:

```
## Codebase Patterns
- Example: Use `sql<number>` template for aggregations
- Example: Always use `IF NOT EXISTS` for migrations
- Example: Export types from actions.ts for UI components
```

Only add patterns that are **general and reusable**, not story-specific details.

## Human Intervention Notes

If you see a `## Human Intervention` section in `progress.txt`, a human made changes between rounds. Read it carefully — it explains what was changed and why. Account for these changes in your implementation. Do not undo or conflict with human changes.

## Quality Requirements

- ALL commits must pass build and tests
- Do NOT commit broken code
- Keep changes focused and minimal
- Follow existing code patterns
- Never delete or skip tests to make build pass
- Run `./gradlew spotlessApply` before committing if Spotless check fails

## Failure Handling

If build or tests fail after 3 attempts at fixing:
1. Record the failure details in the story's `notes` field in prd.json
2. Leave `passes: false`
3. Append failure details to progress.txt with what was tried
4. Move on — another iteration (or a human) will address it

## Stop Condition

After completing a user story, check if ALL stories have `passes: true`.

If ALL stories are complete and passing, output the exact text `RALPH_COMPLETE` on its own line (no other text on that line).

If there are still stories with `passes: false`, end your response normally (another iteration will pick up the next story).

## Important

- Work on **ONE story per iteration**
- Commit frequently
- Keep CI green
- Read the Codebase Patterns section in progress.txt BEFORE starting
- Do NOT attempt multiple stories in one iteration
