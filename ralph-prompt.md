# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** section FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt` — these describe changes made outside the loop that you must account for
4. Check you're on the correct branch from PRD `branchName`. If not, check it out or create from main.
5. Pick the **highest priority** user story where `passes: false`
6. Implement that single user story
7. Run build: `./gradlew :api:build :cli:build`
8. Run tests: `./gradlew :api:test :cli:test`
9. If checks pass, commit and push ALL changes: `git add -A && git commit -m "feat: [{story.id}] {story.title}" && git push`
10. Update `prd.json` — set `passes: true` for the completed story
11. Append your progress to `progress.txt`
12. If you discover reusable patterns, consolidate them into the Codebase Patterns section at the top of `progress.txt`

## Project Context

This is the OpenSearch SQL plugin repo (opensearch-project/sql). We're adding a CLI playground to the unified query API library (`api/` module) so users can try SQL/PPL queries against example data instantly.

Key existing code:
- `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java` — `plan(query) → RelNode`
- `api/src/main/java/org/opensearch/sql/api/compiler/UnifiedQueryCompiler.java` — `compile(RelNode) → PreparedStatement`
- `api/src/main/java/org/opensearch/sql/api/UnifiedQueryContext.java` — Builder: `.language(QueryType).catalog(name, schema).build()`
- `api/src/testFixtures/java/org/opensearch/sql/api/UnifiedQueryTestBase.java` — Contains `SimpleTable` (ScannableTable impl with @Builder) and test schema setup patterns
- `api/src/testFixtures/java/org/opensearch/sql/api/ResultSetAssertion.java` — ResultSet iteration pattern
- `api/build.gradle` — Reference for build config, spotless, jacoco settings
- `settings.gradle` — Where to register new modules

The CLI architecture: JLine3 REPL → QueryRepl.dispatch() → UnifiedQueryPlanner.plan() → UnifiedQueryCompiler.compile() → PreparedStatement.executeQuery() → ResultSetFormatter → stdout

Implementation plan with full details (optional reference, may not exist on all machines): `~/.kiro/plans/opensearch-sql/2026-03-26-cli-implementation-plan.md`

## Build Notes

- Java 21 is enforced globally in root build.gradle
- All modules use spotless with googleJavaFormat 1.32.0 and Apache 2.0 license header
- The api module has 90% jacoco coverage requirement — be careful when modifying api code
- Jackson is available transitively via Calcite (use it for JSON parsing in SampleDataLoader)
- Default catalog name used in tests: "catalog"
- QueryType enum: `org.opensearch.sql.executor.QueryType` with values `PPL` and `SQL`

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
