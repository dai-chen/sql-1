# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** section FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt` — these describe changes made outside the loop that you must account for
4. Check you're on the correct branch from PRD `branchName`. If not, check it out or create from main.
5. Pick the **highest priority** user story where `passes: false`
6. Implement that single user story
7. Run build: `./gradlew :integ-test:compileTestJava`
8. Run tests: `./gradlew :integ-test:integTest`
9. If checks pass, commit and push ALL changes: `git add -A && git commit -m "feat: [{story.id}] {story.title}" && git push`
10. Update `prd.json` — set `passes: true` for the completed story
11. Append your progress to `progress.txt`
12. If you discover reusable patterns, consolidate them into the Codebase Patterns section at the top of `progress.txt`

## Project Context

This is the OpenSearch SQL plugin repository. We are adding a gap analysis mechanism that intercepts
SQL and PPL integration test execution to replay queries through the new `UnifiedQueryPlanner` +
`UnifiedQueryCompiler` pipeline (in the `api` module), producing a categorized error report.

### Key Reference Files

- `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java` — The planner to test against
- `api/src/main/java/org/opensearch/sql/api/UnifiedQueryCompiler.java` — Compiles RelNode to PreparedStatement
- `api/src/main/java/org/opensearch/sql/api/UnifiedQueryContext.java` — Context builder with schema/settings
- `integ-test/src/test/java/org/opensearch/sql/api/UnifiedQueryOpenSearchIT.java` — Reference implementation showing how to set up context with OpenSearchIndex schema, plan, compile, and execute
- `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` — PPL IT base class (intercept `executeQuery`)
- `integ-test/src/test/java/org/opensearch/sql/legacy/SQLIntegTestCase.java` — SQL IT base class (intercept `executeJdbcRequest`, `executeQuery`)
- `docs/plans/2026-03-20-unified-query-gap-analysis-design.md` — Design document

### Key Patterns

- `UnifiedQueryOpenSearchIT` shows the exact pattern: create `OpenSearchRestClient` from `client()`, build `AbstractSchema` with `OpenSearchIndex`, configure `UnifiedQueryContext.builder()` with settings, create planner + compiler
- PPL queries use `source=<index>` but unified planner needs `source=opensearch.<index>` (catalog prefix)
- SQL queries can use `defaultNamespace("opensearch")` in context builder to resolve unqualified table names
- The gap analyzer must NEVER fail the original test — catch all exceptions, log only
- Controlled by `-Dunified.gap.analysis=true` system property

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

## Consolidate Patterns

If you discover a **reusable pattern** that future iterations should know, add it to the `## Codebase Patterns` section at the TOP of progress.txt.

## Human Intervention Notes

If you see a `## Human Intervention` section in `progress.txt`, a human made changes between rounds. Read it carefully and account for these changes.

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

If ALL stories are complete and passing, output the exact text `RALPH_COMPLETE` on its own line.

If there are still stories with `passes: false`, end your response normally.

## Important

- Work on **ONE story per iteration**
- Commit frequently
- Keep CI green
- Read the Codebase Patterns section in progress.txt BEFORE starting
- Do NOT attempt multiple stories in one iteration
