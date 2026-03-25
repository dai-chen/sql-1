# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** section FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt` — these describe changes made outside the loop that you must account for
4. Check you're on the correct branch from PRD `branchName`. If not, check it out or create from main.
5. Pick the **highest priority** user story where `passes: false`
6. Implement that single user story
7. Run build: `./gradlew build -x integTest -x doctest`
8. Run tests: `./gradlew test`
9. Run verification: `./gradlew spotlessCheck`
10. If checks pass, commit and push ALL changes: `git add -A && git commit -m "feat: [{story.id}] {story.title}" && git push`
11. Update `prd.json` — set `passes: true` for the completed story
12. Append your progress to `progress.txt`
13. If you discover reusable patterns, consolidate them into the Codebase Patterns section at the top of `progress.txt`

## Project Context

This project is the OpenSearch SQL plugin (opensearch-project/sql). It uses Calcite 1.41.0 as the
query optimization framework. We are removing datetime UDTs (EXPR_DATE, EXPR_TIME, EXPR_TIMESTAMP)
from the core/ module and replacing them with standard Calcite SqlTypeName.DATE/TIME/TIMESTAMP.

### Why We're Removing UDTs

The datetime UDTs were a workaround for preserving nanosecond precision from OpenSearch's date_nanos
field type. They are VARCHAR-backed, meaning datetime values flow as Strings through Calcite. This:
- Causes type mismatches with standard Calcite DATE (int) / TIMESTAMP (long)
- Prevents Calcite optimizer from applying datetime rules
- Leaks a physical storage concern into the logical query layer
- No PPL function distinguishes date vs date_nanos
- Calcite supports TIMESTAMP(9) via RelDataTypeSystem override (Flink does this)

### Design Document

Full plan: `~/.kiro/plans/opensearch-sql/2026-03-24-remove-datetime-udt-plan.md`

### Key Files to Modify

**Core type system:**
- `core/src/main/java/org/opensearch/sql/executor/OpenSearchTypeSystem.java`
- `core/src/main/java/org/opensearch/sql/calcite/utils/PPLReturnTypes.java`
- `core/src/main/java/org/opensearch/sql/calcite/utils/OpenSearchTypeFactory.java`

**Value boundary:**
- `core/src/main/java/org/opensearch/sql/data/model/ExprValueUtils.java`
- `core/src/main/java/org/opensearch/sql/data/model/ExprDateValue.java`
- `core/src/main/java/org/opensearch/sql/data/model/ExprTimeValue.java`
- `core/src/main/java/org/opensearch/sql/data/model/ExprTimestampValue.java`

**Cast/visitor:**
- `core/src/main/java/org/opensearch/sql/calcite/ExtendedRexBuilder.java`
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`

**OpenSearch adapter:**
- `opensearch/src/main/java/org/opensearch/sql/opensearch/scan/OpenSearchIndexEnumerator.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/request/PredicateAnalyzer.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/request/AggregateAnalyzer.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/serde/ExtendedRelJson.java`

**Function registration:**
- `core/src/main/java/org/opensearch/sql/expression/function/PPLFuncImpTable.java`
- `core/src/main/java/org/opensearch/sql/expression/function/PPLBuiltinOperators.java`

**UDT infrastructure to remove:**
- `core/src/main/java/org/opensearch/sql/calcite/type/ExprDateType.java`
- `core/src/main/java/org/opensearch/sql/calcite/type/ExprTimeType.java`
- `core/src/main/java/org/opensearch/sql/calcite/type/ExprTimeStampType.java`
- `core/src/main/java/org/opensearch/sql/calcite/utils/UserDefinedFunctionUtils.java` (UDT constants)

### Build Commands

```bash
# Fast build (skip integration tests)
./gradlew build -x integTest -x doctest

# Unit tests only
./gradlew test

# Specific module
./gradlew :core:test
./gradlew :opensearch:test
./gradlew :api:test

# Code formatting
./gradlew spotlessApply
./gradlew spotlessCheck

# Integration tests (requires OpenSearch cluster)
./gradlew :integ-test:integTest --tests "*CalcitePPL*"
```

### Important Notes

- Build output can be very large. Use `2>&1 | tail -30` to see just the end.
- If build fails, use `grep -A 5 "error:" build.log` to find errors.
- Run `./gradlew spotlessApply` before committing — the project enforces Google Java Format.
- Do NOT modify tests to make them pass by deleting assertions. Fix the code.
- Each story may cause cascading compilation errors. Fix them all within the story.

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
- Run `./gradlew spotlessApply` before committing

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
