# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** section FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt` — these describe changes made outside the loop that you must account for
4. Check you're on the correct branch from PRD `branchName`. If not, check it out or create from main.
5. Pick the **highest priority** user story where `passes: false`
6. Implement that single user story
7. Run build: `cd unified-functions-datafusion && cargo build 2>&1 | tail -20`
8. Run tests: `cd unified-functions-datafusion && cargo test 2>&1 | tail -40`
9. If checks pass, commit and push ALL changes: `git add -A && git commit -m "feat: [{story.id}] {story.title}" && git push`
10. Update `prd.json` — set `passes: true` for the completed story
11. Append your progress to `progress.txt`
12. If you discover reusable patterns, consolidate them into the Codebase Patterns section at the top of `progress.txt`

## Project Context

This project defines 93 PPL functions as Unified Function specs (Substrait YAML + test DSL) and implements them as DataFusion UDFs in Rust.

**Design doc:** `~/.kiro/plans/opensearch-sql/2026-04-13-ppl-function-spec-on-substrait-design.md` — READ THIS for the full YAML convention, test DSL syntax, metadata fields, and concrete examples.

**Two workspaces:**
- Java side: `api/src/main/resources/unified-functions/` — YAML specs + test DSL files + shared schema
- Rust side: `unified-functions-datafusion/` — standalone DataFusion crate with UDF implementations

**Substrait YAML convention:** URN `extension:org.opensearch:unified_<category>_functions`, metadata fields include null_handling, edge_cases, ai_hints with platform_hints for Rust/DataFusion.

**Substrait test DSL:** `### SUBSTRAIT_SCALAR_TEST: v1.0` header, typed literals (`'hello'::str`, `120::i32`, `null::bool`), test groups (`# group: description`).

**DataFusion UDF pattern:** Columnar API — `values_to_arrays` → `iter().zip()` → `collect::<TypedArray>()`. Implement `ScalarUDFImpl` trait with `invoke_with_args`.

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
