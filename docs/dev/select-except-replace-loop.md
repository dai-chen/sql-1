# SELECT * EXCEPT / REPLACE — Ralph Loop Instructions

## What is this?

This file drives an autonomous implementation loop. Each iteration:
1. Reads `progress.txt` for current state
2. Reads `docs/dev/select-except-replace-design.md` for the full design
3. Picks the current incomplete task
4. Implements it
5. Runs the build/tests to verify
6. Updates `progress.txt`
7. Commits if the task passes

## Loop Protocol

### On each iteration:

1. **Read state**: `cat progress.txt` — find the current task (first unchecked `[ ]`)
2. **Read design**: `docs/dev/select-except-replace-design.md` — get implementation details for that task
3. **Implement**: Write the minimal code for that one task only
4. **Build**: Run `./gradlew :api:test 2>&1 | tail -50` to verify
5. **If build fails**: Fix the issue, rebuild. Max 3 fix attempts per task.
6. **If build passes**: Mark task `[x]` in `progress.txt`, add notes, advance "Current Task"
7. **Commit**: `git add -A && git commit -m "feat(api): <task description>"`
8. **Stop**: One task per iteration. Exit cleanly.

### Key constraints:

- **One task per loop iteration** — don't try to do multiple tasks
- **Build must pass** before marking complete
- **Don't modify existing tests** — only add new ones
- **Don't touch PPL/ANTLR paths** — only the Calcite ANSI_SQL path
- **Minimal code** — no over-engineering

### Key files:

| File | Role |
|------|------|
| `progress.txt` | Current state, task checklist |
| `docs/dev/select-except-replace-design.md` | Full design with task details |
| `api/build.gradle` | Build config (modify for FMPP) |
| `api/src/main/java/.../UnifiedQueryPlanner.java` | Wire rewriter here |
| `api/src/main/java/.../UnifiedQueryContext.java` | Wire parser factory here |
| `api/src/testFixtures/.../UnifiedQueryTestBase.java` | Test base class with SimpleTable |
| `api/src/test/.../UnifiedQueryPlannerTest.java` | Existing planner tests |

### Build commands:

```bash
# Full api module test
./gradlew :api:test 2>&1 | tail -50

# Specific test class
./gradlew :api:test --tests '*StarExcept*' 2>&1 | tail -30

# Just compile (faster feedback)
./gradlew :api:compileJava 2>&1 | tail -20
```

### If FMPP setup is too complex:

The design doc specifies FMPP parser extension (Task 1-3). If setting up the full
FMPP/JavaCC pipeline proves too complex for this project's Gradle setup, there is a
**fallback approach**: skip Tasks 1-3 and instead implement a **pre-parse regex-based
rewriter** or **SqlShuttle post-parse rewriter** that doesn't require grammar changes.

The fallback approach:
1. Parse `SELECT * EXCEPT(a,b) FROM t` as standard SQL — it will fail on `EXCEPT`
2. Instead, pre-process the SQL string to extract EXCEPT/REPLACE clauses, rewrite to
   explicit column list, then pass the rewritten SQL to Calcite's standard parser
3. This avoids all FMPP/JavaCC infrastructure but requires string manipulation

Use the fallback only if Task 1 fails after 3 attempts. Update progress.txt with the
decision.
