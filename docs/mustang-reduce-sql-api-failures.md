# Prompt: Quick fixes to shift SQL IT failures from SQL plugin to analytics engine

## Context

You are working on the [`feature/mustang-sql-it-local-changes`](https://github.com/dai-chen/sql-1/tree/feature/mustang-sql-it-local-changes)
branch of `dai-chen/sql-1`. This branch contains:

- The `ahkcs/feature/mustang-ppl-it-coverage` Mustang tooling (`analyticsSqlCompatibilityReport`
  task, `force_routing` setting)
- The `api/` module pulled from main (`UnifiedSqlSpec`, `LanguageSpec`, `postParseRules`
  hook)
- Prior local fixes: jar-hell excludes, improved report renderer, a `DateTimeLiteralRewriter`
  that converts `DATE('str')` → ANSI `DATE 'str'`
- `mustang-scripts/` — non-blocking test orchestration (strict invariant: no script blocks
  forever)

**Before you start**, run `./mustang-scripts/preflight.sh` to validate your environment. On a
fresh devhost you'll also need to clone the sandbox-enabled OpenSearch fork and build its
native Rust library — see `mustang-scripts/README.md` for the bootstrap sequence.

**Current baseline** (from the most recent run with forced analytics-engine routing):

```
Tests executed: 922  |  Passed: 432 (46.9%)  |  Failed: 490  |  Skipped: 487 (class-level @Ignore)

Failures by origin (post DateTimeLiteralRewriter):
  SQL/api        248  (51%)  ← goal: drive this down
  AE             192  (39%)
  SQL/preflight   26  (5%)
  Test            19  (4%)
  SQL/other        4  (1%)
```

## Goal

**Shift the failure distribution so the dominant origin is AE, not SQL/api.** The point of
the compatibility report is to measure analytics-engine readiness — when `SQL/api` is the top
bucket, we're measuring our own api/ module gaps, not AE's.

Specifically: move as many of the 248 `SQL/api` failures as possible onto AE's plate by
making queries parse and validate successfully through `UnifiedQueryPlanner`. Once a query
passes parse+validate, whatever happens next — including failures — is AE territory.

## Allowed fix categories

Limit yourself to these three techniques. If a failure requires anything more invasive,
**skip it and note it** in a short followup-work list.

### A. Tweak `UnifiedSqlSpec` parser/validator/conformance config

In `api/src/main/java/org/opensearch/sql/api/spec/UnifiedSqlSpec.java`, adjust:

- `Lex` (case sensitivity, quoting, char-literal styles — e.g. `BIG_QUERY` vs `MYSQL` vs
  `MYSQL_ANSI`)
- `SqlParserImplFactory` (stay with `SqlBabelParserImpl.FACTORY` unless you find a standard
  parser feature we're missing — **do not ship a custom `.jj` grammar**)
- `SqlConformanceEnum` (currently `BABEL` — `LENIENT` or `DEFAULT` may change `GROUP BY`
  behavior, alias resolution, etc.)
- `SqlValidator.Config` (type coercion, identifier expansion, default-null, etc.)

**Each change must be motivated by a specific bucket in the report.** Don't flip flags
speculatively.

### B. Register Calcite library functions via the operator table

Calcite ships function libraries beyond `SqlStdOperatorTable`. For example,
`SqlLibraryOperators` has `SUBSTR`, `IF`, `IFNULL`, `LENGTH`, many date/time conversions,
etc.

Extend `LanguageSpec.operatorTable()` (or add a new `LanguageExtension`) to register:

- `SqlLibraryOperators` entries from relevant libraries (`MYSQL`, `BIG_QUERY`, `POSTGRESQL`,
  `STANDARD`)
- Use `SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(Set.of(...))`

**Only register functions that show up as "No match found for function signature X" in the
report.** Don't shotgun every library.

### C. Add post-parse rewriters

Add new `SqlVisitor<SqlNode>` shuttles under `api/src/main/java/org/opensearch/sql/api/spec/`
similar to the existing `DateTimeLiteralRewriter` and `NamedArgRewriter`. Register them
through a new `LanguageExtension` so they're composable.

Good rewriter candidates (verify against the actual report first):

- Rewrites that **translate OpenSearch SQL syntax to ANSI SQL** (e.g., if backtick-quoted
  identifiers or bracket-indexed field paths appear in failures)
- Rewrites that **coerce types** where legacy OpenSearch allowed implicit coercion but
  Calcite doesn't — e.g. `<DATE> = <TIME>` comparisons (4 failures each across `=, >, >=,
  <, <=`)
- Rewrites that **inject casts or unwrap values**

### What to skip

Skip any bucket that would require:

- **Custom Calcite grammar** (adding `.jj` tokens, custom `SqlKind`, new parser hooks)
- **Custom `SqlOperator` subclasses** with complex type inference / operand checking
- **New UDFs** with Java implementation bodies
- **Changes to core/ or legacy/** modules (stay in api/)
- **Any change requiring rebuilding analytics-engine** (it's external)

For skipped buckets, add a one-line entry to `docs/mustang-followup.md` with the bucket's
error text and the class of fix it would need.

## Workflow

Run all commands from `./mustang-scripts/` at the repo root. **Strict rule: never wait on a
command without a bounded timeout.** If you're tempted to `sleep 30`, use `./cluster-wait.sh`
or `./test-wait.sh` — they enforce timeouts and return non-zero on timeout.

For each iteration:

1. **Read the current top-25 report** to pick the biggest `SQL/api` bucket. Report lives at
   `integ-test/build/reports/analytics-sql-compatibility/REPORT.md`.
2. **Decide fix category** (A, B, or C). If it doesn't fit, skip and log.
3. **Make the change** in `api/`.
4. **Rebuild + restart cluster**:
   ```bash
   ./mustang-scripts/plugin-build.sh
   ./mustang-scripts/cluster-stop.sh && ./mustang-scripts/cluster-start.sh && ./mustang-scripts/cluster-wait.sh
   ```
5. **Smoke-test with a single IT class** that targets the bucket (e.g.,
   `DateTimeComparisonIT` for date/time comparison issues):
   ```bash
   ./mustang-scripts/test-run.sh "org.opensearch.sql.sql.DateTimeComparisonIT"
   TEST_WAIT_TIMEOUT=120 ./mustang-scripts/test-wait.sh
   ```
   Verify the target bucket shrank or moved to AE in the report.
6. **Run the full suite** once the smoke test confirms the direction:
   ```bash
   ./mustang-scripts/test-run.sh
   TEST_WAIT_TIMEOUT=600 ./mustang-scripts/test-wait.sh
   ```
7. **Commit with a scoped message** — one fix per commit. Use the format:
   ```
   api: <one-line summary> (-<N> SQL/api failures)

   Bucket addressed: <verbatim error text from report>
   Fix category: A|B|C
   Before: SQL/api=<n1>, AE=<n2>
   After:  SQL/api=<n1>, AE=<n2>
   ```
8. **Stop when**:
   - `SQL/api <= AE` in the origin breakdown, OR
   - The next-biggest `SQL/api` bucket requires a skipped category, OR
   - 10 commits — whichever comes first. Then write a final report.

## Hard constraints

- **Do not push** the branch to any remote unless explicitly asked.
- **Do not modify** `mustang-scripts/` unless genuinely broken — they're reusable tooling.
- **Do not touch** any file under `$OS_REPO` (the sandbox OpenSearch fork); treat it as
  read-only.
- **Do not modify tests** to make them pass. The test queries define the compatibility
  target.
- **Do not `@Ignore` failing tests.** Fix the api/ code or skip the bucket.
- **Do not run `./gradlew clean`**. Incremental builds only.
- **If the cluster crashes** during a test run (check `tail -50 /tmp/sisyphus-cluster/cluster.log`
  for `fatal error`), your most recent api/ change likely triggers an AssertionError in a
  server thread — revert it, add a guard, and retry. Never leave the JVM in a crashed state
  between runs.

## Deliverables

1. A chain of commits on `feature/mustang-sql-it-local-changes`, one per fix.
2. An updated `REPORT.md` in `integ-test/build/reports/analytics-sql-compatibility/` from
   the final full-suite run.
3. A summary at the end with:
   - Starting and ending `SQL/api` vs `AE` counts
   - List of commits with their individual deltas
   - The `docs/mustang-followup.md` file listing any skipped buckets + required fix
     category

## Start here

```bash
# First time on this machine — validate prereqs
./mustang-scripts/preflight.sh

# Current branch — should be feature/mustang-sql-it-local-changes
git branch --show-current

# Baseline report
head -80 integ-test/build/reports/analytics-sql-compatibility/REPORT.md

# Cluster state
./mustang-scripts/status.sh
```

Then pick the largest `SQL/api` bucket and begin.
