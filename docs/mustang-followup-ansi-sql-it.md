# Prompt: Build Calcite-native SQL IT tooling for the Mustang unified path

## Context

You are starting from the clean upstream branch
[`opensearch-project/sql:feature/mustang-ppl-integration`](https://github.com/opensearch-project/sql/tree/feature/mustang-ppl-integration).

This branch already has:
- The unified query path (`UnifiedQueryPlanner`, `RestUnifiedQueryAction`)
- Analytics-engine-backed routing for parquet-prefixed indices
- Jar-hell excludes (`httpcore5-h2`, `httpclient5`) landed upstream in #5400
- PPL-side Calcite integration tests: `CalcitePPLTpchIT`, `CalcitePPLClickBenchIT`,
  `CalcitePPLBig5IT` under `integ-test/src/test/java/org/opensearch/sql/calcite/`
- `opensearch-sql-jdbc` as a test dependency in `integ-test/build.gradle`
- Query resources under `integ-test/src/test/resources/{tpch,clickbench,big5}/queries/`
  where each `.ppl` file has an ANSI SQL form in a leading `/* ... */` block

This branch **does NOT** have (but you will build):
- `analyticsSqlCompatibilityReport` Gradle task (SQL-side coverage tooling)
- `CALCITE_ANALYTICS_FORCE_ROUTING` cluster setting for forcing every query through AE
- `SQLIntegTestCase` hooks for force-routing setup
- The `api/spec/UnifiedSqlSpec` / `LanguageSpec` abstraction
- `SQLClickBenchIT` or any SQL-side Calcite ITs
- Quidem-based ANSI SQL IT for Calcite `.iq` files

## Reference branches for changes to pick

All of these live in **`dai-chen/sql-1`** — cherry-pick / adapt as needed:

### 1. Jar-hell resolution reference
  - Upstream already has this (commit `e113b7f91` in `#5400`). **No action needed.**
  - Historical reference (in case you hit similar issues):
    [`feature/mustang-reduce-sql-api-failures`](https://github.com/dai-chen/sql-1/tree/feature/mustang-reduce-sql-api-failures)
    commit
    [`plugin: exclude httpcore5-h2 / httpclient5`](https://github.com/dai-chen/sql-1/commit/9f84fb07d)

### 2. Latest api/ module from main
  - Pull from `opensearch-project/sql:main` path `api/`. Specifically the
    `api/src/main/java/org/opensearch/sql/api/spec/` package (`LanguageSpec`,
    `UnifiedSqlSpec`, `UnifiedFunctionSpec`, `UnifiedPplSpec`, plus
    `spec/search/NamedArgRewriter` and `spec/search/SearchExtension`).
  - Landed in main via PRs #5257, #5268, #5274, #5279, #5330, #5360.
  - Reference of how I pulled it:
    [`feature/mustang-sql-it-local-changes` commit `7a5a88ef6`](https://github.com/dai-chen/sql-1/commit/7a5a88ef6)

### 3. Existing SQL IT runner + analysis script
  - [`feature/ansi-sql-quidem-it`](https://github.com/dai-chen/sql-1/tree/feature/ansi-sql-quidem-it)
    has everything assembled:
    - `mustang-scripts/` — non-blocking cluster/test lifecycle scripts (copy wholesale)
    - `integ-test/src/test/java/org/opensearch/sql/ansi/AnsiSqlQuidemIT.java` — the JUnit harness
    - `integ-test/src/test/java/org/opensearch/sql/ansi/OpenSearchConnectionFactory.java`
    - `integ-test/src/test/java/org/opensearch/sql/ansi/ScottSchemaSeeder.java`
    - `integ-test/src/test/resources/ansi/*.iq` — 15 adapted Calcite files
    - `integ-test/build.gradle` — adds `analyticsAnsiQuidemTest` / `Report` Gradle tasks
      using the `configureAnalyticsCompatibilityTest` closure pattern
  - [`feature/mustang-sql-it-local-changes`](https://github.com/dai-chen/sql-1/tree/feature/mustang-sql-it-local-changes)
    contains the `analyticsSqlCompatibilityReport` task and the report-renderer closure
    (`renderAnalyticsReport`) which buckets failures by origin (SQL/api vs AE vs
    Calcite library vs Test). Copy the closure + the two report-rendering Gradle tasks.
  - [`feature/mustang-reduce-sql-api-failures`](https://github.com/dai-chen/sql-1/tree/feature/mustang-reduce-sql-api-failures)
    has five post-parse rewriters that eliminate ~80% of SQL/api failures
    (`DateTimeLiteralRewriter`, `MultiArgDateTimeRewriter`,
    `CrossTypeTemporalCompareRewriter`, `AvgTemporalRewriter`, `CastTypeNameRewriter`),
    plus `CalciteLibraryExtension` and `OpenSearchStubFunctionExtension`. Copy these
    AFTER you have `api/spec/` in place — they depend on `LanguageSpec` /
    `UnifiedSqlSpec`.

## Goals

Each goal is independently verifiable. Order them by priority 1 > 2 > 3 > 4.

### Goal 1 — Easy command to run a new **SQLClickBenchIT with correctness assertion**

Today the ahkcs-contributed `SQLClickBenchIT` (on
[`feature/mustang-sql-it-local-changes`](https://github.com/dai-chen/sql-1/blob/feature/mustang-sql-it-local-changes/integ-test/src/test/java/org/opensearch/sql/sql/clickbench/SQLClickBenchIT.java))
runs every ClickBench `.ppl` query's SQL form through the SQL REST endpoint and tallies
**pass / fail by HTTP status**. It does NOT verify correctness — any non-error response
is treated as passing.

**Build a new `SQLClickBenchCorrectnessIT`** that:
1. Runs each ClickBench query through the unified SQL path (with
   `tests.analytics.force_routing=true`).
2. Compares the result against a known-correct reference. Sources for the reference:
   - (Preferred) **Extract from the PPL-side `CalcitePPLClickBenchIT` or
     `PPLClickBenchIT`** — those tests already have hard-coded assertions via
     `verifyDataRows(...)`. Convert them to SQL reference results.
   - (Alternative) Run the query once via the **v2 path** (`force_routing=false`) at
     `@Before` time, snapshot the result, then run the same query through the unified
     path and compare.
3. Emits a per-query pass/fail report (`integ-test/build/reports/clickbench-sql-correctness/REPORT.md`).

Launch via:
```bash
./mustang-scripts/cluster-start.sh && ./mustang-scripts/cluster-wait.sh
./gradlew :integ-test:analyticsSqlClickBenchCorrectnessReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask
```

### Goal 2 — Easy command to run a new **SQLTpchIT with correctness assertion**

Same structure as Goal 1 but for TPC-H. Extract SQL form from each
`integ-test/src/test/resources/tpch/queries/q*.ppl` file's `/* ... */` block.

Reference results can come from:
- `CalcitePPLTpchIT` (already has `verifyDataRows` / `assertJsonEquals` assertions)
- Or the official TPC-H reference answers

Launch via:
```bash
./gradlew :integ-test:analyticsSqlTpchCorrectnessReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask
```

### Goal 3 — Easy command to run **Quidem ANSI SQL IT** for better coverage

Copy from [`feature/ansi-sql-quidem-it`](https://github.com/dai-chen/sql-1/tree/feature/ansi-sql-quidem-it):
- `integ-test/src/test/java/org/opensearch/sql/ansi/` (3 Java files)
- `integ-test/src/test/resources/ansi/*.iq` (15 adapted Calcite files — already has the
  adapt_iq.py logic baked into the output)
- The `analyticsAnsiQuidemTest` / `analyticsAnsiQuidemReport` Gradle tasks

**Also add**: port the `adapt_iq.py` script (check its contents in the commit below)
into the repo at `scripts/calcite-iq/adapt_iq.py` so future contributors can pull more
.iq files from Calcite upstream as they stabilize.

Launch via:
```bash
./gradlew :integ-test:analyticsAnsiQuidemReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask
```

### Goal 4 [LOW PRIORITY] — Easy command to run **existing SQL V2/legacy IT for compat**

Copy the `analyticsSqlCompatibilityTest` / `analyticsSqlCompatibilityReport` tasks from
[`feature/mustang-sql-it-local-changes`](https://github.com/dai-chen/sql-1/blob/feature/mustang-sql-it-local-changes/integ-test/build.gradle).
This requires:
- The `CALCITE_ANALYTICS_FORCE_ROUTING` setting wired into
  `common/Settings.java` + `opensearch/OpenSearchSettings.java` +
  `plugin/RestUnifiedQueryAction.isAnalyticsIndex()`
- `SQLIntegTestCase.init()` and `PPLIntegTestCase.init()` reading
  `tests.analytics.force_routing` to enable the setting
- The `renderAnalyticsReport` closure at the top of `integ-test/build.gradle`
  (~240 lines) that buckets failures by origin

Launch via:
```bash
./gradlew :integ-test:analyticsSqlCompatibilityReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask \
    -PclusterLog=/tmp/sisyphus-cluster/cluster.log
```

## Hard constraints

- **Every Gradle test task must accept `-Dtests.rest.cluster` gating via `onlyIf`** so
  it skips cleanly when no cluster is configured (look at `configureAnalyticsCompatibilityTest`
  in `feature/mustang-sql-it-local-changes` for the pattern).
- **No test task should fail the build on per-query failures** — the report IS the
  deliverable. Use `ignoreFailures = true`.
- **Reuse the existing `mustang-scripts/` lifecycle** (cluster-start, wait, stop, plugin-build,
  preflight). Do not re-invent. These are already battle-tested on macOS and have
  bounded timeouts throughout.
- **Do not push to any remote** unless explicitly asked.
- **Every Gradle task must complete or time out within 10 minutes** — use the scripts
  (`test-wait.sh` has configurable `TEST_WAIT_TIMEOUT`).
- **If the cluster crashes** during development (look for
  `java.lang.AssertionError: cannot cast` or similar in
  `/tmp/sisyphus-cluster/cluster.log`), your most recent rewriter change likely triggers
  an AssertionError in a server thread — revert, add a type guard, retry. Never leave
  the JVM in a crashed state between runs.
- **Do not run `./gradlew clean`**. Incremental builds only. If gradle hits an
  "immutable workspace modified" error, follow the documented recovery:
  `pkill -f GradleDaemon && rm -rf ~/.gradle/caches/*/transforms`.

## Deliverables

1. Four new commits on a branch off `upstream/feature/mustang-ppl-integration`:
   - Goal 3 first (Quidem IT — smallest, fastest to verify)
   - Goal 1 (SQLClickBenchCorrectnessIT)
   - Goal 2 (SQLTpchCorrectnessIT)
   - Goal 4 (legacy SQL v2 compat)
2. Each task produces a machine-readable `REPORT.md` under
   `integ-test/build/reports/<task-name>/REPORT.md`.
3. Each task passes its smoke test (one representative class, e.g.
   `--tests "org.opensearch.sql.ansi.AnsiSqlQuidemIT"`) before the full report is run.
4. Update `mustang-scripts/README.md` with the four new test-run one-liners.

## Start here

```bash
# 1. Check out the upstream base branch
git clone git@github.com:opensearch-project/sql.git
cd sql
git checkout feature/mustang-ppl-integration

# 2. Validate environment
./mustang-scripts/preflight.sh   # copy from feature/ansi-sql-quidem-it first

# 3. Start a cluster
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh

# 4. Begin with Goal 3 — it's the quickest win and produces a usable report on day one.
```
