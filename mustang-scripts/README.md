# mustang-sql helper scripts

Set of small, composable shell scripts for running the mustang SQL compatibility test suite.
**Strict invariant**: every script either returns immediately or has a bounded timeout. None
will hang forever.

## Location

This directory (`mustang-scripts/`) ships inside the SQL repo so the scripts default to using
the enclosing repo as `SQL_REPO`. Override via env var if your layout differs.

## First time on a fresh machine

Run `./preflight.sh` first — it validates every prerequisite (JDKs, sandbox repo, native
library, free ports, disk, memory) and prints actionable install instructions for anything
missing.

### Required tools

- Amazon Corretto **JDK 21** (for SQL plugin build + runtime)
- Amazon Corretto **JDK 25** (for `sandbox/libs/dataformat-native` which uses the FFM API)
- **Rust/cargo** (to build the native library on first run)
- `git`, `curl`, `bash 4+`, `find`, `tar`

Scripts auto-discover JDKs in common locations (`/Library/Java/JavaVirtualMachines/` on macOS,
`/usr/lib/jvm/` on Linux). To override, export `JDK_21` or `JDK_25` pointing at JAVA_HOME.

### Required external repo: OpenSearch sandbox fork

The analyticsSqlCompatibilityReport task requires a **sandbox-enabled OpenSearch build**
(the `sandbox/plugins/` directory with analytics-engine and friends). This is an
Amazon-internal fork. Clone it and set `OS_REPO` to its path.

```bash
# One-time setup
git clone <your-opensearch-sandbox-fork-url> ~/IdeaProjects/OpenSearch
cd ~/IdeaProjects/OpenSearch
# Check out the branch with Mustang sandbox plugins (team-specific name)
# Build native library first (cargo must be installed):
cd sandbox/libs/dataformat-native/rust && cargo build --release
```

Then, from a scratch machine setup:

```bash
# Clone the SQL repo's mustang branch (this one)
git clone git@github.com:dai-chen/sql-1.git
cd sql-1
git checkout feature/mustang-sql-it-local-changes

# Set up env
export OS_REPO=$HOME/IdeaProjects/OpenSearch
# (JDK21/JDK25 auto-discovered, override via JDK_21 / JDK_25 if needed)

# Validate setup
./mustang-scripts/preflight.sh

# Build + run
./mustang-scripts/plugin-build.sh
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh
./mustang-scripts/test-run.sh
./mustang-scripts/test-wait.sh
./mustang-scripts/cluster-stop.sh
```

## Paths (override with env vars)

- `OS_REPO` - OpenSearch sandbox-enabled repo (default: `$HOME/IdeaProjects/OpenSearch`)
- `SQL_REPO` - this SQL repo (default: parent of `mustang-scripts/`, i.e. the repo root)
- `STATE_DIR` - where pid/log files live (default: `/tmp/sisyphus-cluster`)
- `JDK_21`, `JDK_25` - paths to Corretto 21 and 25 (auto-discovered if unset)

## Typical workflow

```bash
cd mustang-scripts/

# First time on a fresh machine — validate environment
./preflight.sh

# Build + publish SQL plugin (after code changes in api/ or plugin/)
./plugin-build.sh

# Start cluster if not already running — returns immediately
./cluster-start.sh

# Wait up to 120s for the cluster HTTP endpoint
./cluster-wait.sh

# Kick off the full analyticsSqlCompatibilityReport — returns immediately
./test-run.sh

# Or kick off a single test class
./test-run.sh "org.opensearch.sql.sql.DateTimeComparisonIT"

# Poll status without blocking
./test-status.sh

# Block (up to 10 min) for test to finish
./test-wait.sh

# Shut down the cluster
./cluster-stop.sh
```

## Mustang correctness / coverage reports

Three Gradle tasks landed with this branch for the Mustang unified-path coverage story.
Each emits a markdown REPORT.md — **per-query failures never fail the build**; the report
itself is the deliverable (see `docs/mustang-followup-ansi-sql-it.md`).

All three need a running cluster first:

```bash
./cluster-start.sh && ./cluster-wait.sh
export JAVA_HOME=$JDK_21   # or wherever Corretto 21 lives
```

### 1. ANSI SQL Quidem coverage (Goal 3)

Replays 15 adapted Apache Calcite `.iq` golden files through the JDBC endpoint. Report
buckets failures by cause (Quidem command / JDBC / Calcite / other).

```bash
./gradlew :integ-test:analyticsAnsiQuidemReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask
```

Report: `integ-test/build/reports/ansi-sql-quidem/REPORT.md`
Fixtures: `integ-test/src/test/resources/ansi/*.iq` (port new ones via
`scripts/calcite-iq/adapt_iq.py`).

### 2. ClickBench SQL correctness (Goal 1)

Replays 43 ClickBench SQL queries through the unified path and validates each response
against a committed JSON snapshot. Creates `hits` as a Parquet-backed index when
`tests.analytics.force_routing=true` (default).

```bash
# Validate against committed snapshots:
./gradlew :integ-test:analyticsSqlClickBenchCorrectnessReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask \
    -Dproject.root=$PWD/integ-test

# Re-capture snapshots (add --rerun-tasks to force re-execution):
./gradlew :integ-test:analyticsSqlClickBenchCorrectnessReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask \
    -Dtests.snapshot.write=true \
    -Dproject.root=$PWD/integ-test \
    --rerun-tasks

# Fall back to the legacy Lucene-backed v2 path:
./gradlew :integ-test:analyticsSqlClickBenchCorrectnessReport \
    -Dtests.analytics.force_routing=false \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask \
    -Dproject.root=$PWD/integ-test
```

Report: `integ-test/build/reports/clickbench-sql-correctness/REPORT.md`
Snapshots: `integ-test/src/test/resources/expectedOutput/clickbench-sql/q{N}.json`

### 3. TPC-H SQL correctness (Goal 2)

Same shape as the ClickBench harness, for 22 TPC-H queries against 8 tables
(customer, lineitem, orders, supplier, part, partsupp, nation, region).

```bash
# Validate:
./gradlew :integ-test:analyticsSqlTpchCorrectnessReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask \
    -Dproject.root=$PWD/integ-test

# Re-capture snapshots:
./gradlew :integ-test:analyticsSqlTpchCorrectnessReport \
    -Dtests.rest.cluster=localhost:9200 \
    -Dtests.cluster=localhost:9300 \
    -Dtests.clustername=runTask \
    -Dtests.snapshot.write=true \
    -Dproject.root=$PWD/integ-test \
    --rerun-tasks
```

Report: `integ-test/build/reports/tpch-sql-correctness/REPORT.md`
Snapshots: `integ-test/src/test/resources/expectedOutput/tpch-sql/q{N}.json`

### Gotchas

- **Every task gates on `-Dtests.rest.cluster` via `onlyIf`** — skipping cleanly if no cluster
  is configured. Pass the flag even if the cluster is local.
- **`ignoreFailures = true`** on every task, so BUILD SUCCESSFUL just means the harness ran
  to completion, not that every query passed.
- **Gradle caches test results aggressively** — if you run a task twice with the same inputs
  (IT class + cluster unchanged), the second run will be a no-op. Use `--rerun-tasks` to
  force fresh execution.
- **Parquet-backed ingestion can be slow** — the ClickBench / TPC-H ITs set a 5-minute socket
  timeout on bulk loads. If init() fails anyway, the report will still be produced with every
  query marked FAIL and the init error text attached, so the failure mode is visible.

## Exit codes

- `cluster-wait.sh`: 0=ready, 1=timeout, 2=gradle died
- `test-status.sh`: 0=BUILD SUCCESSFUL, 1=BUILD FAILED, 2=still running, 3=no run
- `test-wait.sh`: same as test-status after completion, plus 2=timeout

## Config

Edit `lib.sh` to change timeouts or paths. Override per-invocation via env vars:

```bash
CLUSTER_WAIT_TIMEOUT=180 ./cluster-wait.sh
TEST_WAIT_TIMEOUT=1200 ./test-wait.sh
```

## State files

All under `$STATE_DIR` (defaults to `/tmp/sisyphus-cluster`):

- `cluster-gradle.pid` — the gradle `run` task PID
- `cluster.pid` — the OpenSearch java process PID (recorded once cluster is up)
- `cluster.log` — cluster stdout/stderr
- `test.pid` — current test-run gradle PID (if any)
- `test.log` — test-run stdout/stderr

Safe to `rm -rf $STATE_DIR` when nothing is running.
