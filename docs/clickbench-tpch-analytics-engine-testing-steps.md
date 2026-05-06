# ClickBench, TPC-H & ANSI SQL IQ — Analytics Engine Testing Steps

End-to-end steps to run ClickBench (43 queries), TPC-H (22 queries), and ANSI SQL IQ (1867 queries) through the analytics-engine path on a local OpenSearch cluster with the full Mustang plugin set.

## Quick Start (Daily Report)

> **Prerequisite**: Complete "Setup from Scratch" below (one-time). After that, generate all reports with:

```bash
./mustang-scripts/daily-report.sh
```

This builds the plugin, starts the cluster, runs all 3 test suites, saves dated reports to `docs/test-reports/`, and stops the cluster. Takes ~3 minutes.

---

## Setup from Scratch

### 1. Install JDKs

```bash
# macOS (Homebrew)
brew install --cask corretto@21 corretto@25

# Amazon Linux 2
sudo yum install -y java-21-amazon-corretto-devel java-25-amazon-corretto-devel
```

### 2. Install Rust (one-time, for native parquet library)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### 3. Clone the OpenSearch sandbox repo

```bash
cd ~/IdeaProjects
git clone git@github.com:<your-fork>/OpenSearch.git
cd OpenSearch
git checkout main  # must be on clean main
```

### 4. Build the native parquet library (one-time)

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-25.jdk/Contents/Home
cd sandbox/libs/dataformat-native/rust
cargo build --release
```

### 5. Add commons-text to analytics-engine

The SQL plugin's Calcite validator needs `commons-text` (LevenshteinDistance). Add to `$OS_REPO/sandbox/plugins/analytics-engine/build.gradle`:

```groovy
// After the commons-math3 line:
runtimeOnly "org.apache.commons:commons-text:1.11.0"
```

Then build:
```bash
./gradlew :sandbox:plugins:analytics-engine:assemble -Dsandbox.enabled=true
```

### 6. Clone and set up the SQL repo

```bash
cd ~/IdeaProjects
git clone git@github.com:<your-fork>/sql.git my-sql-repo
cd my-sql-repo
git checkout feature/mustang-sql-it-tooling
```

### 7. Validate everything

```bash
./mustang-scripts/preflight.sh
```

All checks must pass. Then run the daily report:
```bash
./mustang-scripts/daily-report.sh
```

---

## Prerequisites

| Requirement | Purpose |
|---|---|
| JDK 21 (Amazon Corretto) | SQL plugin build |
| JDK 25 (Amazon Corretto) | OpenSearch sandbox runtime (FFM API for native parquet lib) |
| Rust/cargo | Native library build (one-time) |
| OpenSearch sandbox-enabled repo | Contains analytics-engine, composite-engine, parquet-data-format, etc. |
| SQL plugin repo (this repo) | `feature/mustang-sql-it-tooling` branch or later |

### Environment variables (auto-discovered by `mustang-scripts/lib.sh`)

```bash
export OS_REPO=~/IdeaProjects/OpenSearch   # sandbox-enabled OpenSearch fork
export SQL_REPO=~/IdeaProjects/my-sql-repo # this repo
```

### Validate environment

```bash
./mustang-scripts/preflight.sh
```

All checks must pass before proceeding.

---

## Step 1: Ensure OpenSearch sandbox repo is on clean `main`

**CRITICAL**: The OpenSearch sandbox repo (`$OS_REPO`) must be on a clean `main` branch with no local modifications to `analytics-backend-datafusion`. Local changes that reference SQL plugin classes (e.g., `PPLBuiltinOperators`) will cause `NoClassDefFoundError` at runtime due to classloader isolation, crashing the node on the first query.

```bash
cd $OS_REPO
git status --short sandbox/plugins/analytics-backend-datafusion/
# Should show nothing. If dirty:
git stash  # or commit to a feature branch
```

---

## Step 1: Pull latest OpenSearch sandbox code (optional)

If the analytics-engine team has pushed fixes, pull and rebuild:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-25.jdk/Contents/Home
cd $OS_REPO
git pull
./gradlew :sandbox:plugins:analytics-engine:assemble \
  :sandbox:plugins:analytics-backend-datafusion:assemble \
  :sandbox:plugins:composite-engine:assemble \
  -Dsandbox.enabled=true
```

Skip this step if you want to test against a known-good AE version.

---

## Step 2: Build and publish the SQL plugin to Maven local

```bash
./mustang-scripts/plugin-build.sh
```

Output: `~/.m2/repository/org/opensearch/plugin/opensearch-sql-plugin/3.7.0.0-SNAPSHOT/opensearch-sql-plugin-3.7.0.0-SNAPSHOT.zip`

**Note**: You do NOT need to run `publishToMavenLocal` on the OpenSearch core repo. The `./gradlew run` task resolves sandbox plugins directly from in-tree `:sandbox:plugins:*` projects. Only `opensearch-sql-plugin` and `opensearch-job-scheduler` are resolved from Maven local.

---

## Step 3: Start the OpenSearch cluster

```bash
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh
```

Under the hood, `cluster-start.sh` runs (in `$OS_REPO`):

```bash
./gradlew run -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT', \
    'analytics-engine', 'analytics-backend-lucene', \
    'analytics-backend-datafusion', 'parquet-data-format', \
    'composite-engine', 'opensearch-sql-plugin:3.7.0.0-SNAPSHOT']" \
  -Dtests.jvm.argline="-Djava.library.path=$OS_REPO/sandbox/libs/dataformat-native/rust/target/release \
    -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true"
```

### Verify plugins loaded

```bash
curl -s http://localhost:9200/_cat/plugins?v
```

Expected (7 plugins):

```
name      component                       version
runTask-0 analytics-backend-datafusion    3.7.0-SNAPSHOT
runTask-0 analytics-backend-lucene        3.7.0-SNAPSHOT
runTask-0 analytics-engine                3.7.0-SNAPSHOT
runTask-0 composite-engine                3.7.0-SNAPSHOT
runTask-0 opensearch-job-scheduler        3.7.0.0-SNAPSHOT
runTask-0 opensearch-sql                  3.7.0.0-SNAPSHOT
runTask-0 parquet-data-format             3.7.0-SNAPSHOT
```

---

## Step 4: Run ClickBench SQL correctness test

The test creates a **parquet-backed** `hits` index (via settings in `clickbench_index_mapping.json`), indexes sample data, enables `force_routing`, and runs all 43 queries through the analytics engine.

```bash
./gradlew :integ-test:analyticsSqlClickBenchCorrectnessReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -Dtests.snapshot.write=true \
  --rerun-tasks
```

Report output: `integ-test/build/reports/clickbench-sql-correctness/REPORT.md`

### Expected Results (as of 2026-05-04)

| Metric | Value |
|---|---:|
| Queries run | 43 |
| Passed (snapshots written) | 29 |
| Failed | 14 |
| Pass rate | **67.4%** |

### ClickBench failure categories

| Root Cause | Queries | Count |
|---|---|---:|
| Unrecognized filter operator [SEARCH] (Calcite LIKE→SEARCH rewrite with date comparison) | q37, q38, q39, q41, q42 | 5 |
| Unable to find binding for MIN on keyword column | q22, q23 | 2 |
| No backend supports scalar function [EXTRACT] | q19 | 1 |
| No backend supports scalar function [null] (length() not mapped) | q28 | 1 |
| No enum constant ScalarFunction.REGEXP_REPLACE | q29 | 1 |
| SqlParseException (test-asset bug: literal in q30.ppl) | q30 | 1 |
| No backend supports scalar function [MINUS] | q36 | 1 |
| NPE in Calcite validator (CASE WHEN ... END AS alias) | q40 | 1 |
| DATE_TRUNC signature mismatch (CHAR, TIMESTAMP) | q43 | 1 |

---

## Step 5: Run TPC-H SQL correctness test

```bash
./gradlew :integ-test:analyticsSqlTpchCorrectnessReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -Dtests.snapshot.write=true \
  --rerun-tasks
```

Report output: `integ-test/build/reports/tpch-sql-correctness/REPORT.md`

### Expected Results (as of 2026-05-04)

| Metric | Value |
|---|---:|
| Queries run | 22 |
| Passed | 0 |
| Failed | 22 |
| Pass rate | **0.0%** |

All TPC-H queries fail because they use multi-table JOINs with arithmetic expressions (`l_extendedprice * (1 - l_discount)`). The datafusion backend does not yet support the `TIMES` (multiplication) scalar function, which blocks all 22 queries at the planning stage.

---

## Step 6: Stop the cluster

```bash
./mustang-scripts/cluster-stop.sh
```

---

## Troubleshooting

### Node crashes with `NoClassDefFoundError: PPLBuiltinOperators`

**Cause**: Local uncommitted changes in `$OS_REPO/sandbox/plugins/analytics-backend-datafusion/` reference classes from the SQL plugin (`org.opensearch.sql.expression.function.PPLBuiltinOperators`). Due to classloader isolation between plugins, the datafusion backend cannot see SQL plugin classes at runtime.

**Fix**: Ensure `$OS_REPO` is on clean `main`:
```bash
cd $OS_REPO
git stash  # or: git checkout main
```
Then restart the cluster.

### "No backend can scan all requested fields on index [hits]"

The index was created WITHOUT parquet settings. The mapping files in this repo already include parquet settings (`pluggable.dataformat.enabled`, `pluggable.dataformat=composite`, `composite.primary_data_format=parquet`). Delete the index and let the test recreate it:

```bash
curl -X DELETE "http://localhost:9200/hits"
# Re-run the test
```

### Gradle cache corruption

If you see `The contents of the immutable workspace ... have been modified`:
```bash
# Delete the specific corrupted transform (hash from error message)
rm -rf ~/.gradle/caches/9.2.0/transforms/<hash-from-error>
# If that doesn't work, stop all daemons and retry:
./gradlew --stop
# Re-run with --rerun-tasks
```

**Prevention**: This happens when the OpenSearch sandbox rebuild changes jars that Gradle already cached. Always stop Gradle daemons before rebuilding the sandbox:
```bash
./gradlew --stop
# Then rebuild sandbox / restart cluster
```

### Stale test results (report shows old data)

The gradle task may use cached test results from a previous run (especially after a cluster crash). Always clean before rerunning:
```bash
rm -rf integ-test/build/test-results/analyticsSqlClickBenchCorrectness
rm -rf integ-test/build/reports/clickbench-sql-correctness
# Re-run with --rerun-tasks
```

### SQL plugin ZIP not picked up by cluster

The `./gradlew run` task in `$OS_REPO` resolves `opensearch-sql-plugin` from Maven local (`~/.m2`). If you rebuild the SQL plugin but the cluster still uses the old version:

1. Verify the ZIP was published: `ls -la ~/.m2/repository/org/opensearch/plugin/opensearch-sql-plugin/3.7.0.0-SNAPSHOT/`
2. The cluster caches installed plugins in `$OS_REPO/build/testclusters/runTask-0/`. A full stop+start is required — the cluster does NOT hot-reload plugins.
3. If the cluster still uses old code after restart, kill all processes and delete the testcluster cache:
```bash
pkill -f "runTask"; pkill -f "GradleDaemon"
rm -rf $OS_REPO/build/testclusters/runTask-0
./mustang-scripts/cluster-start.sh && ./mustang-scripts/cluster-wait.sh
```

### Node crashes with `NoClassDefFoundError: LevenshteinDistance`

**Cause**: `commons-text` jar is missing at runtime. It's excluded from the SQL plugin bundle (to avoid jar hell with analytics-engine parent) but must be provided by `analytics-engine`.

**Fix**: Ensure `analytics-engine/build.gradle` in `$OS_REPO` has:
```groovy
runtimeOnly "org.apache.commons:commons-text:1.11.0"
```
Then rebuild the sandbox:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-25.jdk/Contents/Home
cd $OS_REPO && ./gradlew :sandbox:plugins:analytics-engine:assemble -Dsandbox.enabled=true
```

### Node crashes with `NoClassDefFoundError: PPLBuiltinOperators`

**Cause**: Local uncommitted changes in `$OS_REPO/sandbox/plugins/analytics-backend-datafusion/` reference classes from the SQL plugin. Due to classloader isolation, the datafusion backend cannot see SQL plugin classes at runtime.

**Fix**: Ensure `$OS_REPO` is on clean `main`:
```bash
cd $OS_REPO
git status --short sandbox/plugins/analytics-backend-datafusion/
git stash  # or: git checkout main
```
Then restart the cluster.

### SQL plugin changes not picked up

After modifying code in `api/`, `core/`, or `plugin/`:
```bash
./mustang-scripts/plugin-build.sh
./mustang-scripts/cluster-stop.sh
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh
```

### Cluster won't start (port in use)

```bash
./mustang-scripts/cluster-stop.sh
# or manually:
pkill -f "GradleDaemon"; pkill -f "runTask"
sleep 3
./mustang-scripts/cluster-start.sh
```

### OpenSearch sandbox changes not picked up

After modifying `$OS_REPO/sandbox/plugins/` (e.g., adding commons-text to analytics-engine):
```bash
# Must use JDK 25 for sandbox builds
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-25.jdk/Contents/Home
cd $OS_REPO
./gradlew :sandbox:plugins:analytics-engine:assemble -Dsandbox.enabled=true
# Then restart cluster (it resolves sandbox plugins from the project build, not maven)
cd $SQL_REPO
./mustang-scripts/cluster-stop.sh
./mustang-scripts/cluster-start.sh && ./mustang-scripts/cluster-wait.sh
```

---

## How the test infrastructure works

### Index creation

The mapping JSON files include parquet settings directly:
- `integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json`
- `integ-test/src/test/resources/tpch/mappings/{customer,lineitem,nation,orders,part,partsupp,region,supplier}_index_mapping.json`

Each includes:
```json
"settings": {
  "index": {
    "number_of_shards": 1,
    "pluggable.dataformat.enabled": true,
    "pluggable.dataformat": "composite",
    "composite.primary_data_format": "parquet"
  }
}
```

### Refresh-safe data loading

The test classes (`SQLClickBenchCorrectnessIT`, `SQLClickBenchIT`, `SQLTpchCorrectnessIT`) override `loadIndex()` to use `org.opensearch.sql.util.TestUtils.loadDataByRestClient()` which handles parquet's refresh policy limitation:
- Uses `refresh=true` (non-blocking)
- If the cluster returns HTTP 400 "true refresh policy is not supported", retries without any refresh policy

### Query routing

The Gradle tasks set `tests.analytics.force_routing=true` which flips the cluster setting `plugins.calcite.analytics.force_routing=true`. This makes `RestUnifiedQueryAction.isAnalyticsIndex()` return `true` for ALL queries, routing them through the analytics engine regardless of index name prefix.
