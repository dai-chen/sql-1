# Analytics SQL IT Compatibility Report — Runbook

Run SQL V2/legacy integration tests against the Analytics Engine and generate a failure breakdown report.

## Prerequisites

| Requirement | Purpose |
|---|---|
| JDK 21 (Amazon Corretto) | SQL plugin build |
| JDK 25 (Amazon Corretto) | OpenSearch sandbox runtime |
| Rust/cargo | Native parquet library (one-time build) |
| OpenSearch sandbox repo | Contains analytics-engine, composite-engine, parquet-data-format |

### Environment

Set `OS_REPO` to point at your OpenSearch sandbox-enabled checkout (auto-discovered by `mustang-scripts/lib.sh`):

```bash
export OS_REPO=~/IdeaProjects/OpenSearch   # adjust to your path
```

## Full Run

```bash
# 0. Pull latest main (get new SQL ITs from upstream)
git fetch origin main && git merge origin/main

# 1. Rebuild OpenSearch sandbox plugins
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-25.jdk/Contents/Home
cd "$OS_REPO"
git checkout main && git pull
./gradlew --stop
./gradlew :sandbox:plugins:analytics-engine:assemble \
  :sandbox:plugins:analytics-backend-datafusion:assemble \
  :sandbox:plugins:analytics-backend-lucene:assemble \
  :sandbox:plugins:composite-engine:assemble \
  :plugins:arrow-flight-rpc:assemble \
  -Dsandbox.enabled=true

# 2. Build SQL plugin
cd "$SQL_REPO"   # or cd back to this repo root
./mustang-scripts/plugin-build.sh

# 3. Start cluster
./mustang-scripts/cluster-stop.sh 2>/dev/null
rm -rf "$OS_REPO/build/testclusters/runTask-0"
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh

# 4. Verify 8 plugins
curl -s http://localhost:9200/_cat/plugins?v

# 5. Run the compatibility report
./gradlew :integ-test:analyticsSqlCompatibilityReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -PclusterLog="$OS_REPO/build/testclusters/runTask-0/logs/runTask.log" \
  --rerun-tasks

# 6. Generate detailed breakdown
./mustang-scripts/analyze-report.sh

# 7. Stop cluster
./mustang-scripts/cluster-stop.sh
```

## Quick Re-run (cluster already running)

```bash
./gradlew :integ-test:analyticsSqlCompatibilityReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -PclusterLog="$OS_REPO/build/testclusters/runTask-0/logs/runTask.log" \
  --rerun-tasks
./mustang-scripts/analyze-report.sh
```

## Quick Re-run (SQL plugin changed)

```bash
./mustang-scripts/plugin-build.sh
./mustang-scripts/cluster-stop.sh
rm -rf "$OS_REPO/build/testclusters/runTask-0"
./mustang-scripts/cluster-start.sh && ./mustang-scripts/cluster-wait.sh
./gradlew :integ-test:analyticsSqlCompatibilityReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -PclusterLog="$OS_REPO/build/testclusters/runTask-0/logs/runTask.log" \
  --rerun-tasks
./mustang-scripts/analyze-report.sh
```

## Report Locations

| Report | Path |
|--------|------|
| Auto-generated markdown | `integ-test/build/reports/analytics-sql-compatibility/REPORT.md` |
| Detailed breakdown | stdout of `./mustang-scripts/analyze-report.sh` |
| HTML (browsable) | `integ-test/build/reports/tests/analyticsSqlCompatibility/index.html` |
| JUnit XML | `integ-test/build/test-results/analyticsSqlCompatibility/*.xml` |

## How It Works

1. `analyticsSqlCompatibilityTest` runs all SQL V2/legacy ITs against the external cluster
2. Sets `tests.analytics.force_routing=true` → enables Calcite, sets `plugins.calcite.analytics.force_routing=true`
3. Creates a dual index for FROM-less queries, injects parquet+lucene composite settings
4. Routes ALL queries through the analytics-engine path
5. `analyticsSqlCompatibilityReport` parses JUnit XML + cluster log → auto-generated REPORT.md
6. `analyze-report.sh` parses JUnit XML → detailed Owner/Root Cause breakdown

## Critical: cluster-start.sh JVM Flags

The cluster MUST start with these flags (already configured in `mustang-scripts/cluster-start.sh`):

```
-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true
-Dopensearch.experimental.feature.transport.stream.enabled=true
-da
```

**`-da` (disable assertions) is REQUIRED.** Without it, an assertion in OpenSearch's transport handler registration fires and the cluster crashes.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| "transport handlers already registered" | Ensure `-da` in JVM args. Kill: `pkill -f GradleDaemon; pkill -f runTask` |
| "Missing plugin [arrow-flight-rpc]" | Build it: `cd $OS_REPO && ./gradlew :plugins:arrow-flight-rpc:assemble` |
| Cluster won't start (port in use) | `./mustang-scripts/cluster-stop.sh; pkill -f runTask; sleep 3` |
| Stale results | `--rerun-tasks` or `rm -rf integ-test/build/test-results/analyticsSqlCompatibility` |
| Plugin not picked up | Stop+start cluster + `rm -rf $OS_REPO/build/testclusters/runTask-0` |
| NoClassDefFoundError: PPLBuiltinOperators | `cd $OS_REPO && git stash` (clean main, no local changes) |
| Report shows 96% "?" origin | Pass `-PclusterLog=$OS_REPO/build/testclusters/runTask-0/logs/runTask.log` |
| Gradle cache corruption | `./gradlew --stop; rm -rf ~/.gradle/caches/9.*/transforms/` |
