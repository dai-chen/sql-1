# SOP: SQL Compatibility Test Against OpenSearch Cluster

Standard operating procedure for running the SQL compatibility test suite through the analytics-engine path on a local OpenSearch cluster.

## Prerequisites

| Requirement | Purpose |
|---|---|
| JDK 21 (Amazon Corretto) | SQL plugin build |
| JDK 25 (Amazon Corretto) | OpenSearch sandbox runtime |
| OpenSearch sandbox repo (`~/IdeaProjects/OpenSearch`) | Cluster with analytics-engine plugins |
| SQL plugin repo (this repo) | `feature/mustang-sql-it-tooling` branch |

Ensure environment is validated:

```bash
./mustang-scripts/preflight.sh
```

---

## Step 1: Ensure OpenSearch repo is clean

```bash
cd ~/IdeaProjects/OpenSearch
git status --short sandbox/plugins/analytics-backend-datafusion/
# Should show nothing. If dirty:
git stash
```

---

## Step 2: Build the SQL plugin

```bash
cd ~/IdeaProjects/my-sql-repo
./mustang-scripts/plugin-build.sh
```

Publishes `opensearch-sql-plugin-3.7.0.0-SNAPSHOT.zip` to `~/.m2`.

---

## Step 3: Start the cluster

```bash
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh
```

Verify plugins are loaded:

```bash
curl -s http://localhost:9200/_cat/plugins?v
```

Expected: 7 plugins including `opensearch-sql`, `analytics-engine`, `analytics-backend-datafusion`, `analytics-backend-lucene`, `composite-engine`, `parquet-data-format`, `opensearch-job-scheduler`.

---

## Step 4: Run SQL compatibility test

```bash
./gradlew :integ-test:analyticsSqlCompatibilityReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  --rerun-tasks
```

This runs all SQL IT classes through the analytics-engine path (force-routing enabled) and generates a markdown report.

**Report location**: `integ-test/build/reports/analytics-sql-compatibility/REPORT.md`

---

## Step 5: Stop the cluster

```bash
./mustang-scripts/cluster-stop.sh
```

---

## Reading the Report

The report contains:

- **Summary table**: total tests, passed, failed, skipped, pass rate
- **Top failure buckets**: grouped by error type with origin classification
- **Per-class pass-rate**: which IT classes pass fully, partially, or fail entirely

### Origin legend

| Origin | Meaning |
|---|---|
| SQL/calcite | SQL plugin's Calcite visitors |
| SQL/preflight | SQL plugin's parser/analyzer |
| AE | Analytics-engine plugin |
| Calcite | Apache Calcite library |
| OpenSearch | OpenSearch core |
| Test | Test assertion failure |
| ? | No matching cluster-log frame |

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Cluster won't start (port in use) | `./mustang-scripts/cluster-stop.sh` then retry |
| `NoClassDefFoundError: PPLBuiltinOperators` | Ensure `$OS_REPO` is on clean `main` — no local changes in `analytics-backend-datafusion/` |
| `NoClassDefFoundError: LevenshteinDistance` | Add `runtimeOnly "org.apache.commons:commons-text:1.11.0"` to `analytics-engine/build.gradle` in OS repo |
| Stale results | Delete `integ-test/build/test-results/analyticsSqlCompatibility/` and rerun with `--rerun-tasks` |
| Plugin changes not picked up | Rebuild plugin, stop cluster, start cluster |
| Test skipped (no cluster) | Ensure `-Dtests.rest.cluster=localhost:9200` is passed |

---

## Quick Reference (Copy-Paste)

```bash
# Full run from scratch
./mustang-scripts/plugin-build.sh
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh

./gradlew :integ-test:analyticsSqlCompatibilityReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  --rerun-tasks

# View report
cat integ-test/build/reports/analytics-sql-compatibility/REPORT.md

./mustang-scripts/cluster-stop.sh
```
