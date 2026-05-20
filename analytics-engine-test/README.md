# Analytics Engine IT Compatibility Testing

Run the full SQL V2 + Legacy IT suite against the analytics-engine (DataFusion) path on a local sandbox cluster and generate a bucketed compatibility report — **without modifying SQL plugin production code**.

## Prerequisites

- **JDK 21** — for building the SQL plugin
- **JDK 25** — for running the OpenSearch sandbox cluster (set `JAVA25` env var)
- **Rust toolchain** — for building the native parquet library (one-time)
- **OpenSearch core checkout** on `main` (set `OS_REPO` env var)
- **This SQL plugin repo** checked out

## How It Works

1. Every test index is created with parquet-backed settings (`tests.analytics.parquet_indices=true`)
2. `RestUnifiedQueryAction.isAnalyticsIndex()` detects composite/parquet index metadata and routes data queries to analytics-engine
3. The analytics-engine (DataFusion) executes queries instead of the standard Calcite/DSL path
4. Tests that pass = compatible with analytics-engine; tests that fail = gaps to fill

> **Note:** Metadata, explain, and validation tests that don't hit data indices bypass the analytics-engine path entirely — those passes don't reflect DataFusion compatibility.

## Quick Start

```bash
# Set environment
export OS_REPO=~/path/to/OpenSearch        # OpenSearch core checkout (main branch)
export JAVA25=/usr/lib/jvm/java-25-amazon-corretto  # JDK 25 path

# 1. Build native library (one-time, ~20 min)
cd "$OS_REPO/sandbox/libs/dataformat-native/rust"
cargo build --release

# 2. Publish SQL plugin to maven local
./analytics-engine-test/publish-sql-plugin.sh

# 3. Start cluster (in a separate terminal)
./analytics-engine-test/start-cluster.sh

# 4. Run tests + generate report (in another terminal)
./analytics-engine-test/run-sql-compat-test.sh
```

## Scripts

| Script | Purpose |
|--------|---------|
| `publish-sql-plugin.sh` | Build SQL plugin zip → install to `~/.m2` |
| `start-cluster.sh` | Start sandbox cluster with all 9 plugins (foreground) |
| `run-sql-compat-test.sh` | Run SQL V2 + Legacy ITs → generate report |
| `generate-report.py` | Parse JUnit XML → bucketed markdown report |

## Cluster Plugins (9 total)

| Plugin | Source | Purpose |
|--------|--------|---------|
| `opensearch-job-scheduler` | Maven snapshots | Required by SQL plugin |
| `arrow-base` | Core `:plugins:` | Arrow memory/classloader |
| `arrow-flight-rpc` | Core `:plugins:` | Arrow Flight transport |
| `composite-engine` | Sandbox | Composite index engine |
| `parquet-data-format` | Sandbox | Parquet data format |
| `analytics-engine` | Sandbox | Query routing hub |
| `analytics-backend-datafusion` | Sandbox | DataFusion execution |
| `analytics-backend-lucene` | Sandbox | Lucene committer factory |
| `opensearch-sql-plugin` | Maven local (`~/.m2`) | SQL/PPL plugin under test |

## Key Configuration

| Setting | Where | Purpose |
|---------|-------|---------|
| `tests.analytics.parquet_indices=true` | Gradle sys prop | Inject parquet settings into all test indices |
| `opensearch.experimental.feature.transport.stream.enabled=true` | JVM flag (run.gradle patch) | Enable StreamTransportService for analytics-engine |
| `opensearch.experimental.feature.pluggable.dataformat.enabled=true` | JVM flag (auto-set) | Enable parquet data format |

## Testing a PR

To test a PR's impact on compatibility:

```bash
# Cherry-pick PR commits
git fetch upstream pull/<PR_NUMBER>/head:pr-<PR_NUMBER>
git cherry-pick <commits>

# Rebuild + republish
./analytics-engine-test/publish-sql-plugin.sh

# Restart cluster (Ctrl+C the old one first)
./analytics-engine-test/start-cluster.sh

# Rerun tests
./analytics-engine-test/run-sql-compat-test.sh
```

## Report Location

After running: `integ-test/build/reports/analytics-engine-compatibility/REPORT.md`

## Cleanup

```bash
# Revert run.gradle patch in OpenSearch repo
cd "$OS_REPO" && git checkout gradle/run.gradle

# Remove cherry-picked commits (if any)
git reset --hard HEAD~N

# Delete PR branch
git branch -D pr-<NUMBER>
```

## Known Failure Patterns

| Pattern | Meaning | Count (baseline) |
|---------|---------|-----------------|
| "Other Error" (500) | Analytics-engine doesn't support the query pattern | ~421 |
| "Result Mismatch" | Query runs but returns different results | ~5 |
| "Bad Request" (400) | SQL syntax/feature not supported in unified path | ~3 |
| Skipped (legacy) | Tests skipped due to legacy engine deprecation | ~470 |
