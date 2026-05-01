# mustang-sql helper scripts

Set of small, composable shell scripts for running the mustang SQL compatibility test suite.
**Strict invariant**: every script either returns immediately or has a bounded timeout. None
will hang forever.

## Location

This directory (`mustang-scripts/`) ships inside the SQL repo so the scripts default to using
the enclosing repo as `SQL_REPO`. Override via env var if your layout differs.

## Paths (override with env vars)

- `OS_REPO` - OpenSearch repo (default: `~/IdeaProjects/OpenSearch`)
- `SQL_REPO` - this SQL repo (default: parent of `mustang-scripts/`, i.e. the repo root)
- `STATE_DIR` - where pid/log files live (default: `/tmp/sisyphus-cluster`)
- `JDK21`, `JDK25` - paths to Corretto 21 and 25

## Typical workflow

```bash
cd mustang-scripts/

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
