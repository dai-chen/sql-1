#!/usr/bin/env bash
# Run the full analyticsSqlCompatibilityReport against the running cluster.
# Launches in the background (returns IMMEDIATELY) and writes progress to $TEST_LOG.
# Use `./test-status.sh` or `./test-wait.sh` to check progress / block-with-timeout.
#
# Optional first arg: a specific test filter expression (e.g. "org.opensearch.sql.sql.MetricsIT").
# When a filter is given, uses :integTestRemote instead of :analyticsSqlCompatibilityReport for
# faster single-test debugging.
#
# Exit code: 0 if successfully launched (not if tests pass). Use test-status for pass/fail.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

[[ -d "$JDK21" ]] || die "JDK 21 not found at $JDK21"
[[ -d "$SQL_REPO" ]] || die "SQL repo not found at $SQL_REPO"

# Require cluster already up — fail fast rather than running 1400 tests against a dead cluster.
if ! cluster_http_up; then
    die "Cluster is not up on localhost:9200. Run ./cluster-start.sh && ./cluster-wait.sh first."
fi

# Guard: one test run at a time.
if [[ -f "$TEST_PID_FILE" ]]; then
    pid=$(cat "$TEST_PID_FILE")
    if pid_alive "$pid"; then
        die "Test run already in progress (pid=$pid). Wait for it or ./test-stop.sh."
    else
        rm -f "$TEST_PID_FILE"
    fi
fi

FILTER="${1:-}"

export JAVA_HOME="$JDK21"
export PATH="$JAVA_HOME/bin:$PATH"
cd "$SQL_REPO"
rm -f "$TEST_LOG"

if [[ -n "$FILTER" ]]; then
    log "Starting single-test run (filter=$FILTER)"
    nohup ./gradlew :integ-test:integTestRemote \
        -Dtests.rest.cluster=localhost:9200 \
        -Dtests.cluster=localhost:9300 \
        -Dtests.clustername=runTask \
        -Dtests.analytics.force_routing=true \
        --tests "$FILTER" \
        > "$TEST_LOG" 2>&1 &
else
    log "Starting full analyticsSqlCompatibilityReport run"
    nohup ./gradlew :integ-test:analyticsSqlCompatibilityReport \
        -Dtests.rest.cluster=localhost:9200 \
        -Dtests.cluster=localhost:9300 \
        -Dtests.clustername=runTask \
        -PclusterLog="$CLUSTER_LOG" \
        > "$TEST_LOG" 2>&1 &
fi
TEST_PID=$!
echo "$TEST_PID" > "$TEST_PID_FILE"
disown "$TEST_PID" 2>/dev/null || true
log "Test run launched: PID $TEST_PID"
log "Log: $TEST_LOG"
log "Next step: ./test-status.sh or ./test-wait.sh"
echo "$TEST_PID"
