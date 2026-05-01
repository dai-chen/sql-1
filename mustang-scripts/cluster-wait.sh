#!/usr/bin/env bash
# Wait for the cluster's HTTP to come up, with a strict bounded timeout.
# If cluster is already up, returns 0 immediately.
# Exit 0 if ready, 1 if timeout, 2 if the gradle process died before readiness.
#
# Default timeout: 120 seconds. Override with CLUSTER_WAIT_TIMEOUT env var.
# Also records the OpenSearch java PID once detected so cluster-stop.sh can shut it down cleanly.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TIMEOUT="${CLUSTER_WAIT_TIMEOUT:-120}"

# Short-circuit: already up?
if cluster_http_up; then
    # Capture OpenSearch java PID if we don't have it yet
    if [[ ! -f "$CLUSTER_PID_FILE" ]]; then
        os_pid=$(ps -ef | grep "testclusters/runTask-0" | grep -v grep | awk '{print $2}' | head -1)
        [[ -n "$os_pid" ]] && echo "$os_pid" > "$CLUSTER_PID_FILE"
    fi
    log "Cluster already up"
    status_summary
    exit 0
fi

# --- Check gradle is still alive before waiting ---
if [[ ! -f "$CLUSTER_GRADLE_PID_FILE" ]]; then
    die "No gradle pidfile at $CLUSTER_GRADLE_PID_FILE — did you run cluster-start.sh?"
fi
GRADLE_PID=$(cat "$CLUSTER_GRADLE_PID_FILE")

predicate() {
    if ! pid_alive "$GRADLE_PID"; then
        log "Gradle PID $GRADLE_PID died before cluster came up — check $CLUSTER_LOG"
        return 1
    fi
    cluster_http_up
}

if wait_for "cluster HTTP 9200" "$TIMEOUT" 3 predicate; then
    # Record OS PID for later shutdown
    os_pid=$(ps -ef | grep "testclusters/runTask-0" | grep -v grep | awk '{print $2}' | head -1)
    [[ -n "$os_pid" ]] && echo "$os_pid" > "$CLUSTER_PID_FILE"
    status_summary
    exit 0
fi

# Wait failed — distinguish process-died vs real timeout
if ! pid_alive "$GRADLE_PID"; then
    log "Gradle process died. Last 20 lines of cluster log:"
    tail -20 "$CLUSTER_LOG" >&2 || true
    exit 2
fi

log "Timeout after ${TIMEOUT}s. Last 20 lines of cluster log:"
tail -20 "$CLUSTER_LOG" >&2 || true
exit 1
