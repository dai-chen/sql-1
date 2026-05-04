#!/usr/bin/env bash
# Stop the running cluster cleanly. Bounded wait for graceful termination, then SIGKILL.
# Idempotent: safe to run even if nothing is running.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# Stop OpenSearch first so gradle's watcher sees it exit cleanly.
stop_pid "$CLUSTER_PID_FILE" 10
stop_pid "$CLUSTER_GRADLE_PID_FILE" 5

# Reap any leftover opensearch processes we lost track of.
LEFTOVER=$(ps -ef | grep -iE "testclusters/runTask|gradle.*run" | grep -v grep | awk '{print $2}' || true)
if [[ -n "$LEFTOVER" ]]; then
    log "Reaping leftover processes: $LEFTOVER"
    echo "$LEFTOVER" | xargs -r kill -TERM 2>/dev/null || true
    # Give them 3 seconds max, then SIGKILL anything still alive.
    for _ in 1 2 3; do
        STILL=$(ps -ef | grep -iE "testclusters/runTask|gradle.*run" | grep -v grep | awk '{print $2}' || true)
        [[ -z "$STILL" ]] && break
        sleep 1
    done
    STILL=$(ps -ef | grep -iE "testclusters/runTask|gradle.*run" | grep -v grep | awk '{print $2}' || true)
    [[ -n "$STILL" ]] && echo "$STILL" | xargs -r kill -KILL 2>/dev/null || true
fi

status_summary
