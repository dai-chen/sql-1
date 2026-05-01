#!/usr/bin/env bash
# Common environment and helpers for mustang-sql scripts.
# Source this from other scripts: `source "$(dirname "$0")/lib.sh"`
#
# Design invariants:
#   - No script blocks indefinitely. Every wait has a bounded timeout.
#   - All background processes are tracked via PID files under STATE_DIR.
#   - All scripts are idempotent: re-running them from any state works.
#   - Logs go to files; scripts print short status lines to stdout/stderr.
#   - Non-zero exit code means failure; caller decides what to do.

set -u -o pipefail

# --- Paths ---
# Default SQL_REPO to the parent of this script directory, so scripts work out-of-the-box
# when shipped inside the sql repo itself. Override via env var for custom layouts.
_SCRIPT_DIR_LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export OS_REPO="${OS_REPO:-/Users/daichen/IdeaProjects/OpenSearch}"
export SQL_REPO="${SQL_REPO:-$(cd "$_SCRIPT_DIR_LIB/.." && pwd)}"
export STATE_DIR="${STATE_DIR:-/tmp/sisyphus-cluster}"
export JDK21="${JDK21:-/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home}"
export JDK25="${JDK25:-/Library/Java/JavaVirtualMachines/amazon-corretto-25.jdk/Contents/Home}"

mkdir -p "$STATE_DIR"

# --- PID file locations ---
export CLUSTER_PID_FILE="$STATE_DIR/cluster.pid"
export CLUSTER_GRADLE_PID_FILE="$STATE_DIR/cluster-gradle.pid"
export CLUSTER_LOG="$STATE_DIR/cluster.log"
export TEST_PID_FILE="$STATE_DIR/test.pid"
export TEST_LOG="$STATE_DIR/test.log"

# --- Logging ---
log()  { echo "[$(date +%H:%M:%S)] $*" >&2; }
die()  { log "FATAL: $*"; exit 1; }

# --- Check if a PID is alive (non-blocking, always returns <1s) ---
pid_alive() {
    local pid="${1:-}"
    [[ -z "$pid" ]] && return 1
    kill -0 "$pid" 2>/dev/null
}

# --- Check cluster HTTP, with strict timeout. Non-blocking. Returns 0 if up ---
cluster_http_up() {
    curl -sS --max-time 2 --connect-timeout 1 http://localhost:9200 >/dev/null 2>&1
}

# --- Wait for a condition with bounded timeout.
# Usage: wait_for "description" TIMEOUT_SEC POLL_INTERVAL_SEC predicate_fn
# Returns 0 if predicate became true within timeout, 1 otherwise. NEVER blocks past TIMEOUT_SEC. ---
wait_for() {
    local desc="$1" timeout="$2" interval="$3" predicate="$4"
    local elapsed=0
    while (( elapsed < timeout )); do
        if "$predicate"; then
            log "[wait_for] $desc: ready after ${elapsed}s"
            return 0
        fi
        sleep "$interval"
        elapsed=$(( elapsed + interval ))
    done
    log "[wait_for] $desc: TIMEOUT after ${timeout}s"
    return 1
}

# --- Kill a process tree by PID file, bounded wait for termination.
# Usage: stop_pid <pid-file> <max-wait-sec> ---
stop_pid() {
    local pidfile="$1" maxwait="${2:-10}"
    [[ ! -f "$pidfile" ]] && return 0
    local pid
    pid=$(cat "$pidfile" 2>/dev/null)
    [[ -z "$pid" ]] && { rm -f "$pidfile"; return 0; }
    if ! pid_alive "$pid"; then
        rm -f "$pidfile"; return 0
    fi
    log "[stop_pid] SIGTERM pid=$pid (file=$pidfile)"
    kill -TERM "$pid" 2>/dev/null || true
    local elapsed=0
    while pid_alive "$pid" && (( elapsed < maxwait )); do
        sleep 1; elapsed=$(( elapsed + 1 ))
    done
    if pid_alive "$pid"; then
        log "[stop_pid] SIGKILL pid=$pid (did not exit after ${maxwait}s)"
        kill -KILL "$pid" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$pidfile"
}

# --- Print a short summary of the cluster's state for eyeballing ---
status_summary() {
    echo "Cluster state:"
    if cluster_http_up; then
        echo "  HTTP 9200: UP"
    else
        echo "  HTTP 9200: DOWN"
    fi
    if [[ -f "$CLUSTER_PID_FILE" ]]; then
        local pid; pid=$(cat "$CLUSTER_PID_FILE")
        if pid_alive "$pid"; then
            echo "  cluster PID $pid: alive"
        else
            echo "  cluster PID $pid: DEAD (stale pidfile)"
        fi
    else
        echo "  cluster PID file: none"
    fi
    if [[ -f "$CLUSTER_GRADLE_PID_FILE" ]]; then
        local gp; gp=$(cat "$CLUSTER_GRADLE_PID_FILE")
        if pid_alive "$gp"; then
            echo "  gradle PID $gp: alive"
        else
            echo "  gradle PID $gp: DEAD (stale pidfile)"
        fi
    fi
}
