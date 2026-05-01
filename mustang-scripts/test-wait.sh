#!/usr/bin/env bash
# Block (with a bounded timeout) for a test run to complete.
# Default timeout: 600s (10 min). Override with TEST_WAIT_TIMEOUT env var.
# Exit code matches test-status.sh (0=success, 1=build failed, 2=timeout, 3=no run).

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

TIMEOUT="${TEST_WAIT_TIMEOUT:-600}"

if [[ ! -f "$TEST_PID_FILE" ]]; then
    echo "No test run pid file" >&2
    exit 3
fi
TEST_PID=$(cat "$TEST_PID_FILE")

predicate() { ! pid_alive "$TEST_PID"; }

if wait_for "test run (pid=$TEST_PID)" "$TIMEOUT" 10 predicate; then
    # Completed — defer to test-status for pass/fail classification.
    "$SCRIPT_DIR/test-status.sh"
    exit $?
fi

# Timeout while still running
echo "Test run still in progress after ${TIMEOUT}s timeout (pid=$TEST_PID)" >&2
"$SCRIPT_DIR/test-status.sh" || true
exit 2
