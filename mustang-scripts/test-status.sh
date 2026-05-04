#!/usr/bin/env bash
# Print current status of a test run. Always returns within <2s. Never blocks.
# Exit 0 if test completed successfully (BUILD SUCCESSFUL).
# Exit 1 if test completed with failures (BUILD FAILED).
# Exit 2 if test is still running.
# Exit 3 if no test has been started.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

if [[ ! -f "$TEST_PID_FILE" ]]; then
    echo "No test run pid file"
    exit 3
fi
TEST_PID=$(cat "$TEST_PID_FILE")

if pid_alive "$TEST_PID"; then
    echo "Test run: RUNNING  pid=$TEST_PID  elapsed=$(ps -p "$TEST_PID" -o etime= 2>/dev/null | tr -d ' ')"
    XMLS=$(ls "$SQL_REPO"/integ-test/build/test-results/analyticsSqlCompatibility/*.xml 2>/dev/null | wc -l | tr -d ' ')
    echo "  JUnit XMLs written so far: $XMLS"
    echo "  Last log line: $(tail -1 "$TEST_LOG" 2>/dev/null | head -c 200)"
    exit 2
fi

# Process has exited. Determine success/failure.
echo "Test run: FINISHED  pid=$TEST_PID"
grep -E "tests completed|BUILD SUCCESSFUL|BUILD FAILED|analyticsSqlCompat.*passed" "$TEST_LOG" 2>/dev/null | tail -5
if grep -q "BUILD SUCCESSFUL" "$TEST_LOG" 2>/dev/null; then
    exit 0
fi
exit 1
