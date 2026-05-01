#!/usr/bin/env bash
# Status summary for the whole environment: cluster + test run state. Always returns in <2s.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

echo "=== State dir: $STATE_DIR ==="
status_summary
echo ""
echo "=== Test run ==="
"$SCRIPT_DIR/test-status.sh" || true
