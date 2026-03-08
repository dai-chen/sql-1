#!/usr/bin/env bash
# ppl_it_rate.sh — Run CalcitePPL*IT tests with V4 enabled and report pass rate.
# Usage: ./scripts/ppl_it_rate.sh [--class CalcitePPLBasicIT] [--dry-run]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORT_DIR="$ROOT_DIR/build/ppl-v4-reports"
mkdir -p "$REPORT_DIR"

CLASS_FILTER="org.opensearch.sql.calcite.remote.CalcitePPL*IT"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --class) CLASS_FILTER="org.opensearch.sql.calcite.remote.$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if $DRY_RUN; then
  echo "[dry-run] Would run: ./gradlew :integ-test:integTest --tests \"$CLASS_FILTER\" -Dppl.engine.v4=true --continue"
  exit 0
fi

echo "=== PPL V4 Integration Test Rate ==="
echo "Filter: $CLASS_FILTER"
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

cd "$ROOT_DIR"

# Run tests, capture exit code but don't fail script
set +e
./gradlew :integ-test:integTest \
  --tests "$CLASS_FILTER" \
  -Dppl.engine.v4=true \
  -Dtests.calcite.pushdown.enabled=false \
  --continue \
  2>&1 | tee "$REPORT_DIR/last-run.log"
TEST_EXIT=$?
set -e

# Parse JUnit XML reports
XML_DIR="$ROOT_DIR/integ-test/build/test-results/integTest"
if [[ ! -d "$XML_DIR" ]]; then
  echo "ERROR: No test results found at $XML_DIR"
  exit 1
fi

TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0
ERRORS=0
FAIL_LIST=""

for xml in "$XML_DIR"/TEST-org.opensearch.sql.calcite.remote.CalcitePPL*.xml; do
  [[ -f "$xml" ]] || continue
  # Extract counts from <testsuite> attributes
  t=$(grep -oP 'tests="\K[0-9]+' "$xml" | head -1)
  f=$(grep -oP 'failures="\K[0-9]+' "$xml" | head -1)
  e=$(grep -oP 'errors="\K[0-9]+' "$xml" | head -1)
  s=$(grep -oP 'skipped="\K[0-9]+' "$xml" | head -1)
  t=${t:-0}; f=${f:-0}; e=${e:-0}; s=${s:-0}

  TOTAL=$((TOTAL + t))
  FAILED=$((FAILED + f))
  ERRORS=$((ERRORS + e))
  SKIPPED=$((SKIPPED + s))

  # Collect failing test names
  if [[ $((f + e)) -gt 0 ]]; then
    class=$(basename "$xml" .xml | sed 's/^TEST-//')
    fails=$(grep -oP 'testcase.*?name="\K[^"]+' "$xml" | while read name; do
      if grep -q "testcase.*name=\"$name\"" "$xml" && grep -A1 "name=\"$name\"" "$xml" | grep -q '<failure\|<error'; then
        echo "  FAIL: $class#$name"
      fi
    done)
    FAIL_LIST="$FAIL_LIST$fails"$'\n'
  fi
done

PASSED=$((TOTAL - FAILED - ERRORS - SKIPPED))
if [[ $TOTAL -gt 0 ]]; then
  RATE=$(awk "BEGIN {printf \"%.1f\", ($PASSED/$TOTAL)*100}")
else
  RATE="0.0"
fi

TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Summary
echo ""
echo "========================================="
echo "  PPL V4 Test Results — $TIMESTAMP"
echo "========================================="
echo "  Total:   $TOTAL"
echo "  Passed:  $PASSED"
echo "  Failed:  $FAILED"
echo "  Errors:  $ERRORS"
echo "  Skipped: $SKIPPED"
echo "  Rate:    ${RATE}%"
echo "========================================="

if [[ -n "$FAIL_LIST" ]]; then
  echo ""
  echo "Failing tests:"
  echo "$FAIL_LIST" | head -50
  if [[ $(echo "$FAIL_LIST" | wc -l) -gt 50 ]]; then
    echo "  ... and more (see $REPORT_DIR/failures.txt)"
  fi
fi

# Write machine-readable summary
cat > "$REPORT_DIR/summary.json" <<EOF
{
  "timestamp": "$TIMESTAMP",
  "filter": "$CLASS_FILTER",
  "total": $TOTAL,
  "passed": $PASSED,
  "failed": $FAILED,
  "errors": $ERRORS,
  "skipped": $SKIPPED,
  "rate": $RATE
}
EOF

# Write failures list
echo "$FAIL_LIST" > "$REPORT_DIR/failures.txt"

echo ""
echo "Reports saved to: $REPORT_DIR/"
echo "  summary.json  — machine-readable results"
echo "  failures.txt  — failing test names"
echo "  last-run.log  — full gradle output"
