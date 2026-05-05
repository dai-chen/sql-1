#!/usr/bin/env bash
# daily-report.sh — One-click script to generate all analytics-engine test reports.
#
# Usage:
#   ./mustang-scripts/daily-report.sh
#
# Prerequisites:
#   - ./mustang-scripts/preflight.sh passes
#   - $OS_REPO sandbox has commons-text in analytics-engine (see SOP)
#
# Output:
#   - docs/test-reports/YYYY-MM-DD-clickbench-sql.md
#   - docs/test-reports/YYYY-MM-DD-tpch-sql.md
#   - docs/test-reports/YYYY-MM-DD-ansi-sql-iq.md

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

DATE=$(date +%Y-%m-%d)
REPORT_DIR="$SQL_REPO/docs/test-reports"
mkdir -p "$REPORT_DIR"

# --- Step 1: Clean state ---
log "Stopping any running cluster..."
"$SCRIPT_DIR/cluster-stop.sh" 2>/dev/null || true
sleep 2

log "Stopping Gradle daemons (prevents cache corruption)..."
cd "$SQL_REPO" && ./gradlew --stop 2>/dev/null || true

# Clean corrupted Gradle transform caches if they exist
rm -rf ~/.gradle/caches/9.2.0/transforms/c747b0fe296b3efc33fb0cf420870387 2>/dev/null || true

# --- Step 2: Build SQL plugin ---
log "Building SQL plugin..."
"$SCRIPT_DIR/plugin-build.sh"

# --- Step 3: Start cluster ---
log "Starting cluster..."
"$SCRIPT_DIR/cluster-start.sh"
"$SCRIPT_DIR/cluster-wait.sh"

# --- Step 4: Clean old test results ---
log "Cleaning stale test results..."
rm -rf "$SQL_REPO/integ-test/build/test-results/analyticsSqlClickBenchCorrectness"
rm -rf "$SQL_REPO/integ-test/build/test-results/analyticsSqlTpchCorrectness"
rm -rf "$SQL_REPO/integ-test/build/test-results/analyticsAnsiQuidem"
rm -rf "$SQL_REPO/integ-test/build/reports/clickbench-sql-correctness"
rm -rf "$SQL_REPO/integ-test/build/reports/tpch-sql-correctness"
rm -rf "$SQL_REPO/integ-test/build/reports/ansi-sql-quidem"
rm -rf "$SQL_REPO/integ-test/src/test/resources/expectedOutput/clickbench-sql"
rm -rf "$SQL_REPO/integ-test/src/test/resources/expectedOutput/tpch-sql"

# --- Step 5: Run ClickBench ---
log "Running ClickBench SQL correctness test..."
cd "$SQL_REPO"
./gradlew :integ-test:analyticsSqlClickBenchCorrectnessReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -Dtests.snapshot.write=true \
  --rerun-tasks || true

if [[ -f integ-test/build/reports/clickbench-sql-correctness/REPORT.md ]]; then
  cp integ-test/build/reports/clickbench-sql-correctness/REPORT.md "$REPORT_DIR/$DATE-clickbench-sql.md"
  log "ClickBench report: $REPORT_DIR/$DATE-clickbench-sql.md"
else
  log "WARNING: ClickBench report not generated"
fi

# --- Step 6: Run TPC-H ---
log "Running TPC-H SQL correctness test..."
./gradlew :integ-test:analyticsSqlTpchCorrectnessReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -Dtests.snapshot.write=true \
  --rerun-tasks || true

if [[ -f integ-test/build/reports/tpch-sql-correctness/REPORT.md ]]; then
  cp integ-test/build/reports/tpch-sql-correctness/REPORT.md "$REPORT_DIR/$DATE-tpch-sql.md"
  log "TPC-H report: $REPORT_DIR/$DATE-tpch-sql.md"
else
  log "WARNING: TPC-H report not generated"
fi

# --- Step 7: Run ANSI SQL IQ ---
log "Running ANSI SQL Quidem IQ test..."
./gradlew :integ-test:analyticsAnsiQuidemReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  --rerun-tasks || true

if [[ -f integ-test/build/reports/ansi-sql-quidem/REPORT.md ]]; then
  cp integ-test/build/reports/ansi-sql-quidem/REPORT.md "$REPORT_DIR/$DATE-ansi-sql-iq.md"
  log "IQ report: $REPORT_DIR/$DATE-ansi-sql-iq.md"
else
  log "WARNING: IQ report not generated"
fi

# --- Step 8: Stop cluster ---
log "Stopping cluster..."
"$SCRIPT_DIR/cluster-stop.sh"

# --- Summary ---
echo ""
echo "============================================"
echo "  Daily Report — $DATE"
echo "============================================"
echo ""
for f in "$REPORT_DIR/$DATE"*.md; do
  if [[ -f "$f" ]]; then
    name=$(basename "$f" .md | sed "s/$DATE-//")
    # Extract pass rate from report (handles different formats)
    rate=$(grep -oE '[0-9]+/[0-9]+.*\([0-9.]+%\)' "$f" | head -1)
    if [[ -z "$rate" ]]; then
      rate=$(grep -oE 'Snapshots written \| [0-9]+' "$f" | head -1 | grep -oE '[0-9]+')
      total=$(grep -oE 'Total queries \| [0-9]+' "$f" | head -1 | grep -oE '[0-9]+')
      if [[ -n "$rate" && -n "$total" ]]; then
        rate="$rate/$total passed"
      else
        rate="see report"
      fi
    fi
    echo "  $name: $rate"
  fi
done
echo ""
echo "Reports saved to: $REPORT_DIR/"
echo "============================================"
