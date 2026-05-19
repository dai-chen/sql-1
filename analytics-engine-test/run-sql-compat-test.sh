#!/bin/bash
# Runs SQL V2 + Legacy integration tests against a running analytics-engine cluster,
# then generates a bucketed compatibility report.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_REPO="${SQL_REPO:-$(cd "$SCRIPT_DIR/.." && pwd)}"
CLUSTER="${CLUSTER_URL:-localhost:9200}"
RESULTS_DIR="$SQL_REPO/integ-test/build/test-results/integTestRemote"
REPORT_DIR="$SQL_REPO/integ-test/build/reports/analytics-engine-compatibility"

echo "=== SQL V2 + Legacy IT — Analytics Engine Compatibility ==="
echo "SQL Repo: $SQL_REPO"
echo "Cluster:  $CLUSTER"
echo ""

# Verify cluster
if ! curl -sf "http://$CLUSTER/_cluster/health" > /dev/null 2>&1; then
  echo "ERROR: Cluster not reachable at http://$CLUSTER" >&2
  echo "Start it first with: ./start-cluster.sh" >&2
  exit 1
fi

echo "=== Cluster plugins ==="
curl -s "http://$CLUSTER/_cat/plugins?v"
echo ""

# Enable force routing — route ALL queries through analytics-engine
echo "=== Enabling analytics force routing ==="
curl -sf -X PUT "http://$CLUSTER/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"plugins.calcite.analytics.force_routing":true}}'
echo ""

# Run tests
echo ""
echo "=== Running SQL V2 + Legacy ITs ==="
cd "$SQL_REPO"

./gradlew :integ-test:integTestRemote \
  -Dtests.rest.cluster="$CLUSTER" \
  -Dtests.cluster="$CLUSTER" \
  -Dtests.clustername=runTask \
  -Dtests.analytics.parquet_indices=true \
  --tests "org.opensearch.sql.sql.*" \
  --tests "org.opensearch.sql.legacy.*" \
  --rerun-tasks || true

# Generate report
echo ""
echo "=== Generating compatibility report ==="
mkdir -p "$REPORT_DIR"
python3 "$SCRIPT_DIR/generate-report.py" "$RESULTS_DIR" "$REPORT_DIR/REPORT.md"

echo ""
echo "✅ Report: $REPORT_DIR/REPORT.md"
