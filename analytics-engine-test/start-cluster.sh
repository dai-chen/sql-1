#!/bin/bash
# Starts a local OpenSearch sandbox cluster with analytics-engine + SQL plugin.
# Runs in foreground — use another terminal for tests. Ctrl+C to stop.
set -euo pipefail

OS_REPO="${OS_REPO:?Set OS_REPO to your OpenSearch core checkout}"
JAVA25="${JAVA25:-/usr/lib/jvm/java-25-amazon-corretto}"

if [[ ! -d "$OS_REPO/sandbox" ]]; then
  echo "ERROR: $OS_REPO does not look like an OpenSearch repo (no sandbox/ dir)" >&2
  exit 1
fi
if [[ ! -d "$JAVA25" ]]; then
  echo "ERROR: JDK 25 not found at $JAVA25. Set JAVA25 env var." >&2
  exit 1
fi

echo "=== Starting OpenSearch cluster with analytics-engine + SQL plugin ==="
echo "Repo: $OS_REPO"
echo "JDK:  $JAVA25"
echo ""
echo "Plugins: opensearch-job-scheduler, arrow-base, arrow-flight-rpc,"
echo "         composite-engine, parquet-data-format, analytics-engine,"
echo "         analytics-backend-datafusion, analytics-backend-lucene,"
echo "         opensearch-sql-plugin (from ~/.m2)"
echo ""
echo "Cluster: http://localhost:9200  |  Ctrl+C to stop"
echo ""

cd "$OS_REPO"

# Patch run.gradle for stream transport if needed (analytics-engine requires it)
RUN_GRADLE="gradle/run.gradle"
if ! grep -q "transport.stream.enabled" "$RUN_GRADLE"; then
  echo "[patch] Adding transport.stream.enabled to run.gradle..."
  sed -i "/systemProperty 'io.netty.tryReflectionSetAccessible', 'true'/a\\          systemProperty 'opensearch.experimental.feature.transport.stream.enabled', 'true'" "$RUN_GRADLE"
  echo "[patch] Done. Revert later with: git checkout gradle/run.gradle"
fi

exec env JAVA_HOME="$JAVA25" ./gradlew run \
  -Dtests.jvm.argline="-da -dsa" \
  -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler','arrow-base','arrow-flight-rpc','composite-engine','parquet-data-format','analytics-engine','analytics-backend-datafusion','analytics-backend-lucene','opensearch-sql-plugin:3.8.0.0-SNAPSHOT']"
