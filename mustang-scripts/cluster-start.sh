#!/usr/bin/env bash
# Start the OpenSearch cluster with analytics-engine plugins, in the background.
# Returns IMMEDIATELY after launching gradle (does NOT wait for readiness).
# Use `./cluster-wait.sh` afterward to block-with-timeout until HTTP is up.
#
# Idempotent: if cluster is already running, this is a no-op (exit 0).
# If a stale pidfile exists but the process is gone, it's cleaned up first.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# --- Check if we already have a running cluster ---
if cluster_http_up; then
    log "Cluster already responding on 9200 — no-op"
    status_summary
    exit 0
fi

# Clean up any stale pidfiles before starting
if [[ -f "$CLUSTER_GRADLE_PID_FILE" ]]; then
    pid=$(cat "$CLUSTER_GRADLE_PID_FILE")
    if ! pid_alive "$pid"; then
        log "Cleaning stale gradle pidfile (pid=$pid no longer alive)"
        rm -f "$CLUSTER_GRADLE_PID_FILE"
    fi
fi

# --- Launch ---
[[ -d "$JDK25" ]] || die "JDK 25 not found at $JDK25"
[[ -d "$OS_REPO" ]] || die "OpenSearch repo not found at $OS_REPO"

log "Starting cluster (logs: $CLUSTER_LOG)"
export JAVA_HOME="$JDK25"
export PATH="$JAVA_HOME/bin:$PATH"
cd "$OS_REPO"
rm -f "$CLUSTER_LOG"

# Install order matters: analytics-engine must come before its extenders (opensearch-sql,
# analytics-backend-datafusion). See plugin-descriptor extended.plugins fields.
nohup ./gradlew run -Dsandbox.enabled=true \
    -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT', 'arrow-flight-rpc', 'analytics-engine', 'analytics-backend-lucene', 'analytics-backend-datafusion', 'parquet-data-format', 'composite-engine', 'opensearch-sql-plugin:3.7.0.0-SNAPSHOT']" \
    -Dtests.jvm.argline="-Djava.library.path=$OS_REPO/sandbox/libs/dataformat-native/rust/target/release -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true" \
    > "$CLUSTER_LOG" 2>&1 &
GRADLE_PID=$!
echo "$GRADLE_PID" > "$CLUSTER_GRADLE_PID_FILE"
disown "$GRADLE_PID" 2>/dev/null || true

log "Gradle launched: PID $GRADLE_PID. Cluster typically ready in 30-60s."
log "Next step: ./cluster-wait.sh"
echo "$GRADLE_PID"
