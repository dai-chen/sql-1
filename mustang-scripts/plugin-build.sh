#!/usr/bin/env bash
# Rebuild the SQL plugin + api module and republish to maven local.
# Also clears a known gradle transform-cache directory that gets corrupted by cluster run.
# Blocks until gradle returns (bounded by gradle itself — typically 5-60s depending on what changed).
# Exit 0 on success, non-zero on build failure.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

[[ -d "$JDK21" ]] || die "JDK 21 not found at $JDK21"
[[ -d "$SQL_REPO" ]] || die "SQL repo not found at $SQL_REPO"

# Clear any gradle transform cache that the cluster-run task corrupts in place. Cluster-run
# extracts the OpenSearch tarball into a cached transform dir and then modifies the contents
# (logs, config, installed plugins). Gradle's immutable-workspace check then fails next build
# with "The contents of the immutable workspace ... have been modified." We find such dirs by
# looking for any transform cache that contains `opensearch-*-SNAPSHOT.zip/` as a directory
# (which means it was extracted) — that's the signature of the corrupted ones.
# Safe: gradle will re-extract on demand.
GRADLE_CACHE="$HOME/.gradle/caches"
if [[ -d "$GRADLE_CACHE" ]]; then
    while IFS= read -r -d '' corrupt; do
        log "Clearing corrupted gradle transform cache: $(dirname "$(dirname "$corrupt")")"
        rm -rf "$(dirname "$(dirname "$corrupt")")"
    done < <(find "$GRADLE_CACHE" -type d -name "opensearch-*-SNAPSHOT.zip" -path "*/transforms/*" -print0 2>/dev/null)
fi

export JAVA_HOME="$JDK21"
export PATH="$JAVA_HOME/bin:$PATH"
cd "$SQL_REPO"

# spotlessApply auto-fixes formatting so the next spotlessCheck passes.
log "Running spotlessApply + bundlePlugin + publishToMavenLocal"
./gradlew :api:spotlessApply :opensearch-sql-plugin:bundlePlugin :opensearch-sql-plugin:publishToMavenLocal \
    -x test -x integTest --parallel --build-cache 2>&1 | tail -20

# Verify the ZIP exists at the expected maven local path.
PLUGIN_ZIP="$HOME/.m2/repository/org/opensearch/plugin/opensearch-sql-plugin/3.7.0.0-SNAPSHOT/opensearch-sql-plugin-3.7.0.0-SNAPSHOT.zip"
if [[ ! -f "$PLUGIN_ZIP" ]]; then
    die "Plugin ZIP missing after publish: $PLUGIN_ZIP"
fi
log "Plugin published: $PLUGIN_ZIP ($(stat -f '%z' "$PLUGIN_ZIP") bytes)"
