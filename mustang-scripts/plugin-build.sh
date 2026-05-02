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

# NOTE: we intentionally do NOT scan for and delete corrupted gradle transform caches here.
# Earlier versions of this script did, but the heuristic also deleted caches that the
# integTestRemote/analyticsSqlCompatibilityTest tasks depend on — which then fail with
# "missing: .../opensearch-*-SNAPSHOT.zip" and gradle refuses to re-extract. If you hit an
# "immutable workspace has been modified" error, run:
#     pkill -f GradleDaemon; rm -rf ~/.gradle/caches/*/transforms
# That clears all transforms; gradle will regenerate them on the next run.

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
