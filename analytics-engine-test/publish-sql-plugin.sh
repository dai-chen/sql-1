#!/bin/bash
# Builds the SQL plugin zip and installs it to ~/.m2 for OpenSearch run.gradle resolution.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_REPO="${SQL_REPO:-$(cd "$SCRIPT_DIR/.." && pwd)}"
VERSION="${SQL_VERSION:-3.7.0.0-SNAPSHOT}"
ARTIFACT="opensearch-sql-plugin"
GROUP_PATH="org/opensearch/plugin"
M2_DIR="$HOME/.m2/repository/$GROUP_PATH/$ARTIFACT/$VERSION"

echo "=== Building SQL plugin ==="
cd "$SQL_REPO"
./gradlew :opensearch-sql-plugin:bundlePlugin

# The zip is named opensearch-sql-<version>.zip (not opensearch-sql-plugin-)
ZIP="plugin/build/distributions/opensearch-sql-$VERSION.zip"
if [[ ! -f "$ZIP" ]]; then
  echo "ERROR: Expected zip not found at $ZIP" >&2
  echo "Available:" >&2
  ls plugin/build/distributions/ >&2
  exit 1
fi

echo "=== Installing to Maven local ==="
mkdir -p "$M2_DIR"
cp "$ZIP" "$M2_DIR/$ARTIFACT-$VERSION.zip"

cat > "$M2_DIR/$ARTIFACT-$VERSION.pom" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.opensearch.plugin</groupId>
  <artifactId>$ARTIFACT</artifactId>
  <version>$VERSION</version>
  <packaging>zip</packaging>
</project>
EOF

rm -f "$M2_DIR/$ARTIFACT-$VERSION.module"
rm -f "$M2_DIR/_remote.repositories"

echo "✅ Published $ARTIFACT-$VERSION.zip to $M2_DIR"
