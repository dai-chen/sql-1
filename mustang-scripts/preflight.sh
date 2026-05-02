#!/usr/bin/env bash
# Check all prerequisites for running the mustang SQL IT suite. Fails fast with actionable
# error messages if anything is missing. Run this FIRST on a new machine.
#
# Exit 0 if environment is ready. Non-zero if any check fails.
# Always returns within ~5s.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

FAIL=0
pass() { echo "  [ok]   $*"; }
fail() { echo "  [FAIL] $*" >&2; FAIL=1; }
warn() { echo "  [warn] $*" >&2; }

echo "=== Tooling ==="
for cmd in git curl bash find tar; do
    if command -v "$cmd" >/dev/null 2>&1; then
        pass "$cmd: $(command -v "$cmd")"
    else
        fail "$cmd: NOT FOUND — install via package manager"
    fi
done

# Rust/cargo needed to build the native dataformat library that analytics-engine loads.
# Only flagged as warn because if the library is already built in $OS_REPO, we don't need cargo.
if command -v cargo >/dev/null 2>&1; then
    pass "cargo: $(cargo --version)"
else
    warn "cargo: NOT FOUND — needed to build sandbox/libs/dataformat-native on first run"
    warn "  install: https://rustup.rs/  or  yum install -y rustc cargo"
fi

echo ""
echo "=== JDKs ==="
if [[ -n "$JDK21" && -x "$JDK21/bin/java" ]]; then
    pass "JDK 21: $JDK21"
else
    fail "JDK 21 not found. Install Amazon Corretto 21 and re-run, or set JDK_21 env var."
    fail "  linux: yum install -y java-21-amazon-corretto-devel"
    fail "  macos: brew install --cask corretto@21"
fi
if [[ -n "$JDK25" && -x "$JDK25/bin/java" ]]; then
    pass "JDK 25: $JDK25"
else
    fail "JDK 25 not found. sandbox:libs:dataformat-native needs --release 25 (FFM API)."
    fail "  install: https://corretto.aws/ or download from OpenJDK"
    fail "  or set JDK_25 env var pointing to a local install"
fi

echo ""
echo "=== OpenSearch sandbox repo ==="
if [[ -d "$OS_REPO" ]]; then
    pass "OS_REPO: $OS_REPO"
    if [[ -d "$OS_REPO/sandbox/plugins/analytics-engine" ]]; then
        pass "sandbox/plugins/analytics-engine present"
    else
        fail "sandbox/plugins/analytics-engine missing — this is NOT the sandbox-enabled fork"
    fi
    if [[ -d "$OS_REPO/sandbox/libs/dataformat-native" ]]; then
        pass "sandbox/libs/dataformat-native present"
        # Check if native lib is built (look for actual shared-library extensions, not .d debug files)
        native_dir="$OS_REPO/sandbox/libs/dataformat-native/rust/target/release"
        native_lib=$(ls "$native_dir"/libopensearch_native.so "$native_dir"/libopensearch_native.dylib "$native_dir"/opensearch_native.dll 2>/dev/null | head -1)
        if [[ -n "$native_lib" ]]; then
            pass "native lib built: $native_lib"
        else
            warn "native lib NOT built yet. First run will trigger cargo build (needs rust/cargo)"
            warn "  or build manually: cd $OS_REPO/sandbox/libs/dataformat-native/rust && cargo build --release"
        fi
    else
        fail "sandbox/libs/dataformat-native missing"
    fi
else
    fail "OS_REPO not found: $OS_REPO"
    fail "  Clone the sandbox-enabled OpenSearch fork and set OS_REPO env var."
    fail "  This is an Amazon-internal fork; obtain the URL from your team."
fi

echo ""
echo "=== SQL repo ==="
if [[ -d "$SQL_REPO" ]]; then
    pass "SQL_REPO: $SQL_REPO"
    if [[ -f "$SQL_REPO/gradlew" ]]; then
        pass "gradlew found"
    else
        fail "SQL_REPO has no gradlew — is this the right directory?"
    fi
    if [[ -f "$SQL_REPO/libs/analytics-engine-3.7.0-SNAPSHOT.zip" ]]; then
        pass "libs/analytics-engine-3.7.0-SNAPSHOT.zip present"
    else
        warn "libs/analytics-engine-3.7.0-SNAPSHOT.zip missing (shipped in the mustang branch)"
    fi
else
    fail "SQL_REPO not found: $SQL_REPO"
fi

echo ""
echo "=== Port availability ==="
if lsof -iTCP:9200 -sTCP:LISTEN >/dev/null 2>&1; then
    warn "port 9200 already in use — will conflict with testcluster. Run ./cluster-stop.sh or kill the listener."
else
    pass "port 9200: free"
fi
if lsof -iTCP:9300 -sTCP:LISTEN >/dev/null 2>&1; then
    warn "port 9300 already in use — will conflict with testcluster transport"
else
    pass "port 9300: free"
fi

echo ""
echo "=== Disk / memory ==="
# Require ~8GB free disk in $HOME/.gradle and in SQL_REPO
free_gb_gradle=$(df -k "$HOME/.gradle" 2>/dev/null | awk 'NR==2 {printf "%d", $4/1024/1024}')
if [[ -n "$free_gb_gradle" && "$free_gb_gradle" -ge 8 ]]; then
    pass "free disk ($HOME/.gradle): ${free_gb_gradle}G"
else
    warn "free disk ($HOME/.gradle): ${free_gb_gradle:-unknown}G (recommend ≥8G for gradle caches)"
fi

# Memory: OpenSearch + gradle daemons need at least ~4GB free
if command -v free >/dev/null 2>&1; then
    avail_gb=$(free -g 2>/dev/null | awk '/^Mem:/ {print $7}')
    if [[ -n "$avail_gb" && "$avail_gb" -ge 4 ]]; then
        pass "available memory: ${avail_gb}G"
    else
        warn "available memory: ${avail_gb:-unknown}G (recommend ≥4G)"
    fi
elif command -v vm_stat >/dev/null 2>&1; then
    # macOS: pages * 4KB
    avail_pages=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); print $3}')
    if [[ -n "$avail_pages" ]]; then
        avail_mb=$(( avail_pages * 4 / 1024 ))
        pass "available memory: ~${avail_mb}M (macOS)"
    fi
fi

echo ""
if (( FAIL != 0 )); then
    echo "=== FAIL — fix the items above before running ==="
    exit 1
fi
echo "=== ready ==="
echo "Next: ./plugin-build.sh (rebuilds SQL plugin if you changed api/)"
echo "      ./cluster-start.sh && ./cluster-wait.sh"
echo "      ./test-run.sh [optional-test-filter]"
echo "      ./test-wait.sh"
