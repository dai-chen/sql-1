# ClickBench SQL — Analytics Engine Testing Steps

End-to-end steps to run the 43 ClickBench SQL queries through the analytics-engine path
on a local OpenSearch cluster with the full Mustang plugin set.

## Versions Used

| Component | Version / Branch | Commit | Repository |
|---|---|---|---|
| OpenSearch core | `main` branch, **3.7.0-SNAPSHOT** | `e089d0697a1` | `github.com:dai-chen/OpenSearch.git` (sandbox-enabled fork) |
| SQL plugin | `feature/mustang-sql-it-local-changes` | `ff21c9ff8` (tip) | `github.com:dai-chen/sql-1.git` |
| opensearch-sql-plugin | `3.7.0.0-SNAPSHOT` | (built from above) | — |
| opensearch-job-scheduler | `3.7.0.0-SNAPSHOT` | (resolved from OpenSearch Snapshots repo) | — |
| analytics-engine | `3.7.0-SNAPSHOT` | (built in-tree from sandbox/) | — |
| JDK (runtime) | Amazon Corretto **21** | — | — |
| JDK (native lib build) | Amazon Corretto **25** | — | — |

**Important**: The OpenSearch core repo must be the **sandbox-enabled fork** that contains
`sandbox/plugins/` with: `analytics-engine`, `analytics-backend-datafusion`,
`analytics-backend-lucene`, `composite-engine`, `parquet-data-format`.

---

## Prerequisites

| Requirement | Purpose |
|---|---|
| JDK 21 (Amazon Corretto) | SQL plugin build + OpenSearch runtime |
| JDK 25 (Amazon Corretto) | `sandbox/libs/dataformat-native` (FFM API) |
| Rust/cargo | Native library build (one-time) |
| OpenSearch sandbox-enabled repo (see above) | analytics-engine + composite-engine + parquet |
| SQL plugin repo (this repo) | ClickBench IT + unified SQL spec |

### Directory layout (example — use any paths you like)

```
$HOME/
├── opensearch-core/     # OpenSearch sandbox-enabled fork (set as OS_REPO)
└── sql/                 # This SQL plugin repo (set as SQL_REPO)
```

### Environment variables

```bash
export OS_REPO=/path/to/opensearch-core        # sandbox-enabled OpenSearch fork
export SQL_REPO=/path/to/sql                   # this SQL plugin repo
export JDK_25=/path/to/corretto-25             # only needed if not auto-discovered
```

### One-time setup: build the native library

```bash
cd "$OS_REPO/sandbox/libs/dataformat-native/rust"
cargo build --release
```

This produces `libopensearch_native.so` (Linux) or `libopensearch_native.dylib` (macOS)
in `target/release/`. The cluster startup references this path automatically.

### Validate environment

```bash
cd "$SQL_REPO"
./mustang-scripts/preflight.sh
```

All checks must pass before proceeding.

---

## Step 1: Build and publish the SQL plugin to Maven local

```bash
cd "$SQL_REPO"
./mustang-scripts/plugin-build.sh
```

This runs `spotlessApply` + `bundlePlugin` + `publishToMavenLocal` for the `opensearch-sql-plugin`
module. Output: `~/.m2/repository/org/opensearch/plugin/opensearch-sql-plugin/3.7.0.0-SNAPSHOT/opensearch-sql-plugin-3.7.0.0-SNAPSHOT.zip`

**Note**: You do NOT need to run `publishToMavenLocal` on the OpenSearch core repo separately.
The `./gradlew run` task in step 2 resolves sandbox plugins (analytics-engine, composite-engine,
etc.) directly from the in-tree `:sandbox:plugins:*` projects via Gradle's project resolution.
Only `opensearch-sql-plugin` and `opensearch-job-scheduler` are resolved from Maven
(local repo + [OpenSearch Snapshots](https://ci.opensearch.org/ci/dbc/snapshots/maven/)).

---

## Step 2: Start the OpenSearch cluster with all required plugins

```bash
cd "$SQL_REPO"
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh
```

Under the hood, `cluster-start.sh` runs (in `$OS_REPO`):

```bash
cd "$OS_REPO"
JAVA_HOME="$JDK25" ./gradlew run -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT', \
    'analytics-engine', 'analytics-backend-lucene', \
    'analytics-backend-datafusion', 'parquet-data-format', \
    'composite-engine', 'opensearch-sql-plugin:3.7.0.0-SNAPSHOT']" \
  -Dtests.jvm.argline="-Djava.library.path=$OS_REPO/sandbox/libs/dataformat-native/rust/target/release \
    -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true"
```

`cluster-wait.sh` polls `http://localhost:9200` with a 120s timeout.

### How plugin resolution works (from `gradle/run.gradle`)

| Plugin name in list | Resolution |
|---|---|
| `analytics-engine` | Found at `:sandbox:plugins:analytics-engine` → built in-tree |
| `analytics-backend-lucene` | Found at `:sandbox:plugins:analytics-backend-lucene` → built in-tree |
| `analytics-backend-datafusion` | Found at `:sandbox:plugins:analytics-backend-datafusion` → built in-tree |
| `parquet-data-format` | Found at `:sandbox:plugins:parquet-data-format` → built in-tree |
| `composite-engine` | Found at `:sandbox:plugins:composite-engine` → built in-tree |
| `opensearch-job-scheduler:3.7.0.0-SNAPSHOT` | Maven coordinates → resolved from maven-local or OpenSearch Snapshots repo |
| `opensearch-sql-plugin:3.7.0.0-SNAPSHOT` | Maven coordinates → resolved from maven-local (published in Step 1) |

### Verify plugins loaded

```bash
curl -s http://localhost:9200/_cat/plugins?v
```

Expected output (7 plugins):
```
name      component                    version
runTask-0 analytics-backend-datafusion 3.7.0-SNAPSHOT
runTask-0 analytics-backend-lucene     3.7.0-SNAPSHOT
runTask-0 analytics-engine             3.7.0-SNAPSHOT
runTask-0 composite-engine             3.7.0-SNAPSHOT
runTask-0 opensearch-job-scheduler     3.7.0.0-SNAPSHOT
runTask-0 opensearch-sql               3.7.0.0-SNAPSHOT
runTask-0 parquet-data-format          3.7.0-SNAPSHOT
```

---

## Step 3: Create the parquet-backed `hits` index

The analytics engine requires a **parquet-backed** index (via composite-engine). A plain
lucene-backed index will fail with "No backend can scan all requested fields."

```bash
cd "$SQL_REPO"

python3 -c "
import json, sys
with open('integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json') as f:
    body = json.load(f)
body['settings'] = {
    'index': {
        'number_of_shards': 1,
        'number_of_replicas': 0,
        'pluggable.dataformat.enabled': True,
        'pluggable.dataformat': 'composite',
        'composite.primary_data_format': 'parquet'
    }
}
json.dump(body, sys.stdout)
" > /tmp/hits_create_body.json

curl -X PUT "http://localhost:9200/hits" \
  -H 'Content-Type: application/json' \
  -d @/tmp/hits_create_body.json
```

Expected: `{"acknowledged":true,"shards_acknowledged":true,"index":"hits"}`

**Critical index settings** (from the Quip doc "Mustang + SQL plugin testing steps"):
- `pluggable.dataformat.enabled: true`
- `pluggable.dataformat: composite`
- `composite.primary_data_format: parquet`
- `number_of_shards: 1` (must NOT have more than 1 shard)

---

## Step 4: Index sample data

The ClickBench test ships a minimal 1-row sample. For meaningful verification, index a
second document that satisfies all filter conditions used across the 43 queries:

```bash
cd "$SQL_REPO"

# Original sample row
curl -X POST "http://localhost:9200/hits/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @integ-test/src/test/resources/clickbench/data/clickbench.json

# Additional row satisfying: AdvEngineID<>0, MobilePhoneModel<>'',
# SearchPhrase<>'', UserID=435090932899640449, URL LIKE '%google%',
# CounterID=62, EventDate in July 2013, Title<>'', IsLink<>0
curl -X POST "http://localhost:9200/hits/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  -d '{"index":{}}
{"WatchID":"1234567890123456789","JavaEnable":1,"Title":"Google Search Results","GoodEvent":1,"EventTime":"2013-07-15 10:30:00","EventDate":"2013-07-15","CounterID":62,"ClientIP":123456789,"RegionID":1,"UserID":"435090932899640449","CounterClass":0,"OS":1,"UserAgent":1,"URL":"https://www.google.com/search?q=opensearch","Referer":"https://www.google.com/","IsRefresh":0,"RefererCategoryID":1,"RefererRegionID":1,"URLCategoryID":1,"URLRegionID":1,"ResolutionWidth":1920,"ResolutionHeight":1080,"ResolutionDepth":24,"FlashMajor":0,"FlashMinor":0,"FlashMinor2":"","NetMajor":0,"NetMinor":0,"UserAgentMajor":0,"UserAgentMinor":"0","CookieEnable":1,"JavascriptEnable":1,"IsMobile":1,"MobilePhone":1,"MobilePhoneModel":"iPhone","Params":"","IPNetworkID":12345,"TraficSourceID":2,"SearchEngineID":2,"SearchPhrase":"opensearch sql","AdvEngineID":1,"IsArtifical":0,"WindowClientWidth":1920,"WindowClientHeight":1080,"ClientTimeZone":0,"ClientEventTime":"2013-07-15 10:30:00","SilverlightVersion1":0,"SilverlightVersion2":0,"SilverlightVersion3":0,"SilverlightVersion4":0,"PageCharset":"UTF-8","CodeVersion":1,"IsLink":1,"IsDownload":0,"IsNotBounce":1,"FUniqID":"1","OriginalURL":"","HID":0,"IsOldCounter":0,"IsEvent":0,"IsParameter":0,"DontCountHits":0,"WithHash":0,"HitColor":"0","LocalEventTime":"2013-07-15 10:30:00","Age":25,"Sex":1,"Income":3,"Interests":100,"Robotness":0,"RemoteIP":987654321,"WindowName":0,"OpenerName":0,"HistoryLength":5,"BrowserLanguage":"en","BrowserCountry":"US","SocialNetwork":"","SocialAction":"","HTTPError":0,"SendTiming":0,"DNSTiming":0,"ConnectTiming":0,"ResponseStartTiming":0,"ResponseEndTiming":0,"FetchTiming":0,"SocialSourceNetworkID":0,"SocialSourcePage":"","ParamPrice":"0","ParamOrderID":"","ParamCurrency":"USD","ParamCurrencyID":1,"OpenstatServiceName":"","OpenstatCampaignID":"","OpenstatAdID":"","OpenstatSourceID":"","UTMSource":"google","UTMMedium":"cpc","UTMCampaign":"test","UTMContent":"","UTMTerm":"opensearch","FromTag":"","HasGCLID":1,"RefererHash":"3594120000172545465","URLHash":"2868770270353813622","CLID":0}
'

# Refresh to make data searchable
curl -X POST "http://localhost:9200/hits/_refresh"
```

---

## Step 5: Enable analytics-engine force-routing

This cluster setting makes `RestUnifiedQueryAction.isAnalyticsIndex()` return `true`
unconditionally, routing ALL SQL queries through the analytics-engine planner regardless
of index name.

```bash
curl -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent": {"plugins.calcite.analytics.force_routing": "true"}}'
```

---

## Step 6: Run ClickBench SQL queries

### Quick smoke test

```bash
curl -X POST "http://localhost:9200/_plugins/_sql?format=jdbc" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) FROM hits"}'
```

Expected: `{"schema":[{"name":"EXPR$0","type":"long"}],"datarows":[[2]],"total":1,"size":1}`

### Run all 43 queries via shell loop

```bash
cd "$SQL_REPO"
for i in $(seq 1 43); do
  SQL=$(sed -n '/\/\*/,/\*\//{ s/^\/\*//; s/\*\/$//; p }' \
    integ-test/src/test/resources/clickbench/queries/q${i}.ppl | \
    tr '\n' ' ' | sed 's/^ *//;s/ *$//;s/;$//')

  echo -n "q${i}: "
  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    "http://localhost:9200/_plugins/_sql?format=jdbc" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": $(echo "$SQL" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}")

  HTTP_CODE=$(echo "$RESPONSE" | tail -1)
  BODY=$(echo "$RESPONSE" | sed '$d')

  if [ "$HTTP_CODE" = "200" ]; then
    echo "PASS (HTTP 200)"
  else
    echo "FAIL (HTTP $HTTP_CODE): $(echo "$BODY" | head -c 120)"
  fi
done
```

### Or use the Gradle IT (generates a markdown report)

```bash
cd "$SQL_REPO"
./gradlew :integ-test:integTestRemote \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -Dtests.analytics.force_routing=true \
  --tests "org.opensearch.sql.sql.clickbench.SQLClickBenchIT" \
  --rerun-tasks
```

Report output: `integ-test/build/reports/clickbench-sql/REPORT.md`

---

## Step 7: Stop the cluster

```bash
cd "$SQL_REPO"
./mustang-scripts/cluster-stop.sh
```

---

## Expected Results (as of 2026-05-04)

| Metric | Value |
|---|---:|
| Queries run | 43 |
| Passed | 28 |
| Failed | 15 |
| Pass rate | **65.1%** |

### Failure categories

| Root Cause | Queries | Count |
|---|---|---:|
| Unrecognized filter operator [SEARCH] (Calcite LIKE→SEARCH rewrite) | q37, q38, q39, q41, q42 | 5 |
| Unable to find binding for MIN on string column | q22, q23 | 2 |
| No backend supports scalar function [EXTRACT] | q19 | 1 |
| No backend supports scalar function [null] (length() not mapped) | q28 | 1 |
| No enum constant ScalarFunction.REGEXP_REPLACE | q29 | 1 |
| Unrecognized aggregate function [ANY_VALUE] (GROUP BY ordinal) | q35 | 1 |
| No backend supports scalar function [MINUS] | q36 | 1 |
| NPE in Calcite validator (CASE WHEN ... END AS alias) | q40 | 1 |
| DATE_TRUNC signature mismatch (CHAR, TIMESTAMP) | q43 | 1 |
| SqlParseException (test-asset bug: literal '...' in q30.ppl) | q30 | 1 |

---

## Troubleshooting

### "No backend can scan all requested fields on index [hits]"

The index was created WITHOUT parquet settings. Delete and recreate:

```bash
curl -X DELETE "http://localhost:9200/hits"
# Then redo Step 3 and Step 4
```

### Cluster won't start (port in use)

```bash
cd "$SQL_REPO"
./mustang-scripts/cluster-stop.sh
# or manually:
kill $(cat /tmp/sisyphus-cluster/cluster-gradle.pid)
```

### SQL plugin changes not picked up

After modifying code in `api/` or `plugin/`, rebuild and restart:

```bash
cd "$SQL_REPO"
./mustang-scripts/plugin-build.sh
./mustang-scripts/cluster-stop.sh
./mustang-scripts/cluster-start.sh
./mustang-scripts/cluster-wait.sh
```

### JDK 25 not found

```bash
# Amazon Linux 2:
sudo yum install -y java-25-amazon-corretto-devel
# Or download tarball and set:
export JDK_25=/path/to/corretto-25
```

### Gradle "immutable workspace" error

The `./gradlew run` task corrupts Gradle's transform cache. `plugin-build.sh` auto-cleans
this, but if you hit it manually:

```bash
find ~/.gradle/caches -type d -name "opensearch-*-SNAPSHOT.zip" -path "*/transforms/*" \
  -exec sh -c 'rm -rf "$(dirname "$(dirname "$1")")"' _ {} \;
```

### opensearch-job-scheduler not found

It's resolved from the [OpenSearch Snapshots repo](https://ci.opensearch.org/ci/dbc/snapshots/maven/).
If your network can't reach it, download manually:

```bash
# Check availability:
curl -s "https://ci.opensearch.org/ci/dbc/snapshots/maven/org/opensearch/plugin/opensearch-job-scheduler/3.7.0.0-SNAPSHOT/maven-metadata.xml"
```

### Current limitation: refresh hangs

Per the Quip doc: "Tests that require wait for refresh will hang waiting for refresh."
The `_refresh` API call in Step 4 should work, but if a test does `refresh=wait_for` on
index operations, it may hang indefinitely on parquet-backed indices.
