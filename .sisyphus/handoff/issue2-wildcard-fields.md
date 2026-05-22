# Issue: Wildcard Field Expansion Returns 0 Results

## 1. How Wildcard Fields Are Expanded

The wildcard field pattern (e.g., `*Date`) flows through the SQL plugin **without expansion**:

1. **SQL parsing** → Calcite plan with `multi_match` as a UDF call containing `fields=MAP('*Date', 1.0)`
2. **PredicateAnalyzer** (`opensearch/src/main/java/org/opensearch/sql/opensearch/request/PredicateAnalyzer.java:436-473`) handles multi-field relevance functions. It extracts the `fieldsRexCall` MAP containing the raw wildcard string.
3. **QueryExpression.multiMatch()** (line 1476-1479) delegates to `MultiMatchQuery.build(fieldsRexCall, query, optionalArguments)`
4. **MultiMatchQuery.build()** (`opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/filter/lucene/relevance/MultiFieldQuery.java:70-97`) extracts field names from the RexCall MAP and passes them directly to `createBuilder(fields, query)`
5. **MultiMatchQuery.createBuilder()** (`opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/filter/lucene/relevance/MultiMatchQuery.java:22-28`) calls `QueryBuilders.multiMatchQuery(query).fields(fields)` — passing `*Date` as-is to OpenSearch's `MultiMatchQueryBuilder`

**Key insight**: The SQL plugin does NOT resolve the wildcard. It relies on OpenSearch/Lucene to expand `*Date` against the index mapping at query execution time.

## 2. Where Lucene Query Compilation Happens

### Standard (non-analytics) path:
- `CalciteLogicalIndexScan.pushDownFilter()` (line 160-192) calls `PredicateAnalyzer.analyzeExpression()` to produce a `QueryBuilder`
- The `QueryBuilder` (containing `MultiMatchQueryBuilder` with wildcard fields) is pushed to `OpenSearchRequestBuilder.pushDownFilterForCalcite()`
- This becomes part of the search request sent to OpenSearch, which expands wildcards against its full mapping and executes against Lucene segments that contain ALL mapped fields

### Analytics (parquet-backed) path:
- The same Calcite plan is produced, but execution routes through `AnalyticsExecutionEngine` (`core/src/main/java/org/opensearch/sql/executor/analytics/AnalyticsExecutionEngine.java:41`)
- `AnalyticsExecutionEngine.execute()` delegates to `QueryPlanExecutor<RelNode, Iterable<Object[]>>` — the analytics engine (Project Mustang)
- **`LuceneFilterDelegationHandle`** and **`AbstractRelevanceSerializer`** are part of the analytics engine (NOT in this SQL plugin repo). They serialize the relevance query and compile it against the Lucene reader.
- The analytics engine's `LuceneFilterDelegationHandle.compileQueries()` uses the Lucene reader to execute the query, but the Lucene reader only sees fields stored in the secondary Lucene format.

## 3. Why Lucene Reader Fields Differ from Mapper Fields

When `tests.analytics.parquet_indices=true` is set:
- `TestUtils.AnalyticsIndexConfig.applyIndexCreationSettings()` (`integ-test/src/test/java/org/opensearch/sql/legacy/TestUtils.java:76-89`) adds:
  ```
  index.pluggable.dataformat.enabled = true
  index.pluggable.dataformat = composite
  index.composite.primary_data_format = parquet
  index.composite.secondary_data_formats = [] (empty)
  ```
- With this config, the **MapperService** knows about ALL fields (CreationDate, LastActivityDate, etc.) from dynamic mapping
- But the **Lucene segments** only contain fields stored in the secondary Lucene format. Since `secondary_data_formats` is empty (or only text fields are indexed in Lucene), date fields are stored exclusively in parquet
- The Lucene reader therefore only sees: Body, Body.keyword, ContentLicense, ContentLicense.keyword, Tags, Tags.keyword, Title, Title.keyword, __row_id__

**This is an OpenSearch core behavior** — the SQL plugin has no code referencing `composite.primary_data_format` or controlling which fields go to Lucene vs parquet. The field-filtering logic is entirely in the analytics engine / OpenSearch core composite index implementation.

## 4. The 3 Failing Tests

All three tests use the `TEST_INDEX_BEER` (beer.stackexchange) index with dynamic mapping. The beer data contains date fields: `CreationDate`, `LastActivityDate`, `LastEditDate`, `ClosedDate`.

### MultiMatchIT.verify_wildcard_test
**File**: `integ-test/src/test/java/org/opensearch/sql/sql/MultiMatchIT.java:57-65`
```java
String query = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE multi_match(['*Date'], '2014-01-22');";
JSONObject result = executeJdbcRequest(query);
assertEquals(10, result.getInt("total"));
```
**Wildcard**: `*Date` → expands to CreationDate, LastActivityDate, LastEditDate, ClosedDate
**Expected**: 10 results

### SimpleQueryStringIT.verify_wildcard_test
**File**: `integ-test/src/test/java/org/opensearch/sql/sql/SimpleQueryStringIT.java:68-76`
```java
String query = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE simple_query_string(['*Date'], '2014-01-22');";
var result = new JSONObject(executeQuery(query, "jdbc"));
assertEquals(10, result.getInt("total"));
```
**Wildcard**: `*Date` → same expansion
**Expected**: 10 results

### QueryStringIT.wildcard_test
**File**: `integ-test/src/test/java/org/opensearch/sql/sql/QueryStringIT.java:62-66`
```java
String query3 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query_string(['*Date'], '2014-01-22');";
JSONObject result3 = executeJdbcRequest(query3);
assertEquals(10, result3.getInt("total"));
```
**Wildcard**: `*Date` → same expansion
**Expected**: 10 results

**Confirmation**: All three use `*Date` which expands to date fields (CreationDate, LastActivityDate, LastEditDate, ClosedDate). In a parquet-backed composite index, these date fields are stored in parquet only, NOT in Lucene segments. The Lucene reader cannot find them.

## 5. Root Cause Analysis

### (a) Where the bug lies

This is **NOT a bug in the SQL plugin's query construction**. The SQL plugin correctly passes the wildcard `*Date` to `MultiMatchQueryBuilder`, which is the standard OpenSearch API behavior. In a normal Lucene-backed index, OpenSearch expands the wildcard against the mapping and all those fields exist in Lucene segments.

The bug is in the **analytics engine's Lucene filter delegation layer** (`LuceneFilterDelegationHandle`). When the analytics engine receives a `MultiMatchQueryBuilder` with wildcard fields, OpenSearch's query rewriting expands `*Date` against the MapperService (which knows all fields), producing sub-queries for CreationDate, LastActivityDate, etc. But when these sub-queries are executed against the Lucene reader, those fields don't exist in the Lucene segments (they're in parquet). Result: 0 matching docs.

The fundamental issue: **wildcard expansion uses the full mapping, but query execution uses only the Lucene-indexed subset of fields**.

### (b) Did this work in V2 legacy?

In the **V2 legacy path** (non-Calcite), the query was sent as a standard OpenSearch search request via the REST API. OpenSearch handled both wildcard expansion AND query execution against the same Lucene index — where ALL fields were in Lucene. There was no parquet/Lucene split.

The tests fail specifically when run with `-Dtests.analytics.parquet_indices=true`, which routes queries through the analytics engine. Without this flag, the standard path works correctly because all fields are in Lucene.

There is no "old field-resolution code" in the SQL plugin that pre-resolved wildcards — the plugin has always delegated wildcard expansion to OpenSearch's `MultiMatchQueryBuilder`.

## 6. Suggested Fix Directions

### Option A: Filter wildcard expansion to Lucene-readable fields (analytics engine fix)
In `LuceneFilterDelegationHandle`, before compiling the query, intercept `MultiMatchQueryBuilder` (and `QueryStringQueryBuilder`, `SimpleQueryStringQueryBuilder`) and resolve wildcard patterns against only the fields present in the Lucene reader (not the full mapping). This keeps the query semantically correct for the Lucene execution path.

**Pro**: Correct behavior — only queries fields that can actually be searched in Lucene.
**Con**: Requires changes in the analytics engine (not this repo). May miss valid results if some date values happen to be in Lucene.

### Option B: Pre-resolve wildcards in the SQL plugin against Lucene-indexed fields
In `PredicateAnalyzer.visitRelevanceFunc()`, when the execution target is a composite/parquet index, expand wildcard field patterns against only the Lucene-indexed fields before building the `QueryBuilder`. The SQL plugin already has `WildcardUtils.expandWildcardPattern()` and access to field type information.

**Pro**: Fix is in this repo. Prevents sending unresolvable field queries to the analytics engine.
**Con**: The SQL plugin would need to know which fields are Lucene-indexed vs parquet-only, which is currently not exposed in its field metadata.

### Option C: Adjust test data/expectations for parquet mode
Skip or adjust the `*Date` wildcard tests when running with `tests.analytics.parquet_indices=true`, since date fields are not in Lucene segments in that mode. Alternatively, use a wildcard pattern that only matches Lucene-indexed fields (e.g., `T*` which matches Tags and Title).

**Pro**: Simplest change, no production code modification.
**Con**: Doesn't fix the underlying issue — real users with composite indices would hit the same problem. This is a workaround, not a fix.

### Recommended approach
Option A is the correct long-term fix (analytics engine should handle this gracefully). Option C can be used as a short-term workaround to unblock tests while Option A is implemented.
