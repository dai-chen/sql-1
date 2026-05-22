# Investigation: SQL Test Suite Relevance Function Issues — Issues #1 and #2

**Source**: https://quip-amazon.com/snpzAQi1Xxr2/SQL-Test-Suite-Relevance-Function-Issues
**Repo**: /Users/daichen/Temp/my-sql-repo-cc (opensearch-project/sql)
**Date**: 2026-05-19
**Note**: Investigation only — no code or doc modifications were made.

---

## Issue #1: Mismatch between `text` and `string`

### Symptom
Failing Match*IT tests assert `verifySchema(result, schema("phrase", "text"))` but the Calcite (Analytical Engine) path produces `"type": "string"`.

### Root Cause: Lossy type conversion in the Calcite type factory
The conversion chain `OpenSearch field type → Calcite type → ExprType → JSON type` discards the distinction between `text` and `keyword`:

| Step | Location | What happens |
|---|---|---|
| Forward (OS → Calcite) | `OpenSearchTypeFactory.java:168` | `ExprCoreType.STRING` (keyword) → `SqlTypeName.VARCHAR` |
| Forward (OS → Calcite) | `OpenSearchTypeFactory.java:202` | `OpenSearchTextType` (`legacyTypeName="text"`) → also `SqlTypeName.VARCHAR` |
| Reverse (Calcite → ExprType) | `OpenSearchTypeFactory.java:228` | `VARCHAR` → `ExprCoreType.STRING` (no text/keyword info) |
| Schema in response | `OpenSearchExecutionEngine.java:318` | calls `convertRelDataTypeToExprType(fieldType)` |
| JSON formatter (Calcite path) | `SimpleJsonResponseFormatter.java:53` → `QueryResult.java:78` | `langSpec.typeName(STRING).toLowerCase()` → `"string"` |
| JSON formatter (V2 path, for contrast) | `JdbcResponseFormatter.java:67` | calls `legacyTypeName().toLowerCase()` — would yield `"keyword"` for `STRING` (still not `"text"`) |

### Why V2 legacy works
V2 keeps `OpenSearchTextType` (which holds `MappingType.Text`) all the way to the formatter:
- `OpenSearchDataType.legacyTypeName()` returns `mappingType.toString().toUpperCase()` → `"TEXT"`
- `JdbcResponseFormatter` lowercases it → `"text"` ✅

### Failing tests (confirmed expected types)
- `MatchBoolPrefixIT.additional_parameters_test:39` → expects `schema("phrase", "text")`
- `MatchBoolPrefixIT.query_matches_test:28` → expects `schema("phrase", "text")`
- `MatchIT.match_in_where:33` → expects `schema("firstname", "text")`
- `MatchIT.match_in_having:41` → expects `schema("lastname", "text")`
- (~9 more Match*IT tests with the same pattern)

### Suggested fix directions (informational)
1. **UDT for text** — Create an `EXPR_TEXT` user-defined type analogous to `EXPR_DATE`/`EXPR_IP` that wraps `VARCHAR` but preserves "text" origin in both forward and reverse conversions.
2. **RelDataType metadata** — Annotate the Calcite type with custom metadata recording the original mapping type.
3. **Schema lookup in builder** — In `OpenSearchExecutionEngine.buildResultSet`, look up `OpenSearchDataType` from the original index mapping instead of reverse-converting the `RelDataType`.

Detailed report: `.sisyphus/handoff/issue1-text-string-mismatch.md`

---

## Issue #2: Wildcard syntax — expanded fields not in Lucene → 0 results

### Symptom
`multi_match(['*Date'], '2014-01-22')` against `TEST_INDEX_BEER` produces a Lucene query that correctly translates `*Date` to range queries on `CreationDate`, `LastActivityDate`, `LastEditDate`, `ClosedDate` — but `matchingDocs=0`. The Lucene reader fields list is `[Body, Body.keyword, ContentLicense, ContentLicense.keyword, Tags, Tags.keyword, Title, Title.keyword, __row_id__]` — date fields are absent.

### How the wildcard flows (no expansion in SQL plugin)
1. Calcite plan carries `multi_match(MAP('fields', MAP('*Date', 1.0)), MAP('query', '2014-01-22'))`
2. `PredicateAnalyzer.java:436-473` extracts the fields RexCall MAP
3. `QueryExpression.multiMatch()` (line 1476-1479) → `MultiMatchQuery.build(...)`
4. `MultiFieldQuery.java:70-97` extracts field names from the RexCall MAP
5. `MultiMatchQuery.createBuilder()` (line 22-28) calls `QueryBuilders.multiMatchQuery(query).fields(fields)` — passes `*Date` as-is

The SQL plugin does NOT expand the wildcard. It delegates to OpenSearch's `MultiMatchQueryBuilder`.

### Why 0 results in the analytics engine
When `tests.analytics.parquet_indices=true`, `TestUtils.AnalyticsIndexConfig.applyIndexCreationSettings` (`integ-test/.../legacy/TestUtils.java:76-89`) creates the index with:
```
index.pluggable.dataformat = composite
index.composite.primary_data_format = parquet
index.composite.secondary_data_formats = []   # (or only text fields)
```

- **MapperService** knows all fields (CreationDate etc. via dynamic mapping).
- **Lucene segments** only contain fields stored in the secondary Lucene format → date fields are parquet-only.
- OpenSearch expands `*Date` against the FULL mapping → produces sub-queries on date fields.
- Analytics engine's `LuceneFilterDelegationHandle` (lives in the analytics engine, NOT in this repo) executes against the Lucene reader, which has no date fields → 0 matching docs.

### Failing tests (all use `*Date` + beer index, expect 10 results)
- `MultiMatchIT.verify_wildcard_test` — `MultiMatchIT.java:57-65`
- `SimpleQueryStringIT.verify_wildcard_test` — `SimpleQueryStringIT.java:68-76`
- `QueryStringIT.wildcard_test` — `QueryStringIT.java:62-66`

### Did V2 legacy work?
Yes — V2 sent queries via the standard OpenSearch REST search API where OpenSearch handled BOTH wildcard expansion AND execution against the same Lucene index (no parquet/Lucene split). The bug only surfaces with `-Dtests.analytics.parquet_indices=true`.

### Where the actual bug lives
**Not in this repo.** The mismatch is in the analytics engine's Lucene filter delegation: wildcard expansion uses the full mapping, but execution uses the Lucene-indexed subset. The SQL plugin passes the wildcard correctly per the standard OpenSearch contract.

### Suggested fix directions (informational)
1. **Analytics engine fix (correct)** — In `LuceneFilterDelegationHandle`, intercept `MultiMatchQueryBuilder`/`QueryStringQueryBuilder`/`SimpleQueryStringQueryBuilder` and resolve wildcards against the Lucene reader's field set (not the full mapping). _Lives outside this repo._
2. **SQL plugin pre-resolution** — In `PredicateAnalyzer.visitRelevanceFunc`, when target is a composite/parquet index, expand wildcards against only Lucene-indexed fields before building the `QueryBuilder`. Requires the SQL plugin to know which fields are Lucene-indexed (currently not exposed).
3. **Test workaround** — Skip or adjust `*Date` tests under `tests.analytics.parquet_indices=true`, or use patterns that match only Lucene-indexed fields (e.g., `T*` → `Tags`, `Title`). Doesn't fix the underlying issue for real users with composite indices.

**Recommendation**: Option 1 is the right long-term fix. Option 3 can unblock the test suite short-term.

Detailed report: `.sisyphus/handoff/issue2-wildcard-fields.md`

---

## Cross-cutting observations

| | Issue #1 | Issue #2 |
|---|---|---|
| Where the bug actually lives | This repo (`OpenSearchTypeFactory`) | Analytics engine (external) |
| Triggered by | Any Calcite-path query against text fields | Composite/parquet index + wildcard fields not in Lucene |
| Affects only test suite? | No — affects all users on Calcite path | Only when fields are split across parquet/Lucene |
| Difficulty | Medium (UDT plumbing in this repo) | Coordinated change in analytics engine |
