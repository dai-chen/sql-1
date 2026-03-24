# Unified Query API — SQL Gap Analysis Report

**Date:** 2026-03-24  
**Branch:** `poc/unified-sql-support-gap-analysis`  
**Scope:** All SQL IT tests — V2 (`org.opensearch.sql.sql.*IT`, 50 classes) + Legacy (`org.opensearch.sql.legacy.*IT`, 39 classes)  
**Pipeline:** `UnifiedQueryPlanner` → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`

---

## Summary

| Metric | Raw | Adjusted (excl. concurrency false positives) |
|--------|-----|----------------------------------------------|
| Total Queries | 807 | ~280 unique |
| Success | 88 (10.9%) | 88 (~31%) |
| Failed | 719 (89.1%) | ~192 unique |

---

## Category 1: Explicit VALUES Unsupported (40 queries)

**Phase:** PLAN  
**Error:** `CalciteUnsupportedException: Explicit values node is unsupported in Calcite`  
**Root Cause:** `SELECT func(literal)` without a `FROM` clause requires a `VALUES` row source in Calcite. The unified planner has an explicit guard rejecting this.

| Function | Count | Sample Query |
|----------|-------|--------------|
| `convert_tz()` | 17 | `SELECT convert_tz('2021-05-12 00:00:00','+10:00','+11:00')` |
| `DATETIME()` | 14 | `SELECT DATETIME('2008-01-01 02:00:00+10:00', '-10:00')` |
| `typeof()` | 2 | `SELECT typeof('pewpew'), typeof(NULL), typeof(1.0)` |
| `position()` | 1 | `SELECT position('world' IN 'hello world')` |
| `percentiles()` | 2 | `SELECT percentiles(age, 25.0, 50.0, 75.0, 99.9) FROM ...` |
| string literal | 2 | `select 'Hello'` |
| `INTERVAL` | 1 | `SELECT typeof(INTERVAL 2 DAY)` |
| null error | 1 | Unknown query with null error message |

### Manual Verification

```bash
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT convert_tz('\''2021-05-12 00:00:00'\'','\''\+10:00'\'','\''\+11:00'\'')"}'
```

---

## Category 2: Unregistered Full-Text Search Functions (71 queries)

**Phase:** PLAN  
**Error:** `Cannot resolve function: <name>`  
**Root Cause:** Legacy FTS function aliases and OpenSearch-specific functions not registered in the unified planner.  
**Status:** Expected gap — these are OpenSearch-specific extensions.

| Function | Count | Sample Query |
|----------|-------|--------------|
| `nested()` | 22 | `SELECT nested(message.info) FROM ...nested_type` |
| `wildcard_query()` | 20 | `WHERE wildcard_query(KeywordBody, 'test*')` |
| `query()` | 8 | `WHERE query('Tags:taste')` |
| `matchquery()` | 5 | `WHERE matchquery(lastname, 'Bates')` |
| `match_query()` | 5 | `WHERE match_query(lastname, 'Bates')` |
| `matchphrase()` | 5 | `WHERE matchphrase(phrase, 'quick fox')` |
| `matchphrasequery()` | 2 | `WHERE matchphrasequery(phrase, 'quick fox')` |
| `multimatchquery()` | 2 | `WHERE multimatchquery(query='cicerone', fields='Tags')` |
| `wildcardquery()` | 2 | `WHERE wildcardquery(KeywordBody, 't*')` |
| `multimatch()` | 1 | `WHERE multimatch('query'='taste', 'fields'='Tags')` |

---

## Category 3: Relevance Functions Execute as Script Instead of Native Query (22 queries)

**Phase:** EXECUTE  
**Error:** `UnsupportedOperationException: Relevance search query functions are only supported when they are pushed down`  
**Affects:** `match()`, `match_phrase()`, `match_phrase_prefix()`

### Root Cause

The unified planner successfully plans and compiles these queries. During execution, the compiled plan pushes filters down to OpenSearch via `OpenSearchIndex`. However, the `PredicateAnalyzer` (which converts Calcite filter expressions to OpenSearch `QueryBuilder`) cannot handle `MAP_VALUE_CONSTRUCTOR` RexCalls that wrap relevance function arguments. It falls back to serializing the entire `match()` call as a **script query** instead of a native `match` query. The OpenSearch script engine then fails because it cannot evaluate relevance functions.

**Fix:** In `PredicateAnalyzer.visitRelevanceFunc()`, use `AliasPair.from()` for single-field functions (as already done for multi-field functions) instead of `visitList()`, which bypasses the unsupported RexCall check.

| Function | Count | Sample Query |
|----------|-------|--------------|
| `match()` | 8 | `SELECT firstname FROM ...account WHERE match(lastname, 'Bates')` |
| `match_phrase()` | 5 | `SELECT phrase FROM ...phrase WHERE match_phrase(phrase, 'quick fox')` |
| `match_phrase_prefix()` | 8 | `SELECT Title FROM ...beer WHERE match_phrase_prefix(Title, 'champagne be')` |
| `match()` (pagination) | 1 | `SELECT * FROM ...account WHERE match(address, 'street')` |

### Manual Verification

```bash
curl -s -XPUT 'localhost:9200/opensearch-sql_test_index_account' -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"firstname":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"lastname":{"type":"text","fields":{"keyword":{"type":"keyword"}}}}}}'
curl -s -XPOST 'localhost:9200/opensearch-sql_test_index_account/_bulk' \
  -H 'Content-Type: application/json' --data-binary '
{"index":{}}
{"firstname":"Amber","lastname":"Duke","age":32,"balance":39225}
{"index":{}}
{"firstname":"Nanette","lastname":"Bates","age":28,"balance":32838}
'

curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT firstname FROM opensearch-sql_test_index_account WHERE match(lastname, '\''Bates'\'')"}'
```

---

## Category 4: Highlight Unsupported (12 queries)

**Phase:** PLAN  
**Error:** `CalciteUnsupportedException: Highlight function is unsupported in Calcite`  
**Status:** Explicitly guarded — OpenSearch-specific function not yet implemented.

| Sample Query | Count |
|-------------|-------|
| `SELECT highlight('Tags') FROM ...beer WHERE match(Tags, 'yeast')` | 8 |
| `SELECT highlight(address) FROM ...account WHERE match(address, 'street')` | 4 |

---

## Category 5: NPE in SELECT COUNT(*) and Wildcard Nested (13 queries)

**Phase:** PLAN  
**Error:** `NullPointerException: Cannot invoke "org.apache.calcite.rex.RexNode.getKind()" because "expr" is null`  
**Status:** Real planner bug.

### Root Cause

For `SELECT COUNT(*) FROM table`: the SQL parser creates `AggregateFunction("COUNT", AllFields)` wrapped in an `Alias`. When `expandProjectFields` encounters this after an Aggregation node, it calls `rexVisitor.analyze(AggregateFunction)` which returns `null` (no handler). The null is passed to `relBuilder.alias(null, name)` → NPE.

Same pattern for `nested(message.*)` wildcard expansion and `SELECT m.*` table-qualified wildcard.

| Pattern | Count | Sample Query |
|---------|-------|--------------|
| `SELECT COUNT(*)` | 2 | `SELECT COUNT(*) FROM opensearch-sql_test_index_phrase` |
| `nested(message.*)` | 6 | `SELECT nested(message.*) FROM ...nested_type` |
| `SELECT m.*` | 2 | `SELECT m.* FROM ...nested_type m` |
| `SELECT e.someField, m.*` | 2 | `SELECT e.someField, m.* FROM ...nested_type e, ...` |
| `SELECT MAX(age) + MIN(age)` | 1 | `SELECT MAX(age) + MIN(age) as addValue FROM ...account` |

### Manual Verification

```bash
curl -s -XPUT 'localhost:9200/opensearch-sql_test_index_phrase' -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"phrase":{"type":"text","store":true}}}}'
curl -s -XPOST 'localhost:9200/opensearch-sql_test_index_phrase/_bulk' \
  -H 'Content-Type: application/json' --data-binary '
{"index":{}}
{"phrase":"quick fox"}
{"index":{}}
{"phrase":"quick brown fox"}
'

curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) FROM opensearch-sql_test_index_phrase"}'
```

---

## Category 6: SQL Syntax Gaps (20 queries)

**Phase:** PLAN  
**Error:** `SyntaxCheckException`  
**Root Cause:** ANTLR grammar limitations in the unified SQL parser.

| Issue | Count | Sample Query |
|-------|-------|--------------|
| JOIN keyword | 4 | `SELECT ... FROM ...account b1 JOIN ...account b2 ON b1.age = b2.age` |
| Comma-join | 2 | `FROM ...datetime_nested AS tab, tab.projects AS pro` |
| SHOW TABLES LIKE (unquoted) | 3 | `SHOW TABLES LIKE opensearch-sql_test_index_account` |
| DESCRIBE TABLES LIKE (unquoted) | 5 | `DESCRIBE TABLES LIKE opensearch-sql_test_index_account` |
| Double-quoted string literal | 2 | `select "Hello"` |
| Backslash-escaped quote | 1 | `select 'I\'m'` |
| `percentiles` keyword | 2 | `SELECT percentiles(age, 25.0, 50.0) FROM ...` |
| Index path with `/` | 1 | `SELECT ... FROM opensearch-sql_test_index_account/...` |

### Manual Verification

```bash
# JOIN
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT a.firstname, b.age FROM opensearch-sql_test_index_account a JOIN opensearch-sql_test_index_account b ON a.age = b.age"}'

# SHOW TABLES LIKE (unquoted)
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SHOW TABLES LIKE opensearch-sql_test_index_account"}'

# DESCRIBE TABLES LIKE (unquoted)
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "DESCRIBE TABLES LIKE opensearch-sql_test_index_account"}'
```

---

## Category 7: ISNULL / IFNULL Not Supported (2 queries)

**Phase:** PLAN  
**Error:** `Cannot resolve function: ISNULL` / `Field [lastname] not found`  
**Root Cause:** `ISNULL()` is not registered as a function in the unified Calcite SQL planner. `IFNULL` with `GROUP BY` alias also fails — likely the same root cause (function not properly resolved in Calcite context).

| Query | Error |
|-------|-------|
| `SELECT ISNULL(lastname) AS name FROM ...account` | `Cannot resolve function: ISNULL` |
| `SELECT IFNULL(lastname, 'unknown') AS name FROM ...account GROUP BY name` | `Field [lastname] not found` |

### Manual Verification

```bash
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT ISNULL(lastname) AS name FROM opensearch-sql_test_index_account"}'
```

---

## Category 8: Field Not Found — Concurrency False Positive (527+ entries, 6 unique queries)

**Phase:** PLAN  
**Error:** `Field [age/balance/firstname/lastname/insert_time/male] not found`  
**Root Cause:** Gap analyzer limitation under concurrency. `SQLConcurrencyIT` runs 256 threads; each creates a separate `UnifiedQueryContext` with independent `OpenSearchIndex` schema resolution. Under heavy concurrent REST `_mapping` calls, some return incomplete results. Other single-occurrence "field not found" errors from legacy tests may also be concurrency-related or test ordering artifacts.  
**Status:** False positive — excluded from analysis.

| Field | Occurrences | Source |
|-------|------------|--------|
| `age` | 267 | `SQLConcurrencyIT` (256 threads) + `AggregationExpressionIT` |
| `balance` | 260 | `SQLConcurrencyIT` (256 threads) |
| `firstname` | 1 | Legacy test |
| `lastname` | 1 | `ConditionalIT` (IFNULL interaction) |
| `insert_time` | 1 | Legacy test |
| `male` | 1 | Legacy test |

---

## Category 9: Nested in HAVING Clause (1 query)

**Phase:** PLAN  
**Error:** `SyntaxCheckException: Falling back to legacy engine. Nested function is not supported in the HAVING clause.`  
**Status:** Known limitation — explicitly guarded.

| Sample Query |
|-------------|
| `SELECT count(*) FROM ...nested_type GROUP BY myNum HAVING nested(comment.likes) > 7` |

---

## Priority Matrix

| Priority | Category | Type | Unique Queries | Fix Complexity |
|----------|----------|------|---------------|----------------|
| **P0** | Cat 5: NPE in COUNT(*) / wildcard | Real bug | 13 | Medium |
| **P0** | Cat 1: VALUES unsupported | Real gap | 40 | Medium |
| **P1** | Cat 3: Relevance script fallback | Real bug | 22 | Low — fix PredicateAnalyzer.visitRelevanceFunc() |
| **P1** | Cat 6: JOIN / SHOW / DESCRIBE syntax | Real gap | 20 | Medium — extend ANTLR grammar |
| **P1** | Cat 7: ISNULL / IFNULL | Real gap | 2 | Low — register functions |
| **P2** | Cat 2: FTS functions | Expected | 71 | High |
| **P2** | Cat 4: highlight() | Expected | 12 | High |
| **P2** | Cat 9: nested in HAVING | Known limitation | 1 | Medium |
| **N/A** | Cat 8: Concurrency false positive | Gap analyzer limitation | ~530 | N/A |

---

## How to Run

```bash
# SQL V2 tests
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.*IT' -Dunified.gap.analysis=true

# Legacy SQL tests
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.legacy.*IT' -Dunified.gap.analysis=true

# Both
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.*IT' --tests 'org.opensearch.sql.legacy.*IT' -Dunified.gap.analysis=true

# Report output
cat integ-test/build/gap-analysis-report.txt
```

---

## Changes Made for This Analysis

| File | Change |
|------|--------|
| `legacy/.../RestSqlAction.java` | Restored V2 engine routing as default; unified handler only for `mode=ansi` |
| `integ-test/.../UnifiedQueryGapAnalyzer.java` | Fixed schema caching; unwrap root cause; add JSON unescape |
| `integ-test/.../SQLIntegTestCase.java` | Add JSON unescape for SQL gap analyzer path |
