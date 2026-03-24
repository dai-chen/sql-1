# Unified Query API — SQL Gap Analysis Report

**Date:** 2026-03-24  
**Branch:** `poc/unified-sql-support-gap-analysis`  
**Scope:** All SQL IT tests (`org.opensearch.sql.sql.*IT`) — 50 test classes  
**Pipeline:** `UnifiedQueryPlanner` → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`

---

## Summary

| Metric | Raw | Adjusted (excl. SQLConcurrencyIT) |
|--------|-----|-----------------------------------|
| Total Queries | 736 | 222 |
| Success | 63 (8.6%) | 63 (28.4%) |
| Failed | 673 (91.4%) | 159 (71.6%) |
| Failure Phase | 94% PLAN, 3% EXECUTE | — |

The raw numbers are inflated by `SQLConcurrencyIT` which runs 2 unique queries 256 times each under concurrency — a gap analyzer limitation, not a planner bug.

---

## Category 1: Explicit VALUES Unsupported (37 queries)

**Phase:** PLAN  
**Error:** `CalciteUnsupportedException: Explicit values node is unsupported in Calcite`  
**Root Cause:** Any `SELECT func(literal)` without a `FROM` clause requires a `VALUES` row source in Calcite. The planner has an explicit guard rejecting this pattern.  
**Impact:** All literal-only queries — datetime functions, typeof, position, string literals.

| Function | Count | Sample Query |
|----------|-------|--------------|
| `convert_tz()` | 17 | `SELECT convert_tz('2021-05-12 00:00:00','+10:00','+11:00')` |
| `DATETIME()` | 14 | `SELECT DATETIME('2008-01-01 02:00:00+10:00', '-10:00')` |
| `typeof()` | 2 | `SELECT typeof('pewpew'), typeof(NULL), typeof(1.0)` |
| `position()` | 1 | `SELECT position('world' IN 'hello world')` |
| string literal | 2 | `select 'Hello'` |
| `INTERVAL` | 1 | `SELECT typeof(INTERVAL 2 DAY)` |

### Manual Verification

No index needed — these are literal-only queries.

```bash
# Start OpenSearch on localhost:9200, then:
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT convert_tz('\''2021-05-12 00:00:00'\'','\''\+10:00'\'','\''\+11:00'\'')"}'

curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "select '\''Hello'\''" }'
```

---

## Category 2: Unregistered Full-Text Search Functions (71 queries) — Expected

**Phase:** PLAN  
**Error:** `Cannot resolve function: <name>`  
**Root Cause:** Legacy FTS function aliases not registered in the unified planner. These are OpenSearch-specific functions that haven't been ported yet.  
**Status:** Expected gap — tracked separately.

| Function | Count | Sample Query |
|----------|-------|--------------|
| `nested()` | 21 | `SELECT nested(message.info) FROM ...nested_type` |
| `wildcard_query()` | 20 | `WHERE wildcard_query(KeywordBody, 'test*')` |
| `query()` | 8 | `WHERE query('Tags:taste')` |
| `matchquery()` | 5 | `WHERE matchquery(lastname, 'Bates')` |
| `match_query()` | 5 | `WHERE match_query(lastname, 'Bates')` |
| `matchphrase()` | 5 | `WHERE matchphrase(phrase, 'quick fox')` |
| `matchphrasequery()` | 2 | `WHERE matchphrasequery(phrase, 'quick fox')` |
| `multimatchquery()` | 2 | `WHERE multimatchquery(query='cicerone', fields='Tags')` |
| `multimatch()` | 1 | `WHERE multimatch('query'='taste', 'fields'='Tags')` |
| `ISNULL()` | 1 | `SELECT ISNULL(lastname) AS name FROM ...` |
| `wildcardquery()` | 2 | `WHERE wildcardquery(KeywordBody, 't*')` |

### Manual Verification

```bash
# Create beer index (dynamic mapping)
curl -s -XPUT 'localhost:9200/opensearch-sql_test_index_beer'
curl -s -XPOST 'localhost:9200/opensearch-sql_test_index_beer/_bulk' \
  -H 'Content-Type: application/json' --data-binary '
{"index":{}}
{"Id":10,"PostTypeId":1,"Score":6,"Title":"How do you mull beer?","Tags":"mulling taste"}
{"index":{}}
{"Id":20,"PostTypeId":1,"Score":3,"Title":"IPA vs Pale Ale","Tags":"taste hops ipa"}
'

# Test unregistered function
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT Id FROM opensearch-sql_test_index_beer WHERE query('\''Tags:taste'\'')"}'
```

---

## Category 3: Relevance Function Pushdown Bug (21 queries) — Real Bug

**Phase:** EXECUTE  
**Error:** `UnsupportedOperationException: Relevance search query functions are only supported when they are pushed down`  
**Affects:** `match()`, `match_phrase()`, `match_phrase_prefix()`  
**Status:** Real bug on `main` — not branch-specific.

### Root Cause Analysis

The functions plan and compile successfully, but fail at execution. The full chain:

1. `CalciteRexNodeVisitor.visitUnresolvedArgument()` wraps relevance function arguments in `MAP_VALUE_CONSTRUCTOR` RexCalls
2. `RelevanceFunctionPushdownRule` fires → pushes filter to `CalciteLogicalIndexScan.pushDownFilter()`
3. `pushDownFilter()` calls `PredicateAnalyzer.analyzeExpression()` to convert RexNode → OpenSearch `QueryBuilder`
4. `PredicateAnalyzer.Visitor.visitRelevanceFunc()` calls `visitList()` on operands → hits `visitCall()` → `supportedRexCall()` **returns false for `MAP_VALUE_CONSTRUCTOR`** (`SqlSyntax.SPECIAL` + `SqlKind.MAP_VALUE_CONSTRUCTOR` not whitelisted)
5. `PredicateAnalyzerException` thrown → **falls back to `ScriptQueryExpression`** (serializes match() as script query instead of native match query)
6. At execution, `CalciteScriptEngine` deserializes the script → calls `RelevanceQueryImplementor.implement()` → throws "only supported when pushed down"

**The irony:** The function IS pushed down, but as a script query instead of a native match query. The script engine can't evaluate relevance functions.

### Fix

In `PredicateAnalyzer.Visitor.visitRelevanceFunc()`: use `AliasPair.from()` for single-field functions (like it already does for multi-field functions) instead of `visitList()`. This extracts values from `MAP_VALUE_CONSTRUCTOR` directly, bypassing the `supportedRexCall()` check.

**Key files:**
- `opensearch/src/main/java/org/opensearch/sql/opensearch/request/PredicateAnalyzer.java:405-430` (visitRelevanceFunc)
- `opensearch/src/main/java/org/opensearch/sql/opensearch/request/PredicateAnalyzer.java:283-345` (supportedRexCall)
- `core/src/main/java/org/opensearch/sql/expression/function/udf/RelevanceQueryFunction.java:93-99` (error source)

| Function | Count | Sample Query |
|----------|-------|--------------|
| `match()` | 8 | `SELECT firstname FROM ...account WHERE match(lastname, 'Bates')` |
| `match_phrase()` | 5 | `SELECT phrase FROM ...phrase WHERE match_phrase(phrase, 'quick fox')` |
| `match_phrase_prefix()` | 8 | `SELECT Title FROM ...beer WHERE match_phrase_prefix(Title, 'champagne be')` |

### Manual Verification

```bash
# Create account index
curl -s -XPUT 'localhost:9200/opensearch-sql_test_index_account' -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"firstname":{"type":"text","fielddata":true,"fields":{"keyword":{"type":"keyword"}}},"lastname":{"type":"text","fielddata":true,"fields":{"keyword":{"type":"keyword"}}}}}}'
curl -s -XPOST 'localhost:9200/opensearch-sql_test_index_account/_bulk' \
  -H 'Content-Type: application/json' --data-binary '
{"index":{}}
{"firstname":"Amber","lastname":"Duke","age":32,"balance":39225}
{"index":{}}
{"firstname":"Nanette","lastname":"Bates","age":28,"balance":32838}
'

# Test match() — should work via V2 engine, fails via unified
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT firstname FROM opensearch-sql_test_index_account WHERE match(lastname, '\''Bates'\'')"}'

# Create phrase index
curl -s -XPUT 'localhost:9200/opensearch-sql_test_index_phrase' -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"phrase":{"type":"text","store":true}}}}'
curl -s -XPOST 'localhost:9200/opensearch-sql_test_index_phrase/_bulk' \
  -H 'Content-Type: application/json' --data-binary '
{"index":{}}
{"phrase":"quick fox"}
{"index":{}}
{"phrase":"quick brown fox"}
'

# Test match_phrase()
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT phrase FROM opensearch-sql_test_index_phrase WHERE match_phrase(phrase, '\''quick fox'\'')"}'
```

---

## Category 4: Highlight Unsupported (12 queries) — Expected

**Phase:** PLAN  
**Error:** `CalciteUnsupportedException: Highlight function is unsupported in Calcite`  
**Root Cause:** Explicitly guarded — `highlight()` is an OpenSearch-specific function not yet implemented in Calcite.  
**Status:** Known limitation.

| Sample Query | Count |
|-------------|-------|
| `SELECT highlight('Tags') FROM ...beer WHERE match(Tags, 'yeast')` | 8 |
| `SELECT highlight(address) FROM ...account WHERE match(address, 'street')` | 4 |

### Manual Verification

```bash
# Uses beer index from Category 2 setup above
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT highlight('\''Tags'\'') FROM opensearch-sql_test_index_beer WHERE match(Tags, '\''taste'\'')"}'
```

---

## Category 5: NPE in SELECT COUNT(*) and nested(*.*) (8 queries) — Real Bug

**Phase:** PLAN  
**Error:** `NullPointerException: Cannot invoke "org.apache.calcite.rex.RexNode.getKind()" because "expr" is null`  
**Status:** Real planner bug.

### Root Cause Analysis

For `SELECT COUNT(*) FROM table`:

1. SQL parser creates AST: `Project([Alias("COUNT(*)", AggregateFunction("COUNT", AllFields))])` → `Aggregation` → `Relation`
2. `visitAggregation` correctly builds the aggregate RelNode
3. `visitProject` calls `expandProjectFields` which encounters `Alias(AggregateFunction)` in the project list
4. Calls `rexVisitor.analyze(AggregateFunction)` — but `CalciteRexNodeVisitor` has **no `visitAggregateFunction` handler** → returns `null`
5. `null` passed to `relBuilder.alias(null, name)` → Calcite calls `expr.getKind()` on null → **NPE**

Same pattern for `nested(message.*)` — the wildcard expansion produces an expression the RexNodeVisitor can't handle.

### Fix

In `CalciteRelNodeVisitor.expandProjectFields()`: when the project expression is an `AggregateFunction` (or `Alias` wrapping one), resolve it as a field reference to the aggregation output column instead of re-evaluating as a RexNode.

**Key files:**
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java:396-490` (expandProjectFields)
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java` (missing visitAggregateFunction)

| Pattern | Count | Sample Query |
|---------|-------|--------------|
| `SELECT COUNT(*)` | 2 | `SELECT COUNT(*) FROM opensearch-sql_test_index_phrase` |
| `nested(message.*)` | 6 | `SELECT nested(message.*) FROM ...nested_type` |

### Manual Verification

```bash
# Uses phrase index from Category 3 setup above
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) FROM opensearch-sql_test_index_phrase"}'
```

---

## Category 6: SQL Syntax Gaps (5 queries)

**Phase:** PLAN  
**Error:** `SyntaxCheckException`  
**Root Cause:** ANTLR grammar limitations in the unified SQL parser.

| Issue | Sample Query | Error | Count |
|-------|-------------|-------|-------|
| Double-quoted string | `select "Hello"` | `extraneous input '"'` | 2 |
| Backslash-escaped quote | `select 'I\'m'` | `extraneous input '''` | 1 |
| Comma-join (nested) | `SELECT tab.login_time FROM ...datetime_nested AS tab, tab.projects AS pro` | `Expecting tokens: EOF, ';'` | 1 |
| JOIN keyword | `SELECT one.login_time, two.login_time FROM ...datetime AS one JOIN ...datetime AS two ON one._id = two._id` | `Expecting tokens: EOF, ';'` | 1 |

### Manual Verification

```bash
# Double-quoted string
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "select \\"Hello\\""}'

# JOIN
curl -s -XPUT 'localhost:9200/opensearch-sql_test_index_datetime' -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"login_time":{"type":"date"}}}}'
curl -s -XPOST 'localhost:9200/opensearch-sql_test_index_datetime/_bulk' \
  -H 'Content-Type: application/json' --data-binary '
{"index":{"_id":"1"}}
{"login_time":"2015-01-01"}
{"index":{"_id":"2"}}
{"login_time":"2015-01-01T12:10:30Z"}
'

curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT one.login_time, two.login_time FROM opensearch-sql_test_index_datetime AS one JOIN opensearch-sql_test_index_datetime AS two ON one._id = two._id"}'
```

---

## Category 7: IFNULL + GROUP BY Alias (1 query) — Needs Investigation

**Phase:** PLAN  
**Error:** `Field [lastname] not found`  
**Root Cause:** Likely a GROUP BY alias resolution issue — the planner tries to resolve `lastname` in the context of the GROUP BY alias `name` rather than the original table schema. Or it could be an interaction between `IFNULL` and field resolution.

| Sample Query | Error |
|-------------|-------|
| `SELECT IFNULL(lastname, 'unknown') AS name FROM opensearch-sql_test_index_account GROUP BY name` | `Field [lastname] not found` |

### Manual Verification

```bash
# Uses account index from Category 3 setup above
curl -s -XPOST 'localhost:9200/_plugins/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT IFNULL(lastname, '\''unknown'\'') AS name FROM opensearch-sql_test_index_account GROUP BY name"}'
```

---

## Category 8: False Positive — SQLConcurrencyIT (514 entries, 2 unique queries)

**Phase:** PLAN  
**Error:** `Field [age] not found`, `Field [balance] not found`  
**Root Cause:** Gap analyzer limitation. 256 concurrent threads each create separate `tryUnifiedExecution` calls with independent `UnifiedQueryContext` instances. The `OpenSearchIndex` schema resolution under heavy concurrency causes incomplete field mapping. Not a planner bug.  
**Status:** Excluded from analysis.

| Query | Occurrences |
|-------|------------|
| `SELECT COUNT(age) AS cnt FROM opensearch-sql_test_index_account` | 257 |
| `SELECT SUM(balance) AS total_sales FROM opensearch-sql_test_index_account` | 257 |

---

## Priority Matrix

| Priority | Category | Type | Unique Queries | Fix Complexity |
|----------|----------|------|---------------|----------------|
| **P0** | Cat 5: NPE in COUNT(*) | Real bug | 2 | Medium — add visitAggregateFunction to CalciteRexNodeVisitor |
| **P0** | Cat 1: VALUES unsupported | Real gap | 37 | Medium — implement VALUES node support in Calcite visitor |
| **P1** | Cat 3: Relevance pushdown | Real bug | 21 | Low — fix PredicateAnalyzer.visitRelevanceFunc() to use AliasPair.from() |
| **P1** | Cat 6: JOIN/comma-join syntax | Real gap | 2 | Medium — extend ANTLR SQL grammar |
| **P2** | Cat 2: FTS functions | Expected | 71 | High — register all legacy FTS aliases |
| **P2** | Cat 4: highlight() | Expected | 12 | High — implement highlight in Calcite |
| **P2** | Cat 6: String literal escaping | Real gap | 3 | Low — fix ANTLR grammar for double-quotes and backslash escapes |
| **P3** | Cat 7: IFNULL+GROUP BY | Needs investigation | 1 | Unknown |
| **N/A** | Cat 8: Concurrency false positive | Gap analyzer limitation | 2 | N/A |

---

## How to Run

```bash
# Run SQL gap analysis (all SQL IT tests)
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.*IT' -Dunified.gap.analysis=true

# Run specific test class
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.sql.MatchIT' -Dunified.gap.analysis=true

# Report output
cat integ-test/build/gap-analysis-report.txt
```

---

## Changes Made for This Analysis

| File | Change |
|------|--------|
| `legacy/.../RestSqlAction.java` | Restored V2 engine routing as default; unified handler only for `mode=ansi` |
| `integ-test/.../UnifiedQueryGapAnalyzer.java` | Fixed schema caching (shared HashMap); unwrap root cause in GapResult.failure() |
