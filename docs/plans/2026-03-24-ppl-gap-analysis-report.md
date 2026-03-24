# Unified Query API — PPL Gap Analysis Report

**Date:** 2026-03-24  
**Branch:** `poc/unified-sql-support-gap-analysis`  
**Scope:** PPL IT tests (`SearchCommandIT`) — 86 queries  
**Pipeline:** `UnifiedQueryPlanner` → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`

---

## Summary

| Metric | Value |
|--------|-------|
| Total Queries | 86 |
| Success | 47 (54.7%) |
| Failed | 39 (45.3%) |
| Failure Phase | 100% PLAN |
| Root Cause | Double-quoted string literals in PPL search command |

**Zero COMPILE or EXECUTE phase failures** — once a query is successfully planned, it compiles and executes correctly.

---

## Category 1: Double-Quoted String Literals (37 queries)

**Phase:** PLAN  
**Error:** `SyntaxCheckException: ["] is not a valid term at this part of the query`  
**Root Cause:** The unified PPL parser's ANTLR grammar does not accept double-quoted strings (`"value"`) as valid string literals in search command expressions. The V2 PPL engine handles these, but the unified planner does not.

### Sub-Category 1a: Free-Text / Phrase Search (8 queries)

Queries using double-quoted strings as free-text search terms after `source=<index>`:

| Test Method | Sample Query |
|-------------|-------------|
| `testSearchWithPhraseSearch` | `search source=...otel_logs "Payment failed" \| fields body` |
| `testSearchWithFreeText` | `search source=...otel_logs "@example.com" \| fields body \| head 5` |
| `testSearchWithMultipleFreeTextTerms` | `search source=...otel_logs "email" "user" \| sort time \| fields body` |
| `testSearchWithEmailInBody` | `search source=...otel_logs "john.doe+newsletter@company.com" \| fields body` |
| `testSearchWithLuceneSpecialCharacters` | `search source=...otel_logs "wildcard* fuzzy~2" \| fields body` |
| `testSearchWithSQLInjectionPattern` | `search source=...otel_logs "DROP TABLE users" \| fields body` |
| `testSearchWithJSONSpecialChars` | `search source=...otel_logs"quotes\\"  and: $@#" \| fields body` |
| `testWildcardEscaping` | `search source=...otel_logs "wildcard\*" \| fields body` |

### Sub-Category 1b: Field Comparison with Double-Quoted Values (19 queries)

Queries using `field="value"` or `field!="value"` syntax:

| Test Method | Sample Query |
|-------------|-------------|
| `testSearchWithANDOperator` | `search source=...otel_logs severityText="ERROR" AND severityNumber>10` |
| `testSearchWithOROperator` | `search source=...otel_logs severityText="ERROR" OR severityText="WARN"` |
| `testSearchWithFieldEquals` | `search source=...otel_logs \`resource.attributes.service.name\`="..."` |
| `testSearchWithFieldNotEquals` | `search source=...otel_logs severityText!="INFO"` |
| `testDifferenceBetweenNOTAndNotEquals` | `search source=...otel_logs NOT \`attributes.user.email\`="user@example.com"` |
| `testSearchWithNestedEmailAttribute` | `search source=...otel_logs \`attributes.user.email\`="user@example.com"` |
| `testSearchWithTypeMismatch` | `search source=...otel_logs severityNumber="17"` |
| `testSearchWithIPAddress` | `search source=...otel_logs \`attributes.client.ip\`="..."` |
| `testSearchWithDateFormats` | `search source=...otel_logs @timestamp="..."` (3 date formats) |
| Others | `testSearchWithUpperCaseValue`, `testWildcardPatternMatching`, `testSearchMixedWithPipeCommands`, `testSearchWithSpanLengthInField`, `testSearchWithInvalidFieldName`, `testSearchWithComplexBooleanExpression`, `testSearchWithMultipleFieldTypes` |

### Sub-Category 1c: IN Operator with Double-Quoted Values (4 queries)

| Test Method | Sample Query |
|-------------|-------------|
| `testSearchWithINOperator` | `search source=...otel_logs severityText IN ("ERROR", "WARN", "FATAL")` |
| `testSearchWithSingleValueIN` | `search source=...otel_logs severityText IN ("ERROR")` |
| `testSearchWithSingleValueIN` | `search source=...otel_logs severityText IN ("\"ERROR\"")` |
| `testSearchWithDateINOperator` | `search source=...otel_logs @timestamp IN ("2024-01-15T10:30:00...", ...)` |

### Sub-Category 1d: Date Range Comparisons (4 queries)

| Test Method | Sample Query |
|-------------|-------------|
| `testSearchWithDateRangeComparisons` | `search source=...otel_logs @timestamp>"2024-01-15T10:30:00Z"` |
| `testSearchWithDateRangeComparisons` | `search source=...otel_logs @timestamp<="2024-01-15T10:30:01Z"` |
| `testSearchWithDateRangeComparisons` | `search source=...otel_logs @timestamp>="..." AND @timestamp<"..."` |
| `testSearchWithDateRangeComparisons` | `search source=...otel_logs @timestamp!="2024-01-15T10:30:00.123456789Z"` |

### Fix

Update the unified PPL ANTLR grammar to accept double-quoted strings as valid string literals in search command context, matching the V2 PPL engine behavior. This single fix resolves 37/39 failures (94.9%), bringing PPL success from **54.7% → ~97.7%**.

---

## Category 2: Special Index Names (2 queries)

**Phase:** PLAN  
**Error:** `SyntaxCheckException: [logs-2021.01.11] is not a valid term`  
**Root Cause:** Index names containing dots and hyphens (e.g., `logs-2021.01.11`, `logs-7.10.0-2021.01.11`) are not recognized as valid identifiers by the unified parser. The parser treats dots and hyphens as operators.

| Test Method | Sample Query |
|-------------|-------------|
| `testSearchCommandWithSpecialIndexName` | `search source=logs-2021.01.11` |
| `testSearchCommandWithSpecialIndexName` | `search source=logs-7.10.0-2021.01.11` |

### Fix

Update the unified PPL grammar's index name rule to allow dots and hyphens within unquoted index names, or require backtick-quoting for such names.

---

## Manual Verification

```bash
# Create otel_logs test index
curl -s -XPUT 'localhost:9200/opensearch-sql_test_index_otel_logs' -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"body":{"type":"text"},"severityText":{"type":"keyword"},"severityNumber":{"type":"integer"},"@timestamp":{"type":"date"},"time":{"type":"date"},"attributes.user.email":{"type":"keyword"},"attributes.client.ip":{"type":"ip"},"attributes.error.type":{"type":"keyword"},"attributes.span.duration":{"type":"keyword"},"resource.attributes.service.name":{"type":"keyword"}}}}'
curl -s -XPOST 'localhost:9200/opensearch-sql_test_index_otel_logs/_bulk' \
  -H 'Content-Type: application/json' --data-binary '
{"index":{}}
{"body":"Payment failed for user","severityText":"ERROR","severityNumber":17,"@timestamp":"2024-01-15T10:30:00.123456789Z","time":"2024-01-15T10:30:00Z","attributes.user.email":"user@example.com","resource.attributes.service.name":"payment-service"}
{"index":{}}
{"body":"User login successful","severityText":"INFO","severityNumber":9,"@timestamp":"2024-01-15T10:30:01.234567890Z","time":"2024-01-15T10:30:01Z","attributes.user.email":"admin@example.com","resource.attributes.service.name":"auth-service"}
{"index":{}}
{"body":"Connection timeout warning","severityText":"WARN","severityNumber":13,"@timestamp":"2024-01-15T10:30:02Z","time":"2024-01-15T10:30:02Z"}
'

# Test Category 1b: field="value" (fails in unified pipeline)
curl -s -XPOST 'localhost:9200/_plugins/_ppl' -H 'Content-Type: application/json' \
  -d '{"query": "search source=opensearch-sql_test_index_otel_logs severityText=\"ERROR\" | fields severityText"}'

# Test Category 1a: phrase search (fails in unified pipeline)
curl -s -XPOST 'localhost:9200/_plugins/_ppl' -H 'Content-Type: application/json' \
  -d '{"query": "search source=opensearch-sql_test_index_otel_logs \"Payment failed\" | fields body"}'

# Test Category 1c: IN operator (fails in unified pipeline)
curl -s -XPOST 'localhost:9200/_plugins/_ppl' -H 'Content-Type: application/json' \
  -d '{"query": "search source=opensearch-sql_test_index_otel_logs severityText IN (\"ERROR\", \"WARN\") | fields severityText"}'

# Test Category 2: special index name
curl -s -XPUT 'localhost:9200/logs-2021.01.11' -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"msg":{"type":"text"}}}}'
curl -s -XPOST 'localhost:9200/logs-2021.01.11/_doc' -H 'Content-Type: application/json' \
  -d '{"msg":"test"}'
curl -s -XPOST 'localhost:9200/_plugins/_ppl' -H 'Content-Type: application/json' \
  -d '{"query": "search source=logs-2021.01.11"}'
```

---

## Priority Matrix

| Priority | Category | Unique Queries | Fix Complexity |
|----------|----------|---------------|----------------|
| **P0** | Double-quoted string literals | 37 | Low — update ANTLR grammar string literal rule |
| **P1** | Special index names (dots/hyphens) | 2 | Low — update ANTLR grammar index name rule |

---

## How to Run

```bash
# Run PPL gap analysis (SearchCommandIT)
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.ppl.SearchCommandIT' -Dunified.gap.analysis=true

# Run PPL gap analysis (all PPL tests)
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.ppl.*' -Dunified.gap.analysis=true

# Report output
cat integ-test/build/gap-analysis-report.txt
```
