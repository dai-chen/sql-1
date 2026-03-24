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
| Success | 84 (97.7%) |
| Failed | 2 (2.3%) |
| Failure Phase | 100% PLAN |
| Root Cause | Special index names with dots/hyphens |

**Zero COMPILE or EXECUTE phase failures.** After fixing the gap analyzer's JSON escaping issue (which caused 37 false positives from double-quoted string literals), only 2 real failures remain.

---

## Category 1: Special Index Names (2 queries)

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
