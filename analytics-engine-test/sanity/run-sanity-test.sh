#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ES_URL="http://localhost:9200"

# Check cluster health
if ! curl -s "$ES_URL/_cluster/health" >/dev/null 2>&1; then
  echo "ERROR: Cluster not reachable at $ES_URL" >&2
  exit 1
fi
echo "Cluster is healthy" >&2

# Seed test data
echo "Seeding test data..." >&2
curl -s -X DELETE "$ES_URL/sanity_test" >/dev/null 2>&1 || true
curl -s -X DELETE "$ES_URL/sanity_dept" >/dev/null 2>&1 || true
curl -s -X DELETE "$ES_URL/sanity_datetime" >/dev/null 2>&1 || true

curl -s -X PUT "$ES_URL/sanity_test" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "pluggable.dataformat.enabled": true,
      "pluggable.dataformat": "composite",
      "composite.primary_data_format": "parquet",
      "composite.secondary_data_formats": "lucene"
    }
  },
  "mappings": {
    "properties": {
      "name": {"type": "text"},
      "age": {"type": "integer"},
      "city": {"type": "keyword"},
      "dept_id": {"type": "integer"}
    }
  }
}' >/dev/null

curl -s -X PUT "$ES_URL/sanity_dept" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "pluggable.dataformat.enabled": true,
      "pluggable.dataformat": "composite",
      "composite.primary_data_format": "parquet",
      "composite.secondary_data_formats": "lucene"
    }
  },
  "mappings": {
    "properties": {
      "id": {"type": "integer"},
      "dept": {"type": "keyword"}
    }
  }
}' >/dev/null

curl -s -X PUT "$ES_URL/sanity_datetime" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "pluggable.dataformat.enabled": true,
      "pluggable.dataformat": "composite",
      "composite.primary_data_format": "parquet",
      "composite.secondary_data_formats": "lucene"
    }
  },
  "mappings": {
    "properties": {
      "event": {"type": "keyword"},
      "ts": {"type": "date"},
      "event_date": {"type": "date", "format": "yyyy-MM-dd"},
      "event_time": {"type": "date", "format": "HH:mm:ss"},
      "amount": {"type": "integer"}
    }
  }
}' >/dev/null

curl -s -X POST "$ES_URL/_bulk?refresh=true" -H 'Content-Type: application/json' -d '
{"index":{"_index":"sanity_test"}}
{"name":"Alice","age":32,"city":"Seattle","dept_id":1}
{"index":{"_index":"sanity_test"}}
{"name":"Bob","age":25,"city":"Portland","dept_id":2}
{"index":{"_index":"sanity_test"}}
{"name":"Carol","age":28,"city":"Seattle","dept_id":1}
{"index":{"_index":"sanity_test"}}
{"name":"Dave","age":35,"city":"Portland","dept_id":2}
{"index":{"_index":"sanity_test"}}
{"name":"Eve","age":22,"city":"Denver","dept_id":1}
{"index":{"_index":"sanity_dept"}}
{"id":1,"dept":"Engineering"}
{"index":{"_index":"sanity_dept"}}
{"id":2,"dept":"Marketing"}
{"index":{"_index":"sanity_datetime"}}
{"event":"login","ts":"2024-03-15T10:30:00","event_date":"2024-03-15","event_time":"10:30:00","amount":100}
{"index":{"_index":"sanity_datetime"}}
{"event":"purchase","ts":"2024-03-15T14:00:00","event_date":"2024-03-15","event_time":"14:00:00","amount":250}
{"index":{"_index":"sanity_datetime"}}
{"event":"login","ts":"2024-03-16T09:00:00","event_date":"2024-03-16","event_time":"09:00:00","amount":50}
{"index":{"_index":"sanity_datetime"}}
{"event":"purchase","ts":"2024-03-16T11:30:00","event_date":"2024-03-16","event_time":"11:30:00","amount":300}
' >/dev/null

echo "Test data seeded" >&2

# Run queries and verify results
passed=0
failed=0
total=$(jq 'length' "$SCRIPT_DIR/sanity-queries.json")

for i in $(seq 0 $((total - 1))); do
  entry=$(jq -c ".[$i]" "$SCRIPT_DIR/sanity-queries.json")
  category=$(echo "$entry" | jq -r '.category')
  name=$(echo "$entry" | jq -r '.name')
  query=$(echo "$entry" | jq -r '.query')
  expected=$(echo "$entry" | jq -c '.expected')

  # Choose endpoint based on category
  if [[ "$category" == "EXPLAIN" ]]; then
    endpoint="$ES_URL/_plugins/_sql/_explain"
  else
    endpoint="$ES_URL/_plugins/_sql"
  fi

  response=$(curl -s -w '\n%{http_code}' -X POST "$endpoint" \
    -H 'Content-Type: application/json' \
    -d "$(jq -n --arg q "$query" '{query: $q}')" 2>/dev/null) || true

  http_code=$(echo "$response" | tail -1)
  body=$(echo "$response" | sed '$d')

  # Validate body is JSON
  if ! echo "$body" | jq . >/dev/null 2>&1; then
    status="FAIL"
    reason="HTTP $http_code: $body"
    failed=$((failed + 1))
    body=$(jq -n --arg e "$reason" '{error: $e}')
  else
    has_error=$(echo "$body" | jq 'has("error")') || has_error="true"
    if [[ "$http_code" != "200" || "$has_error" == "true" ]]; then
      status="FAIL"
      reason=$(echo "$body" | jq -r '.error // .message // "HTTP error"' 2>/dev/null | head -1)
      failed=$((failed + 1))
    elif [[ "$expected" == "null" ]]; then
      # No expected result defined — pass if HTTP 200 + no error
      status="PASS"
      reason=""
      passed=$((passed + 1))
    else
      # Verify datarows against expected
      actual_rows=$(echo "$body" | jq -c '.datarows')
      expected_rows=$(echo "$expected" | jq -c '.rows')
      ordered=$(echo "$expected" | jq -r '.ordered')

      if [[ "$ordered" == "true" ]]; then
        # Ordered comparison
        if [[ "$actual_rows" == "$expected_rows" ]]; then
          status="PASS"
          reason=""
          passed=$((passed + 1))
        else
          status="FAIL"
          reason="Result mismatch (ordered). Expected: $expected_rows Got: $actual_rows"
          failed=$((failed + 1))
        fi
      else
        # Unordered comparison — sort both and compare
        actual_sorted=$(echo "$actual_rows" | jq -c 'sort')
        expected_sorted=$(echo "$expected_rows" | jq -c 'sort')
        if [[ "$actual_sorted" == "$expected_sorted" ]]; then
          status="PASS"
          reason=""
          passed=$((passed + 1))
        else
          status="FAIL"
          reason="Result mismatch (unordered). Expected: $expected_rows Got: $actual_rows"
          failed=$((failed + 1))
        fi
      fi
    fi
  fi

  jq -n --arg cat "$category" --arg name "$name" --arg status "$status" \
    --arg reason "$reason" --argjson resp "$body" \
    '{category: $cat, name: $name, status: $status, reason: $reason, response: $resp}'
done

# Summary
jq -n --argjson total "$total" --argjson passed "$passed" --argjson failed "$failed" \
  '{summary: {total: $total, passed: $passed, failed: $failed}}'
