#!/bin/bash
# Parses JUnit XML test results and produces a failure breakdown table.
set -euo pipefail

DIR="${1:-integ-test/build/test-results/analyticsSqlCompatibility}"
[ ! -d "$DIR" ] && { echo "ERROR: Directory not found: $DIR" >&2; exit 1; }

# Extract totals from testsuite elements (skip the Gradle wrapper suite)
read -r TOTAL SKIPPED FAILURES <<< $(
  grep '<testsuite ' "$DIR"/*.xml | grep -v 'Gradle Test Run' |
  sed 's/.*tests="\([^"]*\)".*skipped="\([^"]*\)".*failures="\([^"]*\)".*/\1 \2 \3/' |
  awk '{t+=$1; s+=$2; f+=$3}END{print t, s, f}'
)
EXECUTED=$((TOTAL - SKIPPED))
PASSED=$((EXECUTED - FAILURES))
PCT=$(awk "BEGIN{printf \"%.1f\", $PASSED/$EXECUTED*100}")

# Extract and classify failure messages
TMP=$(mktemp)
sed -n 's/.*<failure message="\([^"]*\)".*/\1/p' "$DIR"/*.xml |
  sed 's/&#10;/ /g;s/&quot;/"/g;s/&amp;/\&/g;s/&lt;/</g;s/&gt;/>/g' |
while IFS= read -r msg; do
  [ -z "$msg" ] && continue
  if echo "$msg" | grep -qi "Unable to convert call TIMESTAMP"; then
    echo "AE|BackendPlanAdapter not invoked for LOCAL_COMPUTE|Unable to convert call TIMESTAMP"
  elif echo "$msg" | grep -qi "DataFormatAwareEngine\|Cannot apply function on indexer"; then
    echo "AE|Lucene backend cant execute on composite index|DataFormatAwareEngine / all shards failed"
  elif echo "$msg" | grep -qi "NulError\|panicked\|Rust"; then
    echo "AE|DataFusion panics on certain patterns|DataFusion panic / NulError"
  elif echo "$msg" | grep -qi "Stage 0 failed\|streaming fragment"; then
    echo "AE|Streaming fragment / null text|Stage 0 failed / streaming fragment"
  elif echo "$msg" | grep -qi "Unable to convert.*DATE_PART"; then
    echo "AE|Substrait cant serialize DATE_PART|Unable to convert DATE_PART"
  elif echo "$msg" | grep -qi "Window.*OVER.*not supported\|No shuffle exchange"; then
    echo "AE|No shuffle exchange available|Window OVER not supported"
  elif echo "$msg" | grep -qi "Unrecognized filter operator"; then
    echo "AE|wildcard_query matchphrase not registered|Unrecognized filter operator"
  elif echo "$msg" | grep -qi "IS_NULL.*not supported\|IS_NOT_NULL.*not supported"; then
    echo "AE|Not in STANDARD_PROJECT_OPS|IS_NULL/IS_NOT_NULL not supported"
  elif echo "$msg" | grep -qi "Unable to convert.*to_timestamp"; then
    echo "AE|Substrait conversion|Unable to convert to_timestamp"
  elif echo "$msg" | grep -qi "Unable to convert.*convert_tz"; then
    echo "AE|Substrait conversion|Unable to convert convert_tz"
  elif echo "$msg" | grep -qi "No backend supports.*TIMESTAMP"; then
    echo "AE|Edge cases where adapter check fails|No backend supports TIMESTAMP"
  elif echo "$msg" | grep -qi "No backend supports scalar function"; then
    echo "AE|Scalar function not supported|No backend supports scalar function"
  elif echo "$msg" | grep -qi "No backend can scan"; then
    echo "AE|Backend cant handle the index/query|No backend can scan"
  elif echo "$msg" | grep -qi "all shards failed"; then
    echo "AE|Lucene backend cant execute on composite index|all shards failed"
  elif echo "$msg" | grep -qi "Failed to plan query"; then
    echo "SQL Plugin|Calcite planning failure|Failed to plan query"
  elif echo "$msg" | grep -qi "can.t resolve Symbol\|not found.*type env"; then
    echo "SQL Plugin|Nested paths, _id, post-agg fields|Field not found (cant resolve Symbol)"
  elif echo "$msg" | grep -qi "Grammar.*error\|parse.*error\|ParseException\|ParserException\|no viable alternative\|is not a valid term"; then
    echo "SQL Plugin|Grammar/parse error|Grammar/parse error"
  elif echo "$msg" | grep -qi "NullPointerException.*RexNode\|NPE.*Calcite"; then
    echo "SQL Plugin|Nested wildcard / Calcite NPE|NullPointerException in Calcite"
  elif echo "$msg" | grep -qi "Highlight.*unsupported\|highlight"; then
    echo "SQL Plugin|Not implemented in Calcite|Highlight unsupported"
  elif echo "$msg" | grep -qi "Illegal nested field\|nested field"; then
    echo "SQL Plugin|Nested paths, _id, post-agg fields|Illegal nested field"
  elif echo "$msg" | grep -qi "cursor.*not found\|cursor.*property\|JSONObject.*cursor"; then
    echo "Mixed|Pagination not supported in AE path|cursor not found"
  elif echo "$msg" | grep -qi "optJSONArray\|JSONArray.*not found\|JSONObject.*not found"; then
    echo "Mixed|Response format mismatch|JSON schema/array not found"
  elif echo "$msg" | grep -qi "AssertionError.*Expected.*but\|expected:<\|but was:<"; then
    echo "Mixed|Wrong results, explain format, error codes|AssertionError (expected vs actual)"
  elif echo "$msg" | grep -qi "AssertionError"; then
    echo "Mixed|Wrong results, explain format, error codes|AssertionError"
  elif echo "$msg" | grep -qi "Memory circuit"; then
    echo "AE|Resource pressure|Memory circuit broken"
  elif echo "$msg" | grep -qi "no such index\|IndexNotFoundException"; then
    echo "SQL Plugin|Missing test index|IndexNotFoundException"
  elif echo "$msg" | grep -qi "IllegalStateException.*Unexpected IOException"; then
    echo "SQL Plugin|Unexpected IOException|IllegalStateException"
  elif echo "$msg" | grep -qi "SemanticCheckException\|All result types"; then
    echo "SQL Plugin|Type checking / semantic error|SemanticCheckException"
  else
    echo "Other|Uncategorized|$(echo "$msg" | cut -c1-70 | sed 's/|/-/g')"
  fi
done | sort | uniq -c | sort -rn > "$TMP"

# Output report
echo "# Analytics SQL IT — Failure Breakdown"
echo ""
echo "**Summary:** ${PASSED}/${EXECUTED} executed passed (${PCT}%), ${FAILURES} failed, ${SKIPPED} skipped"
echo ""
echo "| # | Count | Owner | Error | Root Cause |"
echo "|---|---|---|---|---|"

IDX=0
while read -r count rest; do
  IDX=$((IDX + 1))
  owner=$(echo "$rest" | cut -d'|' -f1)
  root_cause=$(echo "$rest" | cut -d'|' -f2)
  error=$(echo "$rest" | cut -d'|' -f3)
  echo "| $IDX | $count | $owner | $error | $root_cause |"
done < "$TMP"

echo ""
echo "## Totals by Owner"
echo ""
echo "| Owner | Count | % |"
echo "|---|---|---|"
awk '{count=$1; $1=""; split($0, a, "|"); owner=a[1]; gsub(/^[[:space:]]+|[[:space:]]+$/, "", owner); totals[owner]+=count; grand+=count}
  END{for(o in totals) printf "| %s | %d | %.1f%% |\n", o, totals[o], totals[o]/grand*100}' "$TMP" | sort -t'|' -k3 -rn

rm -f "$TMP"
