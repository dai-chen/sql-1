#!/usr/bin/env python3
"""Generate a markdown sanity test report from run-sanity-test.sh JSON output.

Usage:
  bash run-sanity-test.sh 2>/dev/null | python3 generate-sanity-report.py sanity-queries.json [output.md]

If output.md is omitted, prints to stdout.
"""
import json
import sys
from collections import OrderedDict

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <sanity-queries.json> [output.md]", file=sys.stderr)
    sys.exit(1)

queries_file = sys.argv[1]
output_file = sys.argv[2] if len(sys.argv) > 2 else None

with open(queries_file) as f:
    queries = json.load(f)

import re
raw = sys.stdin.read()
# The input is concatenated JSON objects (one per test + summary). Parse them.
decoder = json.JSONDecoder()
results = []
pos = 0
while pos < len(raw):
    match = re.match(r'\s*', raw[pos:])
    pos += match.end()
    if pos >= len(raw):
        break
    obj, end = decoder.raw_decode(raw, pos)
    results.append(obj)
    pos += end - pos
summary = results[-1]
entries = results[:-1]

lines = []
w = lines.append

w("# Analytics Engine SQL Sanity Test Report")
w("")
total = summary["summary"]["total"]
passed = summary["summary"]["passed"]
failed = summary["summary"]["failed"]
rate = passed * 100 // total if total > 0 else 0
w(f"**{total} queries | {passed} PASS | {failed} FAIL | Pass Rate: {rate}%**")
w("")
w("All passing tests have results **verified** against expected values (exact row match).")
w("")

# Group by category
groups = OrderedDict()
for i, e in enumerate(entries):
    cat = e["category"]
    if cat not in groups:
        groups[cat] = []
    groups[cat].append((e, queries[i]))

for cat, items in groups.items():
    cat_passed = sum(1 for e, _ in items if e["status"] == "PASS")
    cat_total = len(items)
    icon = "✅" if cat_passed == cat_total else ("❌" if cat_passed == 0 else "🟡")
    w(f"## {icon} {cat} ({cat_passed}/{cat_total})")
    w("")
    w("| # | Name | Query | Result |")
    w("|---|------|-------|--------|")
    for idx, (item, qdef) in enumerate(items, 1):
        query = qdef["query"].replace("|", "\\|")
        if len(query) > 85:
            query = query[:82] + "..."

        resp = item["response"]
        reason = item.get("reason", "")

        if item["status"] == "PASS":
            if "datarows" in resp:
                rows = resp["datarows"]
                if len(rows) <= 3:
                    r = "✅ " + json.dumps(rows, ensure_ascii=False)
                else:
                    r = f"✅ {len(rows)} rows: {json.dumps(rows[:2], ensure_ascii=False)}..."
                if len(r) > 110:
                    r = r[:107] + "..."
            elif "calcite" in resp:
                r = "✅ (plan returned)"
            else:
                r = "✅ (verified)"
        else:
            r = "❌ " + reason[:100]

        r = r.replace("|", "\\|")
        w(f"| {idx} | {item['name']} | `{query}` | {r} |")
    w("")

# Failure summary
failures = [(e, queries[i]) for i, e in enumerate(entries) if e["status"] == "FAIL"]
if failures:
    w("---")
    w("")
    w("## Failure Summary")
    w("")
    w("| # | Name | Category | Error | Root Cause |")
    w("|---|------|----------|-------|------------|")
    for n, (e, _) in enumerate(failures, 1):
        reason = e.get("reason", "")[:70]
        if "Stage 0 failed" in reason:
            cause = "TaskResourceTrackingService thread reuse"
        elif "UNION" in reason:
            cause = "UNION/UNION ALL not supported by SQL parser"
        elif "not a valid term" in reason:
            cause = "Syntax not supported by SQL parser"
        else:
            cause = "See error"
        w(f"| {n} | {e['name']} | {e['category']} | `{reason}` | {cause} |")
    w("")

output = "\n".join(lines) + "\n"

if output_file:
    with open(output_file, "w") as f:
        f.write(output)
    print(f"Report written to {output_file}", file=sys.stderr)
    print(f"  {total} queries | {passed} PASS | {failed} FAIL | {rate}%", file=sys.stderr)
else:
    print(output)
