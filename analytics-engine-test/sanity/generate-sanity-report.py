#!/usr/bin/env python3
"""Generate a markdown sanity test report from run-sanity-test.sh JSON output.

Usage:
  bash run-sanity-test.sh 2>/dev/null | python3 generate-sanity-report.py sanity-queries.json [output.md]
"""
import json
import sys
import re
from collections import OrderedDict

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <sanity-queries.json> [output.md]", file=sys.stderr)
    sys.exit(1)

queries_file = sys.argv[1]
output_file = sys.argv[2] if len(sys.argv) > 2 else None

with open(queries_file) as f:
    queries = json.load(f)

raw = sys.stdin.read()
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

# Group by category
groups = OrderedDict()
for i, e in enumerate(entries):
    cat = e["category"]
    if cat not in groups:
        groups[cat] = []
    groups[cat].append((e, queries[i]))

# Classify: functions vs statements
# Functions: categories starting with "Functions:" or "Full-text search"
func_groups = OrderedDict()
stmt_groups = OrderedDict()
for cat, items in groups.items():
    if cat.startswith("Functions:"):
        # Normalize family name
        key = cat.replace("Functions: ", "")
        if key not in func_groups:
            func_groups[key] = []
        func_groups[key].extend(items)
    else:
        stmt_groups[cat] = items

total = summary["summary"]["total"]
passed = summary["summary"]["passed"]
failed = summary["summary"]["failed"]
rate = f"{passed * 100 // total}%" if total > 0 else "0%"

# === Header ===
w("# Analytics Engine SQL Sanity Test Report")
w("")
w(f"Summary: {passed}/{total} passed ({rate})")
w("")

# === SQL Statements Summary ===
w("## SQL Statements")
w("")
w("| Category | Passed | Failed | Total |")
w("|----------|--------|--------|-------|")
for cat, items in stmt_groups.items():
    p = sum(1 for e, _ in items if e["status"] == "PASS")
    f = len(items) - p
    w(f"| {cat} | {p} | {f} | {len(items)} |")
w("")

# === SQL Functions Summary ===
w("## SQL Functions")
w("")
w("| Family | Total | Pass | Fail | Coverage |")
w("|--------|-------|------|------|----------|")
func_total_all = 0
func_passed_all = 0
for family, items in func_groups.items():
    ct = len(items)
    cp = sum(1 for e, _ in items if e["status"] == "PASS")
    cf = ct - cp
    cov = f"{cp * 100 // ct}%" if ct > 0 else "0%"
    w(f"| {family} | {ct} | {cp} | {cf} | {cov} |")
    func_total_all += ct
    func_passed_all += cp
func_failed_all = func_total_all - func_passed_all
w(f"| **Total** | **{func_total_all}** | **{func_passed_all}** | **{func_failed_all}** | **{func_passed_all * 100 // func_total_all}%** |")
w("")

# === Results: SQL Statements ===
w("## Results: SQL Statements")
w("")
w("| Category | Name | Query | Status | Result / Error |")
w("|----------|------|-------|--------|----------------|")
for cat, items in stmt_groups.items():
    for item, qdef in items:
        query = qdef["query"].replace("|", "\\|")
        if len(query) > 80:
            query = query[:77] + "..."
        status = item["status"]
        if status == "PASS":
            resp = item["response"]
            if "datarows" in resp:
                rows = resp["datarows"]
                r = ", ".join(json.dumps(row, ensure_ascii=False) for row in rows)
                if len(r) > 100:
                    r = r[:97] + "..."
            elif "calcite" in resp:
                r = "(plan output)"
            else:
                r = "(verified)"
        else:
            r = item.get("reason", "")[:100]
        r = r.replace("|", "\\|")
        w(f"| {cat} | {item['name']} | {query} | {status} | {r} |")
w("")

# === Results: SQL Functions ===
w("## Results: SQL Functions")
w("")
w("| Family | Function | Query | Status | Result / Error |")
w("|--------|----------|-------|--------|----------------|")
for family, items in func_groups.items():
    for item, qdef in items:
        # Extract function name
        name = item["name"]
        if "_" in name:
            parts = name.split("_", 1)
            func_name = parts[1].upper()
        else:
            func_name = name.upper()
        query = qdef["query"].replace("|", "\\|")
        if len(query) > 60:
            query = query[:57] + "..."
        status = item["status"]
        if status == "PASS":
            resp = item["response"]
            if "datarows" in resp:
                r = json.dumps(resp["datarows"], ensure_ascii=False)
                if len(r) > 60:
                    r = r[:57] + "..."
            else:
                r = "(verified)"
        else:
            r = item.get("reason", "")[:80]
        r = r.replace("|", "\\|")
        w(f"| {family} | {func_name} | {query} | {status} | {r} |")
w("")

# === Appendix ===
w("## Appendix")
w("")
w("### Test Setup")
w("")
w("* OpenSearch core: latest main with LuceneFilterDelegationHandle fix")
w("* SQL Plugin: feature/analytics-engine-compat-report branch")
w("* Cluster: single-node with analytics-engine + 9 plugins, `-da -dsa`")
w("")
w("### Test Data")
w("")
w("**sanity_test** (composite/parquet + lucene):")
w("")
w("| name | age | city | dept_id |")
w("|------|-----|------|---------|")
w("| Alice | 32 | Seattle | 1 |")
w("| Bob | 25 | Portland | 2 |")
w("| Carol | 28 | Seattle | 1 |")
w("| Dave | 35 | Portland | 2 |")
w("| Eve | 22 | Denver | 1 |")
w("")
w("**sanity_dept** (composite/parquet + lucene):")
w("")
w("| id | dept |")
w("|----|------|")
w("| 1 | Engineering |")
w("| 2 | Marketing |")
w("")
w("**sanity_datetime** (composite/parquet + lucene):")
w("")
w("| event | ts | event_date | event_time | amount |")
w("|-------|-----|------------|------------|--------|")
w("| login | 2024-03-15T10:30:00 | 2024-03-15 | 10:30:00 | 100 |")
w("| purchase | 2024-03-15T14:00:00 | 2024-03-15 | 14:00:00 | 250 |")
w("| login | 2024-03-16T09:00:00 | 2024-03-16 | 09:00:00 | 50 |")
w("| purchase | 2024-03-16T11:30:00 | 2024-03-16 | 11:30:00 | 300 |")
w("")

output = "\n".join(lines) + "\n"

if output_file:
    with open(output_file, "w") as f:
        f.write(output)
    print(f"Report written to {output_file}", file=sys.stderr)
    print(f"  {total} queries | {passed} PASS | {failed} FAIL | {rate}", file=sys.stderr)
else:
    print(output)
