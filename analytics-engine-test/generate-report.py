#!/usr/bin/env python3
"""Parse JUnit XML results and generate a bucketed analytics-engine compatibility report."""
import xml.etree.ElementTree as ET
import os, sys, glob
from collections import defaultdict

def categorize(msg):
    if "DataFormatAwareEngine" in msg:
        return "Direct Shard Op"
    elif "400 Bad Request" in msg:
        return "Bad Request"
    elif "AssertionError" in msg or "expected" in msg.lower():
        return "Result Mismatch"
    elif "timeout" in msg.lower() or "timed out" in msg.lower():
        return "Timeout"
    elif "NullPointerException" in msg:
        return "NPE"
    elif "index_not_found" in msg:
        return "Index Setup"
    else:
        return "Other Error"

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <results-dir> <output.md>")
        sys.exit(1)

    results_dir, output_path = sys.argv[1], sys.argv[2]
    classes = defaultdict(lambda: {"passed": 0, "failed": 0, "skipped": 0, "failures": []})

    for xml_file in sorted(glob.glob(os.path.join(results_dir, "*.xml"))):
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            classname = root.get("name", "")
            if not (classname.startswith("org.opensearch.sql.sql.") or
                    classname.startswith("org.opensearch.sql.legacy.")):
                continue
            for tc in root.findall(".//testcase"):
                tc_class = tc.get("classname", classname)
                if not (tc_class.startswith("org.opensearch.sql.sql.") or
                        tc_class.startswith("org.opensearch.sql.legacy.")):
                    continue
                failure = tc.find("failure")
                error = tc.find("error")
                skipped_el = tc.find("skipped")
                short_class = tc_class.replace("org.opensearch.sql.", "")
                if skipped_el is not None:
                    classes[short_class]["skipped"] += 1
                elif failure is not None or error is not None:
                    classes[short_class]["failed"] += 1
                    msg = (failure if failure is not None else error).get("message", "")[:200]
                    classes[short_class]["failures"].append((tc.get("name"), msg))
                else:
                    classes[short_class]["passed"] += 1
        except Exception:
            pass

    total_p = sum(c["passed"] for c in classes.values())
    total_f = sum(c["failed"] for c in classes.values())
    total_s = sum(c["skipped"] for c in classes.values())
    total = total_p + total_f + total_s

    if total == 0:
        print("ERROR: No test results found for sql.sql.* or sql.legacy.*")
        sys.exit(1)

    fail_cats = defaultdict(int)
    for c in classes.values():
        for _, msg in c["failures"]:
            fail_cats[categorize(msg)] += 1

    sql_v2 = {k: v for k, v in classes.items() if k.startswith("sql.")}
    legacy = {k: v for k, v in classes.items() if k.startswith("legacy.")}

    def area_totals(d):
        p = sum(c["passed"] for c in d.values())
        f = sum(c["failed"] for c in d.values())
        s = sum(c["skipped"] for c in d.values())
        return p, f, s

    sql_p, sql_f, sql_s = area_totals(sql_v2)
    leg_p, leg_f, leg_s = area_totals(legacy)

    lines = []
    w = lines.append

    w("# SQL V2 + Legacy IT — Analytics Engine Compatibility Report\n")
    w(f"**Total Tests:** {total} | ✅ Passed: {total_p} ({total_p/total*100:.1f}%) | "
      f"❌ Failed: {total_f} ({total_f/total*100:.1f}%) | ⏭️ Skipped: {total_s} ({total_s/total*100:.1f}%)\n")
    w("")
    w("| Area | Passed | Failed | Skipped | Pass Rate |")
    w("|------|-------:|-------:|--------:|----------:|")
    if sql_p + sql_f > 0:
        w(f"| SQL V2 (`sql.sql.*`) | {sql_p} | {sql_f} | {sql_s} | {sql_p/(sql_p+sql_f)*100:.1f}% |")
    if leg_p + leg_f > 0:
        w(f"| Legacy (`sql.legacy.*`) | {leg_p} | {leg_f} | {leg_s} | {leg_p/(leg_p+leg_f)*100:.1f}% |")
    w("")

    w("## Failure Categories\n")
    w("| Category | Count | % |")
    w("|----------|------:|--:|")
    for cat, cnt in sorted(fail_cats.items(), key=lambda x: -x[1]):
        w(f"| {cat} | {cnt} | {cnt/total_f*100:.1f}% |")
    w("")

    w("## ✅ Fully Passing Test Classes\n")
    fully_passing = sorted([(k, v) for k, v in classes.items() if v["failed"] == 0 and v["passed"] > 0],
                           key=lambda x: -x[1]["passed"])
    for name, v in fully_passing:
        w(f"- **{name}** ({v['passed']} tests)")
    w("")

    w("## 🟡 Partially Passing (>50%)\n")
    w("| Class | Passed | Failed | Rate |")
    w("|-------|-------:|-------:|-----:|")
    partial = sorted([(k, v) for k, v in classes.items()
                      if v["failed"] > 0 and v["passed"] > 0 and v["passed"]/(v["passed"]+v["failed"]) > 0.5],
                     key=lambda x: -x[1]["passed"]/(x[1]["passed"]+x[1]["failed"]))
    for name, v in partial:
        rate = v["passed"]/(v["passed"]+v["failed"])*100
        w(f"| {name} | {v['passed']} | {v['failed']} | {rate:.0f}% |")
    w("")

    w("## ❌ Failing Test Classes\n")
    w("| Class | Passed | Failed | Skipped | Top Failure |")
    w("|-------|-------:|-------:|--------:|-------------|")
    failing = sorted([(k, v) for k, v in classes.items()
                      if v["failed"] > 0 and (v["passed"] == 0 or v["passed"]/(v["passed"]+v["failed"]) <= 0.5)],
                     key=lambda x: -x[1]["failed"])
    for name, v in failing:
        top_fail = categorize(v["failures"][0][1]) if v["failures"] else "?"
        w(f"| {name} | {v['passed']} | {v['failed']} | {v['skipped']} | {top_fail} |")
    w("")

    w("## 🔍 Result Mismatch Details\n")
    for name, v in sorted(classes.items()):
        mismatches = [(t, m) for t, m in v["failures"] if categorize(m) == "Result Mismatch"]
        if mismatches:
            w(f"### {name}")
            for test, msg in mismatches[:5]:
                w(f"- `{test}`: {msg[:150]}")
            w("")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Report written to {output_path}")
    print(f"  Total: {total} | Pass: {total_p} ({total_p/total*100:.1f}%) | Fail: {total_f} | Skip: {total_s}")

if __name__ == "__main__":
    main()
