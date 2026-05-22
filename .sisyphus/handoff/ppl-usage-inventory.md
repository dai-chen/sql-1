# PPL Usage Inventory: Aggregate-over-Datetime-UDF Patterns

## 1. Search Scope

- `integ-test/src/test/java/org/opensearch/sql/ppl/` (all *.java files)
- `integ-test/src/test/java/org/opensearch/sql/ppl/dashboard/` (Java + .rst templates)
- `docs/user/ppl/` (all .md files)
- Cross-checked: `integ-test/src/test/java/org/opensearch/sql/sql/AggregationIT.java` (SQL comparison)

## 2. Key Finding: UDT Exposure Path in PPL

The UDT issue in PPL is NOT triggered by `AGG(date(field))` / `AGG(timestamp(field))` (zero PPL tests use this pattern — it only exists in SQL tests). Instead, it's triggered by:

**`stats ... by span(datetime_field, interval)`** — because `span()` on a datetime-typed field (birthdate, event.timestamp, start, @timestamp) produces a group key with UDT type (EXPR_TIMESTAMP/EXPR_DATE). When a subsequent `sort`, `rename`, `eval`, `head`, or `fields` command follows, it creates a Project/Sort above the Aggregate that references the UDT-typed group key via RexInputRef.

## 3. Pattern A — Passes Under Current Fix (Bare Aggregate, No Cascade)

**Definition**: `stats ... by span(datetime_field, interval)` with NO pipe after.

**PPL IT count**: 7 queries
**PPL doc count**: 7 examples

**Sample queries**:
1. `source=bank | stats count() by span(birthdate,1y)` — StatsCommandIT:542
2. `source=big5 | stats percentile(value, 50) as p50 by span(@timestamp, 12h) as half_day` — StatsCommandIT:762
3. `source=vpc | STATS count() by span(start, 30d)` — VpcFlowLogsPplDashboardIT:64
4. `source=vpc | STATS sum(bytes) by span(start, 30d)` — VpcFlowLogsPplDashboardIT:84
5. `source=vpc | STATS sum(packets) by span(start, 30d)` — VpcFlowLogsPplDashboardIT:104
6. `source=example | stats count() as cnt by span(birthday, 1y) as year` — docs/user/ppl/cmd/stats.md:327
7. `source=prometheus | stats avg(@value) by span(@timestamp,15s)` — docs/user/ppl/admin/connectors/prometheus_connector.md:160

**Note**: These also include `stats count() by span(birthdate,1M)` in ExplainIT (3 queries) but ExplainIT only checks plan output, not execution.

## 4. Pattern B — Would Fail Under Current Fix; Needs Project+Filter Cascade Fix (Tier C)

**Definition**: `stats ... by span(datetime_field, interval) | sort/rename/eval/head/fields`

### Sub-pattern B1: stats by span(datetime) | sort/rename/eval/head

**PPL IT count**: 15 queries (all in dashboard tests)
**PPL doc count**: 9 examples (NFW dashboard templates)

**Sample queries**:
1. `source=nfw | stats sum(event.netflow.pkts) as packet_count by span(event.timestamp, 2d) as timestamp_span, event.src_ip | rename event.src_ip as Source IP | sort - packet_count | head 10` — NfwPplDashboardIT:61
2. `source=nfw | stats count() as event_count by span(event.timestamp, 2d) as time_bucket, event.http.hostname | rename event.http.hostname as Hostname | sort - event_count` — NfwPplDashboardIT:195
3. `source=nfw | stats count() as Count by SPAN(event.timestamp, 2d) as timestamp_span, event.src_port | eval Source Port = CAST(event.src_port AS STRING) | sort - Count | HEAD 10` — NfwPplDashboardIT:342
4. `source=nfw | stats count() as Count by SPAN(event.timestamp, 2d) as timestamp_span, event.dest_port | eval Destination Port = CAST(event.dest_port AS STRING) | sort - Count | HEAD 10` — NfwPplDashboardIT:359
5. `source=bank | stats bucket_nullable=false count() as cnt by span(birthdate, 1month) | sort - cnt, span(birthdate,1month) | head 5` — StatsCommandIT:890

### Sub-pattern B2: HAVING-like (stats | where on datetime group key)

**PPL IT count**: 0
**PPL doc count**: 0

### Sub-pattern B3: Subquery wrapping datetime aggregate

**PPL IT count**: 0 (subquery.md example uses `date()` in WHERE, not in aggregate output)
**PPL doc count**: 0

### Sub-pattern B4: Scalar fn wrapping aggregate result (e.g., year(max(timestamp(field))))

**PPL IT count**: 0
**PPL doc count**: 0

## 5. Pattern C — Would Still Fail Even After Project+Filter Cascade Fix

**Definition**: Window over aggregate, set operations, join with aggregate-derived datetime field.

**PPL IT count**: 0
**PPL doc count**: 0

(Window functions over datetime aggregates exist only in SQL tests: AggregationIT:598-638)

## 6. Verdict Table

| Pattern | Tier | PPL IT count | PPL doc count | Impact |
|---|---|---|---|---|
| Bare `stats by span(datetime)` | A | 7 | 7 | covered by current fix |
| `stats by span(datetime) \| sort/rename/eval/head` | B | 15 | 9 | **broken today** |
| HAVING-like `stats \| where` on datetime key | B | 0 | 0 | theoretical |
| Subquery datetime agg | B | 0 | 0 | theoretical |
| Scalar fn wrapping agg result | B | 0 | 0 | theoretical |
| Window over datetime agg | C | 0 | 0 | SQL-only |
| Set ops over mixed datetime | C | 0 | 0 | not tested |

## 7. Recommendation: Tier C (Add Project/Filter Visits)

**Rationale**:
- Pattern B (15 IT queries + 9 doc examples) outnumbers Pattern A (7 IT + 7 docs) by 2:1.
- The NFW dashboard tests are the most "real-world" PPL queries in the repo — they represent actual observability dashboards. **Every single NFW query** uses `stats by span(timestamp) | rename | sort | head`, which is Pattern B.
- The `StatsCommandIT` sort-after-span tests (lines 890-904) are also Pattern B.
- Pattern C (window over datetime agg) is SQL-only and has zero PPL exposure.
- The fix for Tier C (adding `visit(Project)` and `visit(Filter)`) is a small, well-scoped change that covers the dominant real-world PPL pattern.

**Conclusion**: Implement Tier C. The cascade fix is essential for PPL correctness — without it, the most common real-world PPL dashboard pattern (`stats by span(timestamp) | sort | head`) remains broken.
