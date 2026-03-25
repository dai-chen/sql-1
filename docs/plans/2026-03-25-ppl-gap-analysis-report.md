# Unified Query API — PPL Gap Analysis Report

**Date:** 2026-03-25
**Branch:** `poc/gap-analysis-on-formal` (based on `feature/unified-calcite-sql-support`)
**Scope:** All PPL IT tests (`org.opensearch.sql.ppl.*IT`) — 1593 queries
**Pipeline:** `UnifiedQueryPlanner` → `UnifiedQueryCompiler` → `PreparedStatement.executeQuery()`
**Previous Report:** [2026-03-24](./2026-03-24-ppl-gap-analysis-report.md)

---

## What Changed Since Mar 24

> The Mar 24 report only detected **exception-based failures** (PLAN/COMPILE/EXECUTE phases).
> Queries that executed successfully in both V2 and Calcite but returned **different data** were
> counted as "success". This report adds **RESULT_MISMATCH** detection — comparing actual query
> results between the two pipelines.

| Metric | Mar 24 | Mar 25 | Delta |
|--------|--------|--------|-------|
| Total Queries | 1593 | 1593 | — |
| Success | 1566 (98.3%) | 1420 (89.1%) | **−146** |
| Failed (Exception) | 27 (1.7%) | 27 (1.7%) | — |
| Failed (Different Result) | *not measured* | 146 (9.2%) | **+146 (NEW)** |
| Total Failed | 27 (1.7%) | 173 (10.9%) | +146 |

**Key insight:** 146 queries that were previously counted as "success" actually return different results between V2 and Calcite. The exception-based failure categories (Categories 1–7) are unchanged. The PPL result mismatch rate (9.2%) is significantly higher than SQL's (3.3%), primarily due to numeric precision differences in math functions.

---

## Summary

| Metric | Value |
|--------|-------|
| Total Queries | 1593 |
| Success | 1420 (89.1%) |
| Failed (Exception) | 27 (1.7%) |
| Failed (Different Result) | 146 (9.2%) |
| Total Failed | 173 (10.9%) |

---

## Categories 1–7: Exception-Based Failures (27 queries) — UNCHANGED

These categories are identical to the [Mar 24 report](./2026-03-24-ppl-gap-analysis-report.md). Summary:

| # | Category | Queries | Phase |
|---|----------|---------|-------|
| 1 | Special Index Names | 2 | PLAN |
| 2 | Newline in Multi-Line Commands | 2 | PLAN |
| 3 | Unsupported Calcite Features | 3 | PLAN |
| 4 | Unregistered Function (GEOIP) | 1 | PLAN |
| 5 | Prometheus/External Datasource | 5 | PLAN |
| 6 | Index Not Found | 3 | PLAN |
| 7 | Null Statement | 1 | PLAN |

---

## Category 8: Different Query Results — NEW (146 queries)

**Phase:** `RESULT_MISMATCH`
**Root Cause:** Both V2 and Calcite execute the query successfully, but return different data.

### 8a. Numeric Precision — V2 Truncates Decimals (68 queries)

**Diff Pattern:** V2 returns integer-truncated values; Calcite preserves decimal precision.
**Root Cause:** V2's JDBC response truncates floating-point results to integers (e.g., `3.14` → `3`, `2.13` → `2`). Calcite returns full precision.

This is the **dominant** category, affecting nearly all math functions.

| Sub-Group | Count | Sample | V2 | Calcite |
|-----------|-------|--------|----|---------| 
| Trig functions (sin, cos, asin, acos, atan, atan2, cot, sinh, cosh) | 14 | `eval f = sin(1.57)` | `0` | `1.00` |
| Log functions (log, log2, log10, ln) | 6 | `eval f = log2(age)` | `4` | `4.81` |
| Power/root (pow, sqrt, cbrt, exp, expm1) | 8 | `eval f = sqrt(age)` | `5` | `5.29` |
| Constants (pi, e) | 2 | `eval f = pi()` | `3` | `3.14` |
| Rounding (rint, degrees, radians) | 3 | `eval f = degrees(1.57)` | `89` | `89.95` |
| Aggregation (avg, stddev_pop, var_pop, avg with groups) | 20 | `stats avg(age)` | `30` | `30.17` |
| Eval avg/sum (multi-arg) | 13 | `eval f = avg(1, 2)` | `1` | `1.50` |
| rand() | 2 | `eval f = rand()` | `0` | `0.11` |

### 8b. Cast/Type Conversion Precision (12 queries)

**Diff Pattern:** V2 truncates when casting to FLOAT/DOUBLE; Calcite preserves the value.
**Root Cause:** Same underlying issue as 8a — V2 integer-truncates numeric results in JDBC format.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `cast(float_number as DOUBLE)` | `6` | `6.20` |
| `cast(long_number as FLOAT)` | `1` | `1.00` |
| `cast(0.99 as string), cast(12.9 as float)` | `0.99, 12` | `0.99, 12.90` |
| `tonumber('4598.678')` | `4598` | `4598.68` |

### 8c. Geo-Point Format Differences (8 queries)

**Diff Pattern:** V2 returns geo-points as JSON objects; Calcite returns WKT format.
**Root Cause:** Different serialization of `geo_point` type.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `fields point` | `{"lon":74,"lat":40.71}` | `POINT (74 40.71)` |
| Nested geo in map | `{"point":{"lon":-122.33,"lat":47.61}}` | `{point=POINT (-122.33 47.61)}` |

### 8d. Object/Map Serialization Differences (4 queries)

**Diff Pattern:** V2 returns nested objects as JSON strings; Calcite returns Java map `toString()`.
**Root Cause:** Different serialization of object/nested types.

| Sample | V2 | Calcite |
|--------|----|---------| 
| Flattened doc value | `{"json":{"time":100,"status":"SUCCESS"}}` | `{json={status=SUCCESS, time=100}}` |
| Object field sort | Column order differs between V2 and Calcite |

### 8e. Stats with eval — GROUP BY Returns NULL (12 queries)

**Diff Pattern:** V2 returns correct group-by values; Calcite returns `NULL` for group-by columns when `eval` + `stats` are combined.
**Root Cause:** When `eval` creates a new field used in `stats ... by`, Calcite loses the group-by column values.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `eval new_state = lower(state) \| stats count() by gender, new_state \| sort - count()` | `[15, M, OK]` | `[15, NULL, NULL]` |
| `eval new_gender = lower(gender) \| stats sum(balance) by new_gender, new_state` | `[382314, F, RI]` | `[382314, NULL, NULL]` |

### 8f. STDDEV_SAMP/VAR_SAMP — NULL vs 0 for Single-Value Groups (4 queries)

**Diff Pattern:** V2 returns `NULL` for sample variance/stddev of a single value; Calcite returns `0`.
**Root Cause:** Statistical functions on single-value groups: V2 correctly returns NULL (undefined), Calcite returns 0.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `stats STDDEV_SAMP(balance) by age` | `[NULL, 28]` | `[0.00, 36]` |
| `stats VAR_SAMP(balance) by age` | `[NULL, 28]` | `[0.00, 36]` |

### 8g. regexp Return Type — int vs boolean (2 queries)

**Diff Pattern:** V2 returns `0`/`1` (int); Calcite returns `false`/`true` (boolean).
**Root Cause:** Known breaking change documented in `intro-v3-engine.md`.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `eval f=name regexp 'hello'` | `0` | `false` |
| `eval f=name regexp '.*'` | `1` | `true` |

### 8h. top/rare Command — Extra Count Column (5 queries)

**Diff Pattern:** Calcite returns an extra count column that V2 omits.
**Root Cause:** Calcite includes the aggregation count in the output; V2 strips it.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `top 1 state by gender` | `[F, TX]` | `[F, TX, 17]` |
| `rare gender` | `[F]` | `[F, 493]` |

### 8i. rename/eval Column Ordering (5 queries)

**Diff Pattern:** V2 and Calcite return columns in different order after `rename` or `eval`.
**Root Cause:** V2 replaces the column in-place; Calcite appends the new column at the end.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `rename firstname as first` | `[0, 244 Columbus..., ..., Bradshaw]` (firstname removed, first at end) | `[0, Bradshaw, 244 Columbus..., ..., Mckenzie]` (firstname replaced in-place) |
| `eval age=abs(age)` | age stays in original position | age moves to end |

### 8j. Trendline SMA — First Row Behavior (5 queries)

**Diff Pattern:** V2 returns the first value for SMA window=2; Calcite returns NULL.
**Root Cause:** For `sma(2, balance)`, the first row has only 1 data point. V2 returns that value; Calcite returns NULL (insufficient window).

| Sample | V2 Row 0 | Calcite Row 0 |
|--------|----------|---------------|
| `trendline sma(2, balance)` | `39882` | `NULL` |

### 8k. dayname/monthname Locale Differences (4 queries)

**Diff Pattern:** V2 returns English names; Calcite returns localized names (Devanagari script).
**Root Cause:** Calcite uses the JVM's default locale for date name formatting instead of English.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `dayname(date('2020-09-16'))` | `Wednesday` | `बु॒धर` |
| `monthname(date('2020-09-16'))` | `September` | `सप्टेंबर` |

### 8l. str_to_date Returns NULL (1 query)

**Diff Pattern:** V2 parses the date; Calcite returns NULL.
**Root Cause:** Calcite doesn't support the `%b` (abbreviated month name) format specifier.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `str_to_date('1-May-13', '%d-%b-%y')` | `2013-05-01 00:00:00` | `NULL` |

### 8m. Double Field Formatting (8 queries)

**Diff Pattern:** V2 returns `1500` for double fields; Calcite returns `1500.00`.
**Root Cause:** V2 strips trailing `.00` from double values in JDBC format.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `attributes.payment.amount` = 1500.0 | `1500` | `1500.00` |

### 8n. Settings/Query Size Limit (3 queries)

**Diff Pattern:** V2 returns fewer rows; Calcite returns more.
**Root Cause:** V2 respects `plugins.query.size_limit` setting changes during test; Calcite uses the value set at context creation time.

| Sample | V2 | Calcite |
|--------|----|---------| 
| `age>35 \| fields firstname` (after size_limit=1) | 1 row | 3 rows |

### 8o. Trailing Pipe / Stats avg Precision + Sort (2 queries)

**Diff Pattern:** Different sort order and precision in `stats avg() by state | sort state`.
**Root Cause:** Combination of avg precision truncation and different sort behavior.

### 8p. legacy_preferred Setting / Bucket Nullable (1 query)

**Diff Pattern:** V2 returns 5 rows; Calcite returns 6.
**Root Cause:** V2 respects `bucket_nullable` default from `legacy_preferred` setting; Calcite doesn't.

---

## Progress Tracking

| Fix Applied | Before | After | Improvement |
|------------|--------|-------|-------------|
| Baseline (PPL gap analysis) | 1566 / 1593 (98.3%) | — | — |
| + Result comparison (this iteration) | 1566 / 1593 (98.3%) | 1420 / 1593 (89.1%) | −146 queries reclassified |

---

## Updated Priority Matrix

> Items marked with **△** are new from the Mar 24 report.

| Priority | Category | Queries | Fix |
|----------|----------|---------|-----|
| **P0 △** | Numeric precision truncation (8a+8b) | 80 | V2 JDBC formatting issue — Calcite is correct |
| **P0 △** | Stats eval GROUP BY returns NULL (8e) | 12 | Fix Calcite eval+stats column propagation |
| **P1** | Special index names | 2 | Grammar fix |
| **P1 △** | dayname/monthname locale (8k) | 4 | Force English locale in Calcite date functions |
| **P1 △** | Trendline SMA first-row (8j) | 5 | Align SMA window behavior |
| **P1 △** | STDDEV_SAMP/VAR_SAMP NULL vs 0 (8f) | 4 | Fix Calcite to return NULL for single-value groups |
| **P2** | Newline escaping | 2 | Gap analyzer fix |
| **P2** | Unsupported Calcite features | 3 | Medium |
| **P2 △** | Geo-point format (8c) | 8 | Align geo_point serialization |
| **P2 △** | Object/map serialization (8d) | 4 | Align nested object formatting |
| **P2 △** | top/rare extra count column (8h) | 5 | Strip count column or document as intentional |
| **P2 △** | rename/eval column ordering (8i) | 5 | Align column position behavior |
| **P2 △** | regexp return type (8g) | 2 | Known V3 breaking change — document |
| **P2 △** | Double field formatting (8m) | 8 | Align double value formatting |
| **P2 △** | str_to_date %b format (8l) | 1 | Add %b format support |
| **P3** | GEOIP function | 1 | Register function |
| **P3 △** | Settings/size_limit (8n) | 3 | Respect dynamic setting changes |
| **P3 △** | Trailing pipe sort/precision (8o) | 2 | Various |
| **P3 △** | legacy_preferred bucket_nullable (8p) | 1 | Respect setting |
| **N/A** | Prometheus datasource | 5 | External datasource |
| **N/A** | Index not found | 3 | Test data issue |
| **N/A** | Null statement | 1 | Investigation needed |

---

## How to Run

```bash
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.ppl.*IT' -Dunified.gap.analysis=true
```
