# V2 Window Function Documentation Findings

## Source Files Examined

- `docs/user/dql/window.rst` — **primary source** (311 lines)
- `docs/user/dql/aggregations.rst` — GROUP BY aggregations (no window content)
- `docs/user/limitations/limitations.rst` — explicit limitations section
- `docs/user/index.rst` — table of contents (confirms window.rst is the canonical doc)
- `docs/dev/intro-v2-engine.md` — V2 release notes (mentions "ranking and aggregate window functions")
- `docs/dev/sql-aggregate-window-function.md` — dev design doc for aggregate window impl

## 1. Documented Window Functions

### Verbatim from `docs/user/dql/window.rst` (lines 22-25):

> There are three categories of common window functions:
>
> 1. **Aggregate Functions**: COUNT(), MIN(), MAX(), AVG(), SUM(), STDDEV_POP, STDDEV_SAMP, VAR_POP and VAR_SAMP.
> 2. **Ranking Functions**: ROW_NUMBER(), RANK(), DENSE_RANK(), PERCENT_RANK() and NTILE().
> 3. **Analytic Functions**: CUME_DIST(), LAG() and LEAD().

### Functions with worked examples in the doc:
- **Aggregate (9)**: COUNT, MIN, MAX, AVG, SUM, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP
- **Ranking (3)**: ROW_NUMBER, RANK, DENSE_RANK

### Functions listed but NO examples or dedicated section:
- PERCENT_RANK — mentioned in overview only
- NTILE — mentioned in overview only
- CUME_DIST — mentioned in overview only
- LAG — mentioned in overview only
- LEAD — mentioned in overview only

## 2. Documented Features

### PARTITION BY — YES, documented and used in examples
Verbatim syntax (lines 30-35):
```
function_name (expression [, expression...])
OVER (
  PARTITION BY expression [, expression...]
  ORDER BY expression [ASC | DESC] [NULLS {FIRST | LAST}] [, ...]
)
```

### ORDER BY inside OVER(...) — YES, documented
- Includes ASC/DESC and NULLS FIRST/LAST support
- Verbatim: "you can specify null ordering by ``NULLS FIRST`` or ``NULLS LAST`` which has exactly same behavior"

### DISTINCT in window aggregate — NOT documented
- No mention of COUNT(DISTINCT x) OVER(...) anywhere in window.rst
- DISTINCT COUNT is documented only in aggregations.rst for GROUP BY context

### Window frame (ROWS BETWEEN / RANGE BETWEEN) — NOT documented
- The syntax section shows NO frame clause
- The word "frame" appears only in conceptual description: "A window definition defines a window of data rows with a given scope around the current row. The window is also called window frame sometimes."
- No ROWS BETWEEN or RANGE BETWEEN syntax is shown anywhere in user docs
- The dev doc (`sql-aggregate-window-function.md`) discusses "cumulative window frame" as an implementation detail but this is NOT exposed to users as configurable syntax

### Named window (WINDOW clause) — NOT documented
- No mention of WINDOW clause anywhere in user docs

## 3. Explicit Limitations

### From `docs/user/limitations/limitations.rst` — "Limitations on Window Functions" section:

> For now, only the field defined in index is allowed, all the other calculated fields (calculated by scalar or aggregated functions) is not allowed. For example, either ``avg_flight_time`` or ``AVG(FlightTimeMin)`` is not accessible to the rank window definition as follows:
>
> ```sql
> SELECT OriginCountry, AVG(FlightTimeMin) AS avg_flight_time,
>        RANK() OVER (ORDER BY avg_flight_time) AS rnk
> FROM opensearch_dashboards_sample_data_flights
> GROUP BY OriginCountry
> ```
>
> Another limitation is that currently window function cannot be nested in another expression, for example, ``CASE WHEN RANK() OVER(...) THEN ...``.
>
> Workaround for both limitations mentioned above is using a sub-query in FROM clause:
>
> ```sql
> SELECT
>   SUM(t.avg_flight_time) OVER(...)
> FROM (
>     SELECT OriginCountry, AVG(FlightTimeMin) AS avg_flight_time,
>     FROM opensearch_dashboards_sample_data_flights
>     GROUP BY OriginCountry
> ) AS t
> ```

### Implicit limitations (not stated but absent from docs):
- No window frame specification (ROWS/RANGE BETWEEN)
- No named windows (WINDOW clause)
- No DISTINCT in window aggregates
- PERCENT_RANK, NTILE, CUME_DIST, LAG, LEAD listed but not demonstrated

## 4. Representative Examples from the Manual

### Example 1: Aggregate window (COUNT with PARTITION BY + ORDER BY)
```sql
SELECT
  gender, balance,
  COUNT(balance) OVER(
    PARTITION BY gender ORDER BY balance
  ) AS cnt
FROM accounts;
```

### Example 2: Ranking window (ROW_NUMBER with NULLS LAST)
```sql
SELECT
  employer,
  ROW_NUMBER() OVER(
    ORDER BY employer NULLS LAST
  ) AS num
FROM accounts
ORDER BY employer NULLS LAST;
```

### Example 3: RANK without PARTITION BY
```sql
SELECT gender, RANK() OVER(ORDER BY gender DESC) AS rnk FROM accounts;
```

## 5. Summary

The V2 user manual documents **14 window functions** across 3 categories but only provides worked examples for **12** (9 aggregate + 3 ranking). The 5 analytic/ranking functions (PERCENT_RANK, NTILE, CUME_DIST, LAG, LEAD) are listed in the overview but have no examples or dedicated documentation sections.

**Supported features per docs:**
- PARTITION BY ✓
- ORDER BY (with ASC/DESC, NULLS FIRST/LAST) ✓

**Not documented / implicitly unsupported:**
- Window frame (ROWS BETWEEN / RANGE BETWEEN) ✗
- Named windows (WINDOW clause) ✗
- DISTINCT in window aggregates ✗

**Explicit limitations:**
- Cannot use calculated/aggregated fields in window ORDER BY (must use subquery workaround)
- Cannot nest window functions in expressions (e.g., CASE WHEN RANK() OVER(...))
