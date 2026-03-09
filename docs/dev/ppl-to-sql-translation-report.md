# PPL-to-SQL Translation Report

Complete mapping of **365 PPL examples** from **44 commands** in `docs/user/ppl/cmd/`.

## Summary Table

| # | Command | Examples | SQL Feature Needed | Doc |
|---|---------|----------|-------------------|-----|
| 1 | **addcoltotals** | 3 | TVF | [addcoltotals.md](../../user/ppl/cmd/addcoltotals.md) |
| 2 | **addtotals** | 4 | Standard / TVF | [addtotals.md](../../user/ppl/cmd/addtotals.md) |
| 3 | **append** | 2 | Standard (UNION ALL) | [append.md](../../user/ppl/cmd/append.md) |
| 4 | **appendcol** | 4 | Standard (Window + Join) | [appendcol.md](../../user/ppl/cmd/appendcol.md) |
| 5 | **appendpipe** | 2 | Standard (CTE + UNION ALL) | [appendpipe.md](../../user/ppl/cmd/appendpipe.md) |
| 6 | **bin** | 21 | Standard + UDF | [bin.md](../../user/ppl/cmd/bin.md) |
| 7 | **chart** | 7 | Standard / TVF (PIVOT) | [chart.md](../../user/ppl/cmd/chart.md) |
| 8 | **dedup** | 5 | Standard (Window) + EXCEPT | [dedup.md](../../user/ppl/cmd/dedup.md) |
| 9 | **eval** | 5 | Standard / REPLACE | [eval.md](../../user/ppl/cmd/eval.md) |
| 10 | **eventstats** | 10 | Standard (Window) | [eventstats.md](../../user/ppl/cmd/eventstats.md) |
| 11 | **expand** | 1 | EXCEPT + UNNEST | [expand.md](../../user/ppl/cmd/expand.md) |
| 12 | **fieldformat** | 4 | REPLACE | [fieldformat.md](../../user/ppl/cmd/fieldformat.md) |
| 13 | **fields** | 10 | Standard / EXCEPT | [fields.md](../../user/ppl/cmd/fields.md) |
| 14 | **fillnull** | 7 | REPLACE / TVF | [fillnull.md](../../user/ppl/cmd/fillnull.md) |
| 15 | **flatten** | 3 | EXCEPT | [flatten.md](../../user/ppl/cmd/flatten.md) |
| 16 | **graphlookup** | 11 | Standard (Recursive CTE) | [graphlookup.md](../../user/ppl/cmd/graphlookup.md) |
| 17 | **grok** | 3 | Standard + UDF | [grok.md](../../user/ppl/cmd/grok.md) |
| 18 | **head** | 3 | Standard | [head.md](../../user/ppl/cmd/head.md) |
| 19 | **join** | 30 | Standard | [join.md](../../user/ppl/cmd/join.md) |
| 20 | **lookup** | 14 | Standard (LEFT JOIN) | [lookup.md](../../user/ppl/cmd/lookup.md) |
| 21 | **multisearch** | 7 | Standard (UNION ALL) | [multisearch.md](../../user/ppl/cmd/multisearch.md) |
| 22 | **mvcombine** | 4 | REPLACE + GROUP BY EXCEPT | [mvcombine.md](../../user/ppl/cmd/mvcombine.md) |
| 23 | **mvexpand** | 5 | EXCEPT + UNNEST | [mvexpand.md](../../user/ppl/cmd/mvexpand.md) |
| 24 | **nomv** | 2 | REPLACE | [nomv.md](../../user/ppl/cmd/nomv.md) |
| 25 | **parse** | 7 | Standard | [parse.md](../../user/ppl/cmd/parse.md) |
| 26 | **patterns** | 9 | TVF | [patterns.md](../../user/ppl/cmd/patterns.md) |
| 27 | **rare** | 6 | Standard | [rare.md](../../user/ppl/cmd/rare.md) |
| 28 | **regex** | 6 | Standard | [regex.md](../../user/ppl/cmd/regex.md) |
| 29 | **rename** | 5 | REPLACE | [rename.md](../../user/ppl/cmd/rename.md) |
| 30 | **replace** | 12 | REPLACE | [replace.md](../../user/ppl/cmd/replace.md) |
| 31 | **reverse** | 5 | Standard (Window) | [reverse.md](../../user/ppl/cmd/reverse.md) |
| 32 | **rex** | 11 | Standard / REPLACE | [rex.md](../../user/ppl/cmd/rex.md) |
| 33 | **search** | 1 | Standard | [search.md](../../user/ppl/cmd/search.md) |
| 34 | **sort** | 8 | Standard | [sort.md](../../user/ppl/cmd/sort.md) |
| 35 | **spath** | 5 | Standard / TVF | [spath.md](../../user/ppl/cmd/spath.md) |
| 36 | **stats** | 23 | Standard | [stats.md](../../user/ppl/cmd/stats.md) |
| 37 | **streamstats** | 19 | Standard (Window) / TVF | [streamstats.md](../../user/ppl/cmd/streamstats.md) |
| 38 | **subquery** | 45 | Standard | [subquery.md](../../user/ppl/cmd/subquery.md) |
| 39 | **table** | 1 | Standard | [table.md](../../user/ppl/cmd/table.md) |
| 40 | **timechart** | 12 | Standard / TVF (PIVOT) | [timechart.md](../../user/ppl/cmd/timechart.md) |
| 41 | **top** | 7 | Standard | [top.md](../../user/ppl/cmd/top.md) |
| 42 | **transpose** | 3 | TVF | [transpose.md](../../user/ppl/cmd/transpose.md) |
| 43 | **trendline** | 4 | Standard (Window) | [trendline.md](../../user/ppl/cmd/trendline.md) |
| 44 | **where** | 9 | Standard | [where.md](../../user/ppl/cmd/where.md) |
| | **Total** | **365** | | |

### SQL Feature Requirements

| SQL Feature | Example Count |
|-------------|--------------|
| Standard | 146 |
| Standard + UDF | 24 |
| REPLACE | 23 |
| Standard / TVF (PIVOT) | 19 |
| Standard (Window) | 19 |
| Standard (Window) / TVF | 19 |
| Standard / REPLACE | 16 |
| TVF | 15 |
| Standard (LEFT JOIN) | 14 |
| Standard (Recursive CTE) | 11 |
| Standard / EXCEPT | 10 |
| Standard / TVF | 9 |
| Standard (UNION ALL) | 9 |
| REPLACE / TVF | 7 |
| EXCEPT + UNNEST | 6 |
| Standard (Window) + EXCEPT | 5 |
| Standard (Window + Join) | 4 |
| REPLACE + GROUP BY EXCEPT | 4 |
| EXCEPT | 3 |
| Standard (CTE + UNION ALL) | 2 |

### TVF vs SQL Dialect Analysis

Out of 131 command parameter variants analyzed across all 44 commands, **91% translate to standard SQL** (with EXCEPT/REPLACE/Window/CTE). Only 8 specific variants truly require TVF — no SQL database dialect offers an alternative.

**Truly needs TVF (8 variants) — no SQL alternative in any dialect:**

| Command Variant | Why | Checked Against |
|---|---|---|
| `fillnull` (all columns, no field list) | No dialect has "COALESCE all columns" | BigQuery, Spark, Snowflake, PostgreSQL, Oracle, MySQL |
| `addtotals` (no explicit fields) | No dialect has "sum all numeric columns" | All major dialects |
| `addtotals` (col=true) | Append summary row needs schema discovery | ROLLUP helps for grouped data only |
| `addcoltotals` | Append summary row with column totals | Same as addtotals col=true |
| `patterns` (all modes) | Log pattern extraction — no SQL equivalent | Spark UDAFs closest, but not SQL |
| `transpose` | Fully dynamic row-to-column | Oracle/SQL Server UNPIVOT needs known columns |
| `spath` (auto-extract, no path) | Expand all JSON keys to columns | PostgreSQL json_each returns rows not columns |
| `rename` (wildcard: `*name` as `*_name`) | Discover columns matching glob pattern | No dialect supports wildcard rename |

**Eliminated by database dialect features (3 variants):**

| Command Variant | SQL Feature | Available In |
|---|---|---|
| `chart` (2-field PIVOT) | `PIVOT *` (discover values at runtime) | Spark SQL 3.4+ |
| `timechart` (with by-clause) | `PIVOT *` | Spark SQL 3.4+ |
| `streamstats` (reset_before/reset_after) | `MATCH_RECOGNIZE` | Oracle, **Calcite 1.41.0** (already supported) |

**Borderline (1 variant):**

| Command Variant | Notes |
|---|---|
| `fields` (wildcard: `account*`, `*name`) | Calcite SqlValidator may resolve `*` patterns during validation phase |

**Key SQL features needed from dialects:**

| Feature | Source Dialect | Calcite Support | Eliminates |
|---|---|---|---|
| `SELECT * EXCEPT (cols)` | BigQuery, Spark | Needs parser config | fields-, dedup _rn removal, flatten, expand |
| `SELECT * REPLACE (expr AS col)` | BigQuery | Needs parser config | rename, fillnull, replace, fieldformat, nomv, eval override |
| `GROUP BY * EXCEPT (col)` | BigQuery | Needs parser config | mvcombine |
| `PIVOT *` | Spark 3.4+ | Not yet | chart/timechart with column split |
| `MATCH_RECOGNIZE` | Oracle | **Already in Calcite** | streamstats reset |
| `WITH RECURSIVE` | PostgreSQL, all modern | **Already in Calcite** | graphlookup |
| `UNNEST` / `CROSS JOIN UNNEST` | BigQuery, Spark, Trino | **Already in Calcite** | expand, mvexpand |

---

# Appendix: Per-Command Translation Details

## addcoltotals

**Reference:** [docs/user/ppl/cmd/addcoltotals.md](../../user/ppl/cmd/addcoltotals.md)
**SQL Feature:** TVF

### Example 1

**PPL:**
```ppl
source=accounts | fields firstname, balance | head 3 | addcoltotals labelfield='firstname'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_ADDCOLTOTALS(TABLE(<source>), ...)
```

### Example 2

**PPL:**
```ppl
source=accounts | stats count() by gender | addcoltotals `count()` label='Sum' labelfield='Total'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_ADDCOLTOTALS(TABLE(<source>), ...)
```

### Example 3

**PPL:**
```ppl
source=accounts | where age > 30 | stats avg(balance) as avg_balance, count() as count by state | head 3 | addcoltotals avg_balance, count label='Sum' labelfield='Column Total'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_ADDCOLTOTALS(TABLE(<source>), ...)
```

---

## addtotals

**Reference:** [docs/user/ppl/cmd/addtotals.md](../../user/ppl/cmd/addtotals.md)
**SQL Feature:** Standard / TVF

### Example 1

**PPL:**
```ppl
source=accounts | fields account_number, firstname , balance , age | addtotals col=true row=false label='Sum' labelfield='Total'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_ADDTOTALS(TABLE(<source>), ...)
```

### Example 2

**PPL:**
```ppl
source=accounts | head 3 | fields firstname, balance | addtotals col=true labelfield='firstname' label='Total'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_ADDTOTALS(TABLE(<source>), ...)
```

### Example 3

**PPL:**
```ppl
source=accounts | fields account_number, firstname , balance , age | addtotals col=true row=true label='Sum' labelfield='Total'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_ADDTOTALS(TABLE(<source>), ...)
```

### Example 4

**PPL:**
```ppl
source=accounts | where age > 30 | stats avg(balance) as avg_balance, count() as count by state | head 3 | addtotals avg_balance, count row=true col=true fieldname='Row Total' label='Sum' labelfield='Column Total'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_ADDTOTALS(TABLE(<source>), ...)
```

---

## append

**Reference:** [docs/user/ppl/cmd/append.md](../../user/ppl/cmd/append.md)
**SQL Feature:** Standard (UNION ALL)

### Example 1

**PPL:**
```ppl
source=accounts | stats sum(age) by gender, state | sort -`sum(age)` | head 5 | append [ source=accounts | stats count(age) by gender ]
```

**SQL:**
```sql
<main_query>
UNION ALL
<subsearch_query>
```

### Example 2

**PPL:**
```ppl
source=accounts | stats sum(age) as sum by gender, state | sort -sum | head 5 | append [ source=accounts | stats sum(age) as sum by gender ]
```

**SQL:**
```sql
<main_query>
UNION ALL
<subsearch_query>
```

---

## appendcol

**Reference:** [docs/user/ppl/cmd/appendcol.md](../../user/ppl/cmd/appendcol.md)
**SQL Feature:** Standard (Window + Join)

### Example 1

**PPL:**
```ppl
source=accounts | stats sum(age) by gender, state | appendcol [ stats count(age) by gender ] | head 10
```

**SQL:**
```sql
SELECT a.*, b.*
FROM (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <main>) a
LEFT JOIN (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <subsearch>) b
ON a._rn = b._rn
```

### Example 2

**PPL:**
```ppl
source=accounts | stats sum(age) by gender, state | appendcol override=true [ stats count(age) by gender ] | head 10
```

**SQL:**
```sql
SELECT a.*, b.*
FROM (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <main>) a
LEFT JOIN (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <subsearch>) b
ON a._rn = b._rn
```

### Example 3

**PPL:**
```ppl
source=employees | fields name, dept, age | appendcol [ stats avg(age) as avg_age ] | appendcol [ stats max(age) as max_age ]
```

**SQL:**
```sql
SELECT a.*, b.*
FROM (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <main>) a
LEFT JOIN (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <subsearch>) b
ON a._rn = b._rn
```

### Example 4

**PPL:**
```ppl
source=employees | stats avg(age) as agg by dept | appendcol override=true [ stats max(age) as agg by dept ]
```

**SQL:**
```sql
SELECT a.*, b.*
FROM (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <main>) a
LEFT JOIN (SELECT *, ROW_NUMBER() OVER () AS _rn FROM <subsearch>) b
ON a._rn = b._rn
```

---

## appendpipe

**Reference:** [docs/user/ppl/cmd/appendpipe.md](../../user/ppl/cmd/appendpipe.md)
**SQL Feature:** Standard (CTE + UNION ALL)

### Example 1

**PPL:**
```ppl
source=accounts | stats sum(age) as part by gender, state | sort -part | head 5 | appendpipe [ stats sum(part) as total by gender ]
```

**SQL:**
```sql
WITH base AS (<main_query>)
SELECT * FROM base
UNION ALL
<subpipeline applied to base>
```

### Example 2

**PPL:**
```ppl
source=accounts | stats sum(age) as total by gender, state | sort -total | head 5 | appendpipe [ stats sum(total) as total by gender ]
```

**SQL:**
```sql
WITH base AS (<main_query>)
SELECT * FROM base
UNION ALL
<subpipeline applied to base>
```

---

## bin

**Reference:** [docs/user/ppl/cmd/bin.md](../../user/ppl/cmd/bin.md)
**SQL Feature:** Standard + UDF

### Example 1

**PPL:**
```ppl
source=accounts | bin age span=10 | fields age, account_number | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | bin balance span=25000 | fields balance | head 2
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | bin balance span=log10 | fields balance | head 2
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=accounts | bin balance span=2log10 | fields balance | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 5

**PPL:**
```ppl
source=time_test | bin value bins=5 | fields value | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 6

**PPL:**
```ppl
source=accounts | bin age bins=2 | fields age | head 1
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 7

**PPL:**
```ppl
source=accounts | bin age bins=21 | fields age, account_number | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 8

**PPL:**
```ppl
source=accounts | bin age minspan=5 | fields age, account_number | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 9

**PPL:**
```ppl
source=accounts | bin age minspan=101 | fields age | head 1
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 10

**PPL:**
```ppl
source=accounts | bin age start=0 end=101 | fields age | head 1
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 11

**PPL:**
```ppl
source=accounts | bin balance start=0 end=100001 | fields balance | head 1
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 12

**PPL:**
```ppl
source=accounts | bin age span=1 start=25 end=35 | fields age | head 6
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 13

**PPL:**
```ppl
source=time_test | bin @timestamp span=1h | fields @timestamp, value | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 14

**PPL:**
```ppl
source=time_test | bin @timestamp span=45minute | fields @timestamp, value | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 15

**PPL:**
```ppl
source=time_test | bin @timestamp span=30seconds | fields @timestamp, value | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 16

**PPL:**
```ppl
source=time_test | bin @timestamp span=7day | fields @timestamp, value | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 17

**PPL:**
```ppl
source=time_test | bin @timestamp span=2h aligntime='@d+3h' | fields @timestamp, value | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 18

**PPL:**
```ppl
source=time_test | bin @timestamp span=2h aligntime=1500000000 | fields @timestamp, value | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 19

**PPL:**
```ppl
source=accounts | bin age | fields age, account_number | head 3
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 20

**PPL:**
```ppl
source=accounts | eval age_str = CAST(age AS STRING) | bin age_str bins=3 | stats count() by age_str | sort age_str
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

### Example 21

**PPL:**
```ppl
source=events | bin @timestamp bins=3 | stats count() by @timestamp
```

**SQL:**
```sql
SELECT *, SPAN_BUCKET(<field>, <span>) AS <field>
FROM <source>
```

---

## chart

**Reference:** [docs/user/ppl/cmd/chart.md](../../user/ppl/cmd/chart.md)
**SQL Feature:** Standard / TVF (PIVOT)

### Example 1

**PPL:**
```ppl
source=accounts | chart avg(balance)
```

**SQL:**
```sql
SELECT <agg>, <group>
FROM <source>
GROUP BY <group>
```

### Example 2

**PPL:**
```ppl
source=accounts | chart count() by gender
```

**SQL:**
```sql
SELECT <agg>, <group>
FROM <source>
GROUP BY <group>
```

### Example 3

**PPL:**
```ppl
source=accounts | chart avg(balance) over gender by age
```

**SQL:**
```sql
TVF (PIVOT): dynamic columns from column-split field
```

### Example 4

**PPL:**
```ppl
source=accounts | chart limit=1 count() over gender by age
```

**SQL:**
```sql
TVF (PIVOT): dynamic columns from column-split field
```

### Example 5

**PPL:**
```ppl
source=accounts | chart limit=top1 useother=true otherstr='minor_gender' count() over state by gender
```

**SQL:**
```sql
TVF (PIVOT): dynamic columns from column-split field
```

### Example 6

**PPL:**
```ppl
source=accounts | chart usenull=true nullstr='employer not specified' count() over firstname by employer
```

**SQL:**
```sql
TVF (PIVOT): dynamic columns from column-split field
```

### Example 7

**PPL:**
```ppl
source=accounts | chart max(balance) by age span=10, gender
```

**SQL:**
```sql
TVF (PIVOT): dynamic columns from column-split field
```

---

## dedup

**Reference:** [docs/user/ppl/cmd/dedup.md](../../user/ppl/cmd/dedup.md)
**SQL Feature:** Standard (Window) + EXCEPT

### Example 1

**PPL:**
```ppl
source=accounts | dedup gender | fields account_number, gender | sort account_number
```

**SQL:**
```sql
SELECT * EXCEPT (_rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY <fields> ORDER BY <fields>) AS _rn
  FROM <source>
)
WHERE _rn <= <N>
```

### Example 2

**PPL:**
```ppl
source=accounts | dedup 2 gender | fields account_number, gender | sort account_number
```

**SQL:**
```sql
SELECT * EXCEPT (_rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY <fields> ORDER BY <fields>) AS _rn
  FROM <source>
)
WHERE _rn <= <N>
```

### Example 3

**PPL:**
```ppl
source=accounts | dedup email keepempty=true | fields account_number, email | sort account_number
```

**SQL:**
```sql
SELECT * EXCEPT (_rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY <fields> ORDER BY <fields>) AS _rn
  FROM <source>
)
WHERE _rn <= <N>
```

### Example 4

**PPL:**
```ppl
source=accounts | dedup email | fields account_number, email | sort account_number
```

**SQL:**
```sql
SELECT * EXCEPT (_rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY <fields> ORDER BY <fields>) AS _rn
  FROM <source>
)
WHERE _rn <= <N>
```

### Example 5

**PPL:**
```ppl
source=accounts | dedup gender consecutive=true | fields account_number, gender | sort account_number
```

**SQL:**
```sql
SELECT * EXCEPT (_rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY <fields> ORDER BY <fields>) AS _rn
  FROM <source>
)
WHERE _rn <= <N>
```

---

## eval

**Reference:** [docs/user/ppl/cmd/eval.md](../../user/ppl/cmd/eval.md)
**SQL Feature:** Standard / REPLACE

### Example 1

**PPL:**
```ppl
source=accounts | eval doubleAge = age * 2 | fields age, doubleAge
```

**SQL:**
```sql
-- Pipe chain: each eval wraps as subquery
-- source=accounts | eval doubleAge = age * 2 | fields age, doubleAge
SELECT *, <expr> AS <alias>
FROM <source>
-- Then wrap as subquery for downstream visibility
```

### Example 2

**PPL:**
```ppl
source=accounts | eval age = age + 1 | fields age
```

**SQL:**
```sql
-- Pipe chain: each eval wraps as subquery
-- source=accounts | eval age = age + 1 | fields age
SELECT *, <expr> AS <alias>
FROM <source>
-- Then wrap as subquery for downstream visibility
```

### Example 3

**PPL:**
```ppl
source=accounts | eval doubleAge = age * 2, ddAge = doubleAge * 2 | fields age, doubleAge, ddAge
```

**SQL:**
```sql
-- Pipe chain: each eval wraps as subquery
-- source=accounts | eval doubleAge = age * 2, ddAge = doubleAge * 2 | fields age, doubleAge, ddAge
SELECT *, <expr> AS <alias>
FROM <source>
-- Then wrap as subquery for downstream visibility
```

### Example 4

**PPL:**
```ppl
source=accounts | eval greeting = 'Hello ' + firstname | fields firstname, greeting
```

**SQL:**
```sql
-- Pipe chain: each eval wraps as subquery
-- source=accounts | eval greeting = 'Hello ' + firstname | fields firstname, greeting
SELECT *, <expr> AS <alias>
FROM <source>
-- Then wrap as subquery for downstream visibility
```

### Example 5

**PPL:**
```ppl
source=accounts | eval full_info = 'Name: ' + firstname + ', Age: ' + CAST(age AS STRING) | fields firstname, age, full_info
```

**SQL:**
```sql
-- Pipe chain: each eval wraps as subquery
-- source=accounts | eval full_info = 'Name: ' + firstname + ', Age: ' + CAST(age AS STRING) | fields firstname, age, full_info
SELECT *, <expr> AS <alias>
FROM <source>
-- Then wrap as subquery for downstream visibility
```

---

## eventstats

**Reference:** [docs/user/ppl/cmd/eventstats.md](../../user/ppl/cmd/eventstats.md)
**SQL Feature:** Standard (Window)

### Example 1

**PPL:**
```ppl
source = table | eventstats avg(a)
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source = table | where a < 50 | eventstats count(c)
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source = table | eventstats min(c), max(c) by b
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 4

**PPL:**
```ppl
source = table | eventstats count(c) as count_by by b | where count_by > 1000
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 5

**PPL:**
```ppl
source = table | eventstats dc(field) as distinct_count
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 6

**PPL:**
```ppl
source = table | eventstats distinct_count(category) by region
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 7

**PPL:**
```ppl
source=accounts | fields account_number, gender, age | eventstats avg(age), sum(age), count() by gender | sort account_number
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 8

**PPL:**
```ppl
source=accounts | fields account_number, gender, age | eventstats count() as cnt by span(age, 5) as age_span, gender | sort account_number
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 9

**PPL:**
```ppl
source=accounts | eventstats bucket_nullable=false count() as cnt by employer | fields account_number, firstname, employer, cnt | sort account_number
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

### Example 10

**PPL:**
```ppl
source=accounts | eventstats bucket_nullable=true count() as cnt by employer | fields account_number, firstname, employer, cnt | sort account_number
```

**SQL:**
```sql
SELECT *, <agg> OVER (PARTITION BY <groups>) AS <alias>
FROM <source>
```

---

## expand

**Reference:** [docs/user/ppl/cmd/expand.md](../../user/ppl/cmd/expand.md)
**SQL Feature:** EXCEPT + UNNEST

### Example 1

**PPL:**
```ppl
source=migration | expand address as addr
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), t.val AS <alias>
FROM <source>
CROSS JOIN UNNEST(<field>) AS t(val)
```

---

## fieldformat

**Reference:** [docs/user/ppl/cmd/fieldformat.md](../../user/ppl/cmd/fieldformat.md)
**SQL Feature:** REPLACE

### Example 1

**PPL:**
```ppl
source=accounts | fieldformat doubleAge = age * 2 | fields age, doubleAge
```

**SQL:**
```sql
SELECT * REPLACE (<expr> AS <field>)
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | fieldformat age = age + 1 | fields age
```

**SQL:**
```sql
SELECT * REPLACE (<expr> AS <field>)
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | fieldformat greeting = 'Hello '.tostring( firstname) | fields firstname, greeting
```

**SQL:**
```sql
SELECT * REPLACE (<expr> AS <field>)
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=accounts | fieldformat age_info = 'Age: '.CAST(age AS STRING).' years.' | fields firstname, age, age_info
```

**SQL:**
```sql
SELECT * REPLACE (<expr> AS <field>)
FROM <source>
```

---

## fields

**Reference:** [docs/user/ppl/cmd/fields.md](../../user/ppl/cmd/fields.md)
**SQL Feature:** Standard / EXCEPT

### Example 1

**PPL:**
```ppl
source=accounts | fields account_number, firstname, lastname
```

**SQL:**
```sql
SELECT account_number, firstname, lastname
FROM accounts
```

### Example 2

**PPL:**
```ppl
source=accounts | fields account_number, firstname, lastname | fields - account_number
```

**SQL:**
```sql
SELECT * EXCEPT (account_number)
FROM accounts
```

### Example 3

**PPL:**
```ppl
source=accounts | fields firstname lastname age
```

**SQL:**
```sql
SELECT firstname lastname age
FROM accounts
```

### Example 4

**PPL:**
```ppl
source=accounts | fields account*
```

**SQL:**
```sql
SELECT account*
FROM accounts
```

### Example 5

**PPL:**
```ppl
source=accounts | fields *name
```

**SQL:**
```sql
SELECT *name
FROM accounts
```

### Example 6

**PPL:**
```ppl
source=accounts | fields *a* | head 1
```

**SQL:**
```sql
SELECT *a*
FROM accounts
```

### Example 7

**PPL:**
```ppl
source=accounts | fields firstname, account* *name
```

**SQL:**
```sql
SELECT firstname, account* *name
FROM accounts
```

### Example 8

**PPL:**
```ppl
source=accounts | fields firstname, *name
```

**SQL:**
```sql
SELECT firstname, *name
FROM accounts
```

### Example 9

**PPL:**
```ppl
source=accounts | fields `*` | head 1
```

**SQL:**
```sql
SELECT `*`
FROM accounts
```

### Example 10

**PPL:**
```ppl
source=accounts | fields - *name
```

**SQL:**
```sql
SELECT * EXCEPT (*name)
FROM accounts
```

---

## fillnull

**Reference:** [docs/user/ppl/cmd/fillnull.md](../../user/ppl/cmd/fillnull.md)
**SQL Feature:** REPLACE / TVF

### Example 1

**PPL:**
```ppl
source=accounts | fields email, employer | fillnull with '<not found>' in email
```

**SQL:**
```sql
SELECT * REPLACE (COALESCE(<field>, <value>) AS <field>)
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | fields email, employer | fillnull with '<not found>' in email, employer
```

**SQL:**
```sql
SELECT * REPLACE (COALESCE(<field>, <value>) AS <field>)
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | fields email, employer | fillnull with '<not found>'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_FILLNULL(TABLE(<source>), <value>)
```

### Example 4

**PPL:**
```ppl
source=accounts | fields email, employer | fillnull using email = '<not found>', employer = '<no employer>'
```

**SQL:**
```sql
SELECT * REPLACE (COALESCE(<field>, <value>) AS <field>)
FROM <source>
```

### Example 5

**PPL:**
```ppl
source=accounts | fields email, employer | fillnull value="<not found>" email employer
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_FILLNULL(TABLE(<source>), <value>)
```

### Example 6

**PPL:**
```ppl
source=accounts | fields email, employer | fillnull value='<not found>'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_FILLNULL(TABLE(<source>), <value>)
```

### Example 7

**PPL:**
```ppl
source=accounts | fillnull value=0 firstname, age
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_FILLNULL(TABLE(<source>), <value>)
```

---

## flatten

**Reference:** [docs/user/ppl/cmd/flatten.md](../../user/ppl/cmd/flatten.md)
**SQL Feature:** EXCEPT

### Example 1

**PPL:**
```ppl
source=my-index | flatten message as (creator, dow, info)
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), <field>.*
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=my-index | fields message | flatten message
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), <field>.*
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=my-index | flatten message
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), <field>.*
FROM <source>
```

---

## graphlookup

**Reference:** [docs/user/ppl/cmd/graphlookup.md](../../user/ppl/cmd/graphlookup.md)
**SQL Feature:** Standard (Recursive CTE)

### Example 1

**PPL:**
```ppl
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name as reportingHierarchy
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 2

**PPL:**
```ppl
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name maxDepth=2 as reportingHierarchy
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 3

**PPL:**
```ppl
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name depthField=level as reportingHierarchy
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 4

**PPL:**
```ppl
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name direction=bi as connections
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 5

**PPL:**
```ppl
source = travelers | graphLookup airports startField=nearestAirport fromField=connects toField=airport supportArray=true as reachableAirports
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 6

**PPL:**
```ppl
source = airports | graphLookup airports startField=airport fromField=connects toField=airport supportArray=true as reachableAirports
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 7

**PPL:**
```ppl
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name filter=(status = 'active' AND age > 18) as reportingHierarchy
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 8

**PPL:**
```ppl
source = employees | graphLookup employees
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 9

**PPL:**
```ppl
source = airports | graphLookup airports
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 10

**PPL:**
```ppl
source = travelers | graphLookup airports
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

### Example 11

**PPL:**
```ppl
source = employees | where name = 'Ron' | graphLookup employees
```

**SQL:**
```sql
WITH RECURSIVE traversal AS (
  SELECT s.*, l.<toField>, 0 AS depth
  FROM <source> s JOIN <lookupIndex> l ON s.<startField> = l.<toField>
  UNION ALL
  SELECT t.*, l.<toField>, t.depth + 1
  FROM traversal t JOIN <lookupIndex> l ON t.<fromField> = l.<toField>
  WHERE t.depth < <maxDepth>
)
SELECT * FROM traversal
```

---

## grok

**Reference:** [docs/user/ppl/cmd/grok.md](../../user/ppl/cmd/grok.md)
**SQL Feature:** Standard + UDF

### Example 1

**PPL:**
```ppl
source=accounts | grok email '.+@%{HOSTNAME:host}' | fields email, host
```

**SQL:**
```sql
SELECT *, GROK_EXTRACT(<field>, '<pattern>', '<group>') AS <group>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | grok address '%{NUMBER} %{GREEDYDATA:address}' | fields address
```

**SQL:**
```sql
SELECT *, GROK_EXTRACT(<field>, '<pattern>', '<group>') AS <group>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=apache | grok message '%{COMMONAPACHELOG}' | fields COMMONAPACHELOG, timestamp, response, bytes
```

**SQL:**
```sql
SELECT *, GROK_EXTRACT(<field>, '<pattern>', '<group>') AS <group>
FROM <source>
```

---

## head

**Reference:** [docs/user/ppl/cmd/head.md](../../user/ppl/cmd/head.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | fields firstname, age | head
```

**SQL:**
```sql
SELECT *
FROM accounts
LIMIT 10
```

### Example 2

**PPL:**
```ppl
source=accounts | fields firstname, age | head 3
```

**SQL:**
```sql
SELECT *
FROM accounts
LIMIT 3
```

### Example 3

**PPL:**
```ppl
source=accounts | fields firstname, age | head 3 from 1
```

**SQL:**
```sql
SELECT *
FROM accounts
LIMIT 3
OFFSET 1
```

---

## join

**Reference:** [docs/user/ppl/cmd/join.md](../../user/ppl/cmd/join.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source = table1 | inner join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 2

**PPL:**
```ppl
source = table1 | inner join left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 3

**PPL:**
```ppl
source = table1 | left join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 4

**PPL:**
```ppl
source = table1 | right join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 5

**PPL:**
```ppl
source = table1 | full left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 6

**PPL:**
```ppl
source = table1 | cross join left = l right = r on 1=1 table2
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 7

**PPL:**
```ppl
source = table1 | left semi join left = l right = r on l.a = r.a table2
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 8

**PPL:**
```ppl
source = table1 | left anti join left = l right = r on l.a = r.a table2
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 9

**PPL:**
```ppl
source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ]
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 10

**PPL:**
```ppl
source = table1 | inner join on table1.a = table2.a table2 | fields table1.a, table2.a, table1.b, table1.c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 11

**PPL:**
```ppl
source = table1 | inner join on a = c table2 | fields a, b, c, d
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 12

**PPL:**
```ppl
source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields l.a, r.a
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 13

**PPL:**
```ppl
source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields t1.a, t2.a
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 14

**PPL:**
```ppl
source = table1 | join left = l right = r on l.a = r.a [ source = table2 ] as s | fields l.a, s.a
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 15

**PPL:**
```ppl
source = table1 | join type=outer left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 16

**PPL:**
```ppl
source = table1 | join type=left left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 17

**PPL:**
```ppl
source = table1 | join type=inner max=1 left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 18

**PPL:**
```ppl
source = table1 | join a table2 | fields a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 19

**PPL:**
```ppl
source = table1 | join a, b table2 | fields a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 20

**PPL:**
```ppl
source = table1 | join type=outer a b table2 | fields a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 21

**PPL:**
```ppl
source = table1 | join type=inner max=1 a, b table2 | fields a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 22

**PPL:**
```ppl
source = table1 | join type=left overwrite=false max=0 a, b [source=table2 | rename d as b] | fields a, b, c
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 23

**PPL:**
```ppl
source = state_country | inner join left=a right=b ON a.name = b.name occupation | stats avg(salary) by span(age, 10) as age_span, b.country
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 24

**PPL:**
```ppl
source = state_country as a | where country = 'USA' OR country = 'England' | left join ON a.name = b.name [ source = occupation | where salary > 0 | fields name, country, salary | sort salary | head 3 ] as b | stats avg(salary) by span(age, 10) as age_span, b.country
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 25

**PPL:**
```ppl
source = state_country | where country = 'USA' OR country = 'England' | join type=left overwrite=true name [ source = occupation | where salary > 0 | fields name, country, salary | sort salary | head 3 ] | stats avg(salary) by span(age, 10) as age_span, country
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 26

**PPL:**
```ppl
source = state_country | join type=inner overwrite=false max=1 name occupation | stats avg(salary) by span(age, 10) as age_span, country
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 27

**PPL:**
```ppl
source=table1 | join left=t1 right=t2 on t1.id=t2.id table2 | eval a = 1
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 28

**PPL:**
```ppl
source=table1 | join on table1.id=table2.id table2 | eval a = 1
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 29

**PPL:**
```ppl
source=table1 | join on table1.id=t2.id table2 as t2 | eval a = 1
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

### Example 30

**PPL:**
```ppl
source=table1 | join right=tt on table1.id=t2.id [ source=table2 as t2 | eval b = id ] | eval a = 1
```

**SQL:**
```sql
SELECT *
FROM <left> AS <l>
<JOIN_TYPE> JOIN <right> AS <r>
ON <condition>
```

---

## lookup

**Reference:** [docs/user/ppl/cmd/lookup.md](../../user/ppl/cmd/lookup.md)
**SQL Feature:** Standard (LEFT JOIN)

### Example 1

**PPL:**
```ppl
source = table1 | lookup table2 id
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 2

**PPL:**
```ppl
source = table1 | lookup table2 id, name
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 3

**PPL:**
```ppl
source = table1 | lookup table2 id as cid, name
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 4

**PPL:**
```ppl
source = table1 | lookup table2 id as cid, name replace dept as department
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 5

**PPL:**
```ppl
source = table1 | lookup table2 id as cid, name replace dept as department, city as location
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 6

**PPL:**
```ppl
source = table1 | lookup table2 id as cid, name append dept as department
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 7

**PPL:**
```ppl
source = table1 | lookup table2 id as cid, name append dept as department, city as location
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 8

**PPL:**
```ppl
source = table1 | lookup table2 id as cid, name output dept as department
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 9

**PPL:**
```ppl
source = table1 | lookup table2 id as cid, name output dept as department, city as location
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 10

**PPL:**
```ppl
source = worker | LOOKUP work_information uid AS id REPLACE department | fields id, name, occupation, country, salary, department
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 11

**PPL:**
```ppl
source = worker | LOOKUP work_information uid AS id APPEND department | fields id, name, occupation, country, salary, department
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 12

**PPL:**
```ppl
source = worker | LOOKUP work_information uid AS id, name | fields id, name, occupation, country, salary, department
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 13

**PPL:**
```ppl
source = worker | LOOKUP work_information name REPLACE occupation AS new_col | fields id, name, occupation, country, salary, new_col
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

### Example 14

**PPL:**
```ppl
source = worker | LOOKUP work_information uid AS id OUTPUT department | fields id, name, occupation, country, salary, department
```

**SQL:**
```sql
SELECT <source>.*, <lookup>.<output_fields>
FROM <source>
LEFT JOIN <lookup_table> ON <source>.<src_key> = <lookup>.<lookup_key>
```

---

## multisearch

**Reference:** [docs/user/ppl/cmd/multisearch.md](../../user/ppl/cmd/multisearch.md)
**SQL Feature:** Standard (UNION ALL)

### Example 1

**PPL:**
```ppl
| multisearch [search source=table | where condition1] [search source=table | where condition2]
```

**SQL:**
```sql
<search1>
UNION ALL
<search2>
...
```

### Example 2

**PPL:**
```ppl
| multisearch [search source=index1 | fields field1, field2] [search source=index2 | fields field1, field2]
```

**SQL:**
```sql
<search1>
UNION ALL
<search2>
...
```

### Example 3

**PPL:**
```ppl
| multisearch [search source=table | where status="success"] [search source=table | where status="error"]
```

**SQL:**
```sql
<search1>
UNION ALL
<search2>
...
```

### Example 4

**PPL:**
```ppl
| multisearch [search source=accounts | where age < 30 | eval age_group = "young" | fields firstname, age, age_group] [search source=accounts | where age >= 30 | eval age_group = "adult" | fields firstname, age, age_group] | sort age
```

**SQL:**
```sql
<search1>
UNION ALL
<search2>
...
```

### Example 5

**PPL:**
```ppl
| multisearch [search source=accounts | where balance > 20000 | eval query_type = "high_balance" | fields firstname, balance, query_type] [search source=accounts | where balance > 0 AND balance <= 20000 | eval query_type = "regular" | fields firstname, balance, query_type] | sort balance desc
```

**SQL:**
```sql
<search1>
UNION ALL
<search2>
...
```

### Example 6

**PPL:**
```ppl
| multisearch [search source=time_data | where category IN ("A", "B")] [search source=time_data2 | where category IN ("E", "F")] | fields @timestamp, category, value, timestamp | head 5
```

**SQL:**
```sql
<search1>
UNION ALL
<search2>
...
```

### Example 7

**PPL:**
```ppl
| multisearch [search source=accounts | where age < 30 | eval young_flag = "yes" | fields firstname, age, young_flag] [search source=accounts | where age >= 30 | fields firstname, age] | sort age
```

**SQL:**
```sql
<search1>
UNION ALL
<search2>
...
```

---

## mvcombine

**Reference:** [docs/user/ppl/cmd/mvcombine.md](../../user/ppl/cmd/mvcombine.md)
**SQL Feature:** REPLACE + GROUP BY EXCEPT

### Example 1

**PPL:**
```ppl
source=mvcombine_data | where ip='10.0.0.1' and bytes=100 and tags='t1' | fields ip, bytes, tags, packets_str | mvcombine packets_str
```

**SQL:**
```sql
SELECT * REPLACE (ARRAY_AGG(<field>) AS <field>)
FROM <source>
GROUP BY * EXCEPT (<field>)
```

### Example 2

**PPL:**
```ppl
source=mvcombine_data | where bytes=700 and tags='t7' | fields ip, bytes, tags, packets_str | sort ip, packets_str | mvcombine packets_str | sort ip
```

**SQL:**
```sql
SELECT * REPLACE (ARRAY_AGG(<field>) AS <field>)
FROM <source>
GROUP BY * EXCEPT (<field>)
```

### Example 3

**PPL:**
```ppl
source=mvcombine_data | where ip='10.0.0.3' and bytes=300 and tags='t3' | fields ip, bytes, tags, packets_str | mvcombine packets_str
```

**SQL:**
```sql
SELECT * REPLACE (ARRAY_AGG(<field>) AS <field>)
FROM <source>
GROUP BY * EXCEPT (<field>)
```

### Example 4

**PPL:**
```ppl
source=mvcombine_data | mvcombine does_not_exist
```

**SQL:**
```sql
SELECT * REPLACE (ARRAY_AGG(<field>) AS <field>)
FROM <source>
GROUP BY * EXCEPT (<field>)
```

---

## mvexpand

**Reference:** [docs/user/ppl/cmd/mvexpand.md](../../user/ppl/cmd/mvexpand.md)
**SQL Feature:** EXCEPT + UNNEST

### Example 1

**PPL:**
```ppl
source=people | eval tags = array('error', 'warning', 'info') | fields tags | head 1 | mvexpand tags | fields tags
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), t.val AS <field>
FROM <source>
CROSS JOIN UNNEST(<field>) AS t(val)
```

### Example 2

**PPL:**
```ppl
source=people | eval ids = array(1, 2, 3, 4, 5) | fields ids | head 1 | mvexpand ids limit=3 | fields ids
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), t.val AS <field>
FROM <source>
CROSS JOIN UNNEST(<field>) AS t(val)
```

### Example 3

**PPL:**
```ppl
source=people | head 1 | fields projects | mvexpand projects | fields projects.name
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), t.val AS <field>
FROM <source>
CROSS JOIN UNNEST(<field>) AS t(val)
```

### Example 4

**PPL:**
```ppl
source=people | eval tags = array('error') | fields tags | head 1 | mvexpand tags | fields tags
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), t.val AS <field>
FROM <source>
CROSS JOIN UNNEST(<field>) AS t(val)
```

### Example 5

**PPL:**
```ppl
source=people | eval some_field = 'x' | fields some_field | head 1 | mvexpand tags | fields tags
```

**SQL:**
```sql
SELECT * EXCEPT (<field>), t.val AS <field>
FROM <source>
CROSS JOIN UNNEST(<field>) AS t(val)
```

---

## nomv

**Reference:** [docs/user/ppl/cmd/nomv.md](../../user/ppl/cmd/nomv.md)
**SQL Feature:** REPLACE

### Example 1

**PPL:**
```ppl
source=accounts | where account_number=1 | eval names = array(firstname, lastname) | nomv names | fields account_number, names
```

**SQL:**
```sql
SELECT * REPLACE (ARRAY_JOIN(<field>, '\n') AS <field>)
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | where account_number=1 | eval location = array(city, state) | nomv location | fields account_number, location
```

**SQL:**
```sql
SELECT * REPLACE (ARRAY_JOIN(<field>, '\n') AS <field>)
FROM <source>
```

---

## parse

**Reference:** [docs/user/ppl/cmd/parse.md](../../user/ppl/cmd/parse.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | parse email '.+@(?<host>.+)' | fields email, host
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <group_idx>) AS <group_name>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | parse address '\d+ (?<address>.+)' | fields address
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <group_idx>) AS <group_name>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | parse address '(?<streetNumber>\d+) (?<street>.+)' | where cast(streetNumber as int) > 500 | sort num(streetNumber) | fields streetNumber, street
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <group_idx>) AS <group_name>
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=accounts | parse address '\d+ (?<street>.+)' | parse street '\w+ (?<road>\w+)'
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <group_idx>) AS <group_name>
FROM <source>
```

### Example 5

**PPL:**
```ppl
source=accounts | parse address '\d+ (?<street>.+)' | eval street='1' | where street='1'
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <group_idx>) AS <group_name>
FROM <source>
```

### Example 6

**PPL:**
```ppl
source=accounts | parse address '\d+ (?<street>.+)' | eval address='1'
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <group_idx>) AS <group_name>
FROM <source>
```

### Example 7

**PPL:**
```ppl
source=accounts | parse email '.+@(?<host>.+)' | stats avg(age) by host | where host=pyrami.com
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <group_idx>) AS <group_name>
FROM <source>
```

---

## patterns

**Reference:** [docs/user/ppl/cmd/patterns.md](../../user/ppl/cmd/patterns.md)
**SQL Feature:** TVF

### Example 1

**PPL:**
```ppl
source=accounts | patterns email method=simple_pattern | fields email, patterns_field
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 2

**PPL:**
```ppl
source=apache | patterns message method=simple_pattern | fields message, patterns_field
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 3

**PPL:**
```ppl
source=apache | patterns message method=simple_pattern new_field='no_numbers' pattern='[0-9]' | fields message, no_numbers
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 4

**PPL:**
```ppl
source=apache | patterns message method=simple_pattern mode=aggregation | fields patterns_field, pattern_count, sample_logs
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 5

**PPL:**
```ppl
source=apache | patterns message method=simple_pattern mode=aggregation show_numbered_token=true | fields patterns_field, pattern_count, tokens | head 1
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 6

**PPL:**
```ppl
source=apache | patterns message method=brain | fields message, patterns_field
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 7

**PPL:**
```ppl
source=apache | patterns message method=brain variable_count_threshold=2 | fields message, patterns_field
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 8

**PPL:**
```ppl
source=apache | patterns message method=brain mode=aggregation variable_count_threshold=2 | fields patterns_field, pattern_count, sample_logs
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

### Example 9

**PPL:**
```ppl
source=apache | patterns message method=brain mode=aggregation show_numbered_token=true variable_count_threshold=2 | fields patterns_field, pattern_count, tokens
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_PATTERNS(TABLE(<source>), '<field>', ...)
```

---

## rare

**Reference:** [docs/user/ppl/cmd/rare.md](../../user/ppl/cmd/rare.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | rare showcount=false gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count ASC
LIMIT <N>
```

### Example 2

**PPL:**
```ppl
source=accounts | rare showcount=false age by gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count ASC
LIMIT <N>
```

### Example 3

**PPL:**
```ppl
source=accounts | rare gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count ASC
LIMIT <N>
```

### Example 4

**PPL:**
```ppl
source=accounts | rare countfield='cnt' gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count ASC
LIMIT <N>
```

### Example 5

**PPL:**
```ppl
source=accounts | rare usenull=false email
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count ASC
LIMIT <N>
```

### Example 6

**PPL:**
```ppl
source=accounts | rare usenull=true email
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count ASC
LIMIT <N>
```

---

## regex

**Reference:** [docs/user/ppl/cmd/regex.md](../../user/ppl/cmd/regex.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | regex lastname="^[A-Z][a-z]+$" | fields account_number, firstname, lastname
```

**SQL:**
```sql
SELECT account_number, firstname, lastname
FROM accounts
WHERE REGEXP_LIKE(lastname, '^[A-Z][a-z]+$')
```

### Example 2

**PPL:**
```ppl
source=accounts | regex lastname!=".*ms$" | fields account_number, lastname
```

**SQL:**
```sql
SELECT account_number, lastname
FROM accounts
WHERE NOT REGEXP_LIKE(lastname, '.*ms$')
```

### Example 3

**PPL:**
```ppl
source=accounts | regex email="@pyrami\.com$" | fields account_number, email
```

**SQL:**
```sql
SELECT account_number, email
FROM accounts
WHERE REGEXP_LIKE(email, '@pyrami\.com$')
```

### Example 4

**PPL:**
```ppl
source=accounts | regex address="\\d{3,4}\\s+[A-Z][a-z]+\\s+(Street|Lane|Court)" | fields account_number, address
```

**SQL:**
```sql
SELECT account_number, address
FROM accounts
WHERE REGEXP_LIKE(address, '\\d{3,4}\\s+[A-Z][a-z]+\\s+(Street|Lane|Court)')
```

### Example 5

**PPL:**
```ppl
source=accounts | regex state="va" | fields account_number, state
```

**SQL:**
```sql
SELECT account_number, state
FROM accounts
WHERE REGEXP_LIKE(state, 'va')
```

### Example 6

**PPL:**
```ppl
source=accounts | regex state="VA" | fields account_number, state
```

**SQL:**
```sql
SELECT account_number, state
FROM accounts
WHERE REGEXP_LIKE(state, 'VA')
```

---

## rename

**Reference:** [docs/user/ppl/cmd/rename.md](../../user/ppl/cmd/rename.md)
**SQL Feature:** REPLACE

### Example 1

**PPL:**
```ppl
source=accounts | rename account_number as an | fields an
```

**SQL:**
```sql
SELECT * REPLACE (<old> AS <new>)
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | rename account_number as an, employer as emp | fields an, emp
```

**SQL:**
```sql
SELECT * REPLACE (<old> AS <new>)
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | rename *name as *_name | fields first_name, last_name
```

**SQL:**
```sql
SELECT * REPLACE (<old> AS <new>)
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=accounts | rename *name as *_name, *_number as *number | fields first_name, last_name, accountnumber
```

**SQL:**
```sql
SELECT * REPLACE (<old> AS <new>)
FROM <source>
```

### Example 5

**PPL:**
```ppl
source=accounts | rename firstname as age | fields age
```

**SQL:**
```sql
SELECT * REPLACE (<old> AS <new>)
FROM <source>
```

---

## replace

**Reference:** [docs/user/ppl/cmd/replace.md](../../user/ppl/cmd/replace.md)
**SQL Feature:** REPLACE

### Example 1

**PPL:**
```ppl
source=accounts | replace "IL" WITH "Illinois" IN state | fields state
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | replace "IL" WITH "Illinois" IN state, address | fields state, address
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | replace "IL" WITH "Illinois" IN state | where age > 30 | fields state, age
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=accounts | replace "IL" WITH "Illinois", "TN" WITH "Tennessee" IN state | fields state
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 5

**PPL:**
```ppl
source=accounts | where LIKE(address, '%Holmes%') | replace "Holmes" WITH "HOLMES" IN address | fields address, state, gender, age, city
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 6

**PPL:**
```ppl
source=accounts | replace "*IL" WITH "Illinois" IN state | fields state
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 7

**PPL:**
```ppl
source=accounts | replace "IL*" WITH "Illinois" IN state | fields state
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 8

**PPL:**
```ppl
source=accounts | replace "* Lane" WITH "Lane *" IN address | fields address
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 9

**PPL:**
```ppl
source=accounts | replace "* *" WITH "*_*" IN address | fields address
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 10

**PPL:**
```ppl
source=accounts | replace "*IL*" WITH "Illinois" IN state | fields state
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 11

**PPL:**
```ppl
source=accounts | eval note = 'price: *sale*' | replace 'price: \*sale\*' WITH 'DISCOUNTED' IN note | fields note
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

### Example 12

**PPL:**
```ppl
source=accounts | eval label = 'file123.txt' | replace 'file*.*' WITH '\**.*' IN label | fields label
```

**SQL:**
```sql
SELECT * REPLACE (REPLACE(<field>, '<old>', '<new>') AS <field>)
FROM <source>
```

---

## reverse

**Reference:** [docs/user/ppl/cmd/reverse.md](../../user/ppl/cmd/reverse.md)
**SQL Feature:** Standard (Window)

### Example 1

**PPL:**
```ppl
source=accounts | fields account_number, age | reverse
```

**SQL:**
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER () AS _seq
  FROM <source>
)
ORDER BY _seq DESC
```

### Example 2

**PPL:**
```ppl
source=accounts | sort age | fields account_number, age | reverse
```

**SQL:**
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER () AS _seq
  FROM <source>
)
ORDER BY _seq DESC
```

### Example 3

**PPL:**
```ppl
source=accounts | reverse | head 2 | fields account_number, age
```

**SQL:**
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER () AS _seq
  FROM <source>
)
ORDER BY _seq DESC
```

### Example 4

**PPL:**
```ppl
source=accounts | reverse | reverse | fields account_number, age
```

**SQL:**
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER () AS _seq
  FROM <source>
)
ORDER BY _seq DESC
```

### Example 5

**PPL:**
```ppl
source=accounts | where age > 30 | fields account_number, age | reverse
```

**SQL:**
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER () AS _seq
  FROM <source>
)
ORDER BY _seq DESC
```

---

## rex

**Reference:** [docs/user/ppl/cmd/rex.md](../../user/ppl/cmd/rex.md)
**SQL Feature:** Standard / REPLACE

### Example 1

**PPL:**
```ppl
source=accounts | rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)" | fields email, username, domain | head 2
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | rex field=email "(?<user>[^@]+)@(?<domain>gmail\\.com)" | fields email, user, domain | head 2
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | rex field=address "(?<words>[A-Za-z]+)" max_match=2 | fields address, words | head 3
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=accounts | rex field=email mode=sed "s/@.*/@company.com/" | fields email | head 2
```

**SQL:**
```sql
SELECT * REPLACE (REGEXP_REPLACE(<field>, '<pattern>', '<replacement>') AS <field>)
FROM <source>
```

### Example 5

**PPL:**
```ppl
source=accounts | rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)" offset_field=matchpos | fields email, username, domain, matchpos | head 2
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 6

**PPL:**
```ppl
source=accounts | rex field=email "(?<user>[a-zA-Z0-9._%+-]+)@(?<domain>[a-zA-Z0-9.-]+)\\.(?<tld>[a-zA-Z]{2,})" | fields email, user, domain, tld | head 2
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 7

**PPL:**
```ppl
source=accounts | rex field=firstname "(?<firstinitial>^.)" | rex field=lastname "(?<lastinitial>^.)" | fields firstname, lastname, firstinitial, lastinitial | head 3
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 8

**PPL:**
```ppl
source=accounts | rex field=email "(?<user_name>[^@]+)@(?<email_domain>[^.]+)" | fields email, user_name, email_domain
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 9

**PPL:**
```ppl
source=accounts | rex field=email "(?<username>[^@]+)@(?<emaildomain>[^.]+)" | fields email, username, emaildomain | head 2
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 10

**PPL:**
```ppl
source=accounts | rex field=address "(?<digit>\\d*)" max_match=0 | eval digit_count=array_length(digit) | fields address, digit_count | head 1
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

### Example 11

**PPL:**
```ppl
source=accounts | rex field=address "(?<digit>\\d*)" max_match=100 | fields address, digit | head 1
```

**SQL:**
```sql
SELECT *, REGEXP_EXTRACT(<field>, '<pattern>', <N>) AS <group_name>
FROM <source>
```

---

## search

**Reference:** [docs/user/ppl/cmd/search.md](../../user/ppl/cmd/search.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=logs | where cast(ip_address as string) like '1%'
```

**SQL:**
```sql
SELECT * FROM <index>
```

---

## sort

**Reference:** [docs/user/ppl/cmd/sort.md](../../user/ppl/cmd/sort.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | sort age | fields account_number, age
```

**SQL:**
```sql
SELECT account_number, age
FROM accounts
ORDER BY age
```

### Example 2

**PPL:**
```ppl
source=accounts | sort - age | fields account_number, age
```

**SQL:**
```sql
SELECT account_number, age
FROM accounts
ORDER BY age DESC
```

### Example 3

**PPL:**
```ppl
source=accounts | sort age desc | fields account_number, age
```

**SQL:**
```sql
SELECT account_number, age
FROM accounts
ORDER BY age desc
```

### Example 4

**PPL:**
```ppl
source=accounts | sort + gender, - age | fields account_number, gender, age
```

**SQL:**
```sql
SELECT account_number, gender, age
FROM accounts
ORDER BY + gender, - age
```

### Example 5

**PPL:**
```ppl
source=accounts | sort gender asc, age desc | fields account_number, gender, age
```

**SQL:**
```sql
SELECT account_number, gender, age
FROM accounts
ORDER BY gender asc, age desc
```

### Example 6

**PPL:**
```ppl
source=accounts | sort employer | fields employer
```

**SQL:**
```sql
SELECT employer
FROM accounts
ORDER BY employer
```

### Example 7

**PPL:**
```ppl
source=accounts | sort 2 age | fields account_number, age
```

**SQL:**
```sql
SELECT account_number, age
FROM accounts
ORDER BY 2 age
```

### Example 8

**PPL:**
```ppl
source=accounts | sort str(account_number) | fields account_number
```

**SQL:**
```sql
SELECT account_number
FROM accounts
ORDER BY str(account_number)
```

---

## spath

**Reference:** [docs/user/ppl/cmd/spath.md](../../user/ppl/cmd/spath.md)
**SQL Feature:** Standard / TVF

### Example 1

**PPL:**
```ppl
source=structured | spath input=doc_n n | fields doc_n n
```

**SQL:**
```sql
SELECT *, JSON_EXTRACT(<input>, '$.<path>') AS <output>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=structured | spath input=doc_list output=first_element list{0} | spath input=doc_list output=all_elements list{} | spath input=doc_list output=nested nest_out.nest_in | fields doc_list first_element all_elements nested
```

**SQL:**
```sql
SELECT *, JSON_EXTRACT(<input>, '$.<path>') AS <output>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=structured | spath input=doc_n n | eval n=cast(n as int) | stats sum(n) | fields `sum(n)`
```

**SQL:**
```sql
SELECT *, JSON_EXTRACT(<input>, '$.<path>') AS <output>
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=structured | spath output=a input=doc_escape "['a fancy field name']" | spath output=b input=doc_escape "['a.b.c']" | fields a b
```

**SQL:**
```sql
SELECT *, JSON_EXTRACT(<input>, '$.<path>') AS <output>
FROM <source>
```

### Example 5

**PPL:**
```ppl
source=structured | spath input=doc_auto output=doc | fields doc_auto, doc.user.name, doc.user.age, doc.`tags{}`, doc.active
```

**SQL:**
```sql
SELECT *, JSON_EXTRACT(<input>, '$.<path>') AS <output>
FROM <source>
```

---

## stats

**Reference:** [docs/user/ppl/cmd/stats.md](../../user/ppl/cmd/stats.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | stats count()
```

**SQL:**
```sql
SELECT COUNT(*)
FROM accounts
```

### Example 2

**PPL:**
```ppl
source=accounts | stats avg(age)
```

**SQL:**
```sql
SELECT avg(age)
FROM accounts
```

### Example 3

**PPL:**
```ppl
source=accounts | stats avg(age) by gender
```

**SQL:**
```sql
SELECT avg(age), gender
FROM accounts
GROUP BY gender
```

### Example 4

**PPL:**
```ppl
source=accounts | stats avg(age), sum(age), count() by gender
```

**SQL:**
```sql
SELECT avg(age), sum(age), COUNT(*), gender
FROM accounts
GROUP BY gender
```

### Example 5

**PPL:**
```ppl
source=accounts | stats max(age)
```

**SQL:**
```sql
SELECT max(age)
FROM accounts
```

### Example 6

**PPL:**
```ppl
source=accounts | stats max(age), min(age) by gender
```

**SQL:**
```sql
SELECT max(age), min(age), gender
FROM accounts
GROUP BY gender
```

### Example 7

**PPL:**
```ppl
source=accounts | stats count(gender), distinct_count(gender)
```

**SQL:**
```sql
SELECT count(gender), distinct_count(gender)
FROM accounts
```

### Example 8

**PPL:**
```ppl
source=accounts | stats count(age) by span(age, 10) as age_span
```

**SQL:**
```sql
SELECT count(age), span(age, 10) as age_span
FROM accounts
GROUP BY span(age, 10) as age_span
```

### Example 9

**PPL:**
```ppl
source=accounts | stats count() as cnt by span(age, 5) as age_span, gender
```

**SQL:**
```sql
SELECT COUNT(*) as cnt, span(age, 5) as age_span, gender
FROM accounts
GROUP BY span(age, 5) as age_span, gender
```

### Example 10

**PPL:**
```ppl
source=accounts | stats count() as cnt by gender, span(age, 5) as age_span
```

**SQL:**
```sql
SELECT COUNT(*) as cnt, gender, span(age, 5) as age_span
FROM accounts
GROUP BY gender, span(age, 5) as age_span
```

### Example 11

**PPL:**
```ppl
source=accounts | stats count() as cnt, take(email, 5) by span(age, 5) as age_span, gender
```

**SQL:**
```sql
SELECT COUNT(*) as cnt, take(email, 5), span(age, 5) as age_span, gender
FROM accounts
GROUP BY span(age, 5) as age_span, gender
```

### Example 12

**PPL:**
```ppl
source=accounts | stats percentile(age, 90)
```

**SQL:**
```sql
SELECT percentile(age, 90)
FROM accounts
```

### Example 13

**PPL:**
```ppl
source=accounts | stats percentile(age, 90) by gender
```

**SQL:**
```sql
SELECT percentile(age, 90), gender
FROM accounts
GROUP BY gender
```

### Example 14

**PPL:**
```ppl
source=accounts | stats percentile(age, 90) as p90 by span(age, 10) as age_span, gender
```

**SQL:**
```sql
SELECT percentile(age, 90) as p90, span(age, 10) as age_span, gender
FROM accounts
GROUP BY span(age, 10) as age_span, gender
```

### Example 15

**PPL:**
```ppl
source=accounts | stats list(firstname)
```

**SQL:**
```sql
SELECT list(firstname)
FROM accounts
```

### Example 16

**PPL:**
```ppl
source=accounts | stats bucket_nullable=false count() as cnt by email
```

**SQL:**
```sql
SELECT bucket_nullable=false COUNT(*) as cnt, email
FROM accounts
GROUP BY email
```

### Example 17

**PPL:**
```ppl
source=accounts | stats values(firstname)
```

**SQL:**
```sql
SELECT values(firstname)
FROM accounts
```

### Example 18

**PPL:**
```ppl
source=example | stats count() as cnt by span(birthday, 1y) as year
```

**SQL:**
```sql
SELECT COUNT(*) as cnt, span(birthday, 1y) as year
FROM example
GROUP BY span(birthday, 1y) as year
```

### Example 19

**PPL:**
```ppl
source=example | stats count() as cnt by span(birthday, 1y) as year, DEPTNO
```

**SQL:**
```sql
SELECT COUNT(*) as cnt, span(birthday, 1y) as year, DEPTNO
FROM example
GROUP BY span(birthday, 1y) as year, DEPTNO
```

### Example 20

**PPL:**
```ppl
source=example | stats bucket_nullable=false count() as cnt by span(birthday, 1y) as year, DEPTNO
```

**SQL:**
```sql
SELECT bucket_nullable=false COUNT(*) as cnt, span(birthday, 1y) as year, DEPTNO
FROM example
GROUP BY span(birthday, 1y) as year, DEPTNO
```

### Example 21

**PPL:**
```ppl
source=big5 | stats count() by span(1month)
```

**SQL:**
```sql
SELECT COUNT(*), span(1month)
FROM big5
GROUP BY span(1month)
```

### Example 22

**PPL:**
```ppl
source=hits | stats bucket_nullable=false count() as c by URL | sort - c | head 10
```

**SQL:**
```sql
SELECT bucket_nullable=false COUNT(*) as c, URL | sort - c | head 10
FROM hits
GROUP BY URL | sort - c | head 10
```

### Example 23

**PPL:**
```ppl
source=hits | stats bucket_nullable=false count() as c by URL | sort + c | head 10
```

**SQL:**
```sql
SELECT bucket_nullable=false COUNT(*) as c, URL | sort + c | head 10
FROM hits
GROUP BY URL | sort + c | head 10
```

---

## streamstats

**Reference:** [docs/user/ppl/cmd/streamstats.md](../../user/ppl/cmd/streamstats.md)
**SQL Feature:** Standard (Window) / TVF

### Example 1

**PPL:**
```ppl
source = table | streamstats avg(a)
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source = table | streamstats current = false avg(a)
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source = table | streamstats window = 5 sum(b)
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 4

**PPL:**
```ppl
source = table | streamstats current = false window = 2 max(a)
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 5

**PPL:**
```ppl
source = table | where a < 50 | streamstats count(c)
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 6

**PPL:**
```ppl
source = table | streamstats min(c), max(c) by b
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 7

**PPL:**
```ppl
source = table | streamstats count(c) as count_by by b | where count_by > 1000
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 8

**PPL:**
```ppl
source = table | streamstats dc(field) as distinct_count
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 9

**PPL:**
```ppl
source = table | streamstats distinct_count(category) by region
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 10

**PPL:**
```ppl
source = table | streamstats current=false window=2 global=false avg(a) by b
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 11

**PPL:**
```ppl
source = table | streamstats window=2 reset_before=a>31 avg(b)
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_STREAMSTATS(TABLE(<source>), ...)
```

### Example 12

**PPL:**
```ppl
source = table | streamstats current=false reset_after=a>31 avg(b) by c
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_STREAMSTATS(TABLE(<source>), ...)
```

### Example 13

**PPL:**
```ppl
source=accounts | streamstats avg(age) as running_avg, sum(age) as running_sum, count() as running_count by gender
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 14

**PPL:**
```ppl
source=state_country | streamstats current=false window=2 max(age) as prev_max_age
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 15

**PPL:**
```ppl
source=state_country | streamstats window=2 global=true avg(age) as running_avg by country
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 16

**PPL:**
```ppl
source=state_country | streamstats window=2 global=false avg(age) as running_avg by country
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 17

**PPL:**
```ppl
source=state_country | streamstats current=false reset_before=age>34 reset_after=age<25 avg(age) as avg_age by country
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_STREAMSTATS(TABLE(<source>), ...)
```

### Example 18

**PPL:**
```ppl
source=accounts | streamstats bucket_nullable=false count() as cnt by employer | fields account_number, firstname, employer, cnt
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 19

**PPL:**
```ppl
source=accounts | streamstats bucket_nullable=true count() as cnt by employer | fields account_number, firstname, employer, cnt
```

**SQL:**
```sql
SELECT *, <agg> OVER (
  PARTITION BY <groups>
  ORDER BY _seq
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

---

## subquery

**Reference:** [docs/user/ppl/cmd/subquery.md](../../user/ppl/cmd/subquery.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source = outer | where a in [ source = inner | fields b ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 2

**PPL:**
```ppl
source = outer | where (a) in [ source = inner | fields b ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 3

**PPL:**
```ppl
source = outer | where (a,b,c) in [ source = inner | fields d,e,f ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 4

**PPL:**
```ppl
source = outer | where a not in [ source = inner | fields b ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 5

**PPL:**
```ppl
source = outer | where (a) not in [ source = inner | fields b ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 6

**PPL:**
```ppl
source = outer | where (a,b,c) not in [ source = inner | fields d,e,f ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 7

**PPL:**
```ppl
source = outer a in [ source = inner | fields b ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 8

**PPL:**
```ppl
source = outer a not in [ source = inner | fields b ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 9

**PPL:**
```ppl
source = outer | where a in [ source = inner1 | where b not in [ source = inner2 | fields c ] | fields b ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 10

**PPL:**
```ppl
source = table1 | inner join left = l right = r on l.a = r.a AND r.a in [ source = inner | fields d ] | fields l.a, r.a, b, c
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 11

**PPL:**
```ppl
source = outer | where exists [ source = inner | where a = c ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 12

**PPL:**
```ppl
source = outer | where not exists [ source = inner | where a = c ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 13

**PPL:**
```ppl
source = outer | where exists [ source = inner | where a = c and b = d ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 14

**PPL:**
```ppl
source = outer | where not exists [ source = inner | where a = c and b = d ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 15

**PPL:**
```ppl
source = outer exists [ source = inner | where a = c ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 16

**PPL:**
```ppl
source = outer not exists [ source = inner | where a = c ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 17

**PPL:**
```ppl
source = table as t1 exists [ source = table as t2 | where t1.a = t2.a ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 18

**PPL:**
```ppl
source = outer | where exists [ source = inner1 | where a = c and exists [ source = nested | where c = e ] ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 19

**PPL:**
```ppl
source = outer | where exists [ source = inner1 | where a = c | where exists [ source = nested | where c = e ] ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 20

**PPL:**
```ppl
source = outer | where exists [ source = inner | where c > 10 ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 21

**PPL:**
```ppl
source = outer | where not exists [ source = inner | where c > 10 ]
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 22

**PPL:**
```ppl
source = outer | where exists [ source = inner ] | eval l = "nonEmpty" | fields l
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE EXISTS (SELECT 1 FROM <inner> WHERE <correlation>)
```

### Example 23

**PPL:**
```ppl
source = outer | eval m = [ source = inner | stats max(c) ] | fields m, a
```

**SQL:**
```sql
SELECT *, (SELECT <agg> FROM <inner> WHERE ...) AS <alias>
FROM <outer>
```

### Example 24

**PPL:**
```ppl
source = outer | eval m = [ source = inner | stats max(c) ] + b | fields m, a
```

**SQL:**
```sql
SELECT *, (SELECT <agg> FROM <inner> WHERE ...) AS <alias>
FROM <outer>
```

### Example 25

**PPL:**
```ppl
source = outer | where a > [ source = inner | stats min(c) ] | fields a
```

**SQL:**
```sql
-- Complex subquery: source = outer | where a > [ source = inner | stats min(c) ] | fields a
```

### Example 26

**PPL:**
```ppl
source = outer a > [ source = inner | stats min(c) ] | fields a
```

**SQL:**
```sql
-- Complex subquery: source = outer a > [ source = inner | stats min(c) ] | fields a
```

### Example 27

**PPL:**
```ppl
source = outer | eval m = [ source = inner | where outer.b = inner.d | stats max(c) ] | fields m, a
```

**SQL:**
```sql
SELECT *, (SELECT <agg> FROM <inner> WHERE ...) AS <alias>
FROM <outer>
```

### Example 28

**PPL:**
```ppl
source = outer | eval m = [ source = inner | where b = d | stats max(c) ] | fields m, a
```

**SQL:**
```sql
SELECT *, (SELECT <agg> FROM <inner> WHERE ...) AS <alias>
FROM <outer>
```

### Example 29

**PPL:**
```ppl
source = outer | eval m = [ source = inner | where outer.b > inner.d | stats max(c) ] | fields m, a
```

**SQL:**
```sql
SELECT *, (SELECT <agg> FROM <inner> WHERE ...) AS <alias>
FROM <outer>
```

### Example 30

**PPL:**
```ppl
source = outer | where a = [ source = inner | where outer.b = inner.d | stats max(c) ]
```

**SQL:**
```sql
-- Complex subquery: source = outer | where a = [ source = inner | where outer.b = inner.d | stats ma
```

### Example 31

**PPL:**
```ppl
source = outer | where a = [ source = inner | where b = d | stats max(c) ]
```

**SQL:**
```sql
-- Complex subquery: source = outer | where a = [ source = inner | where b = d | stats max(c) ]
```

### Example 32

**PPL:**
```ppl
source = outer | where [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a
```

**SQL:**
```sql
-- Complex subquery: source = outer | where [ source = inner | where outer.b = inner.d OR inner.d = 1
```

### Example 33

**PPL:**
```ppl
source = outer a = [ source = inner | where b = d | stats max(c) ]
```

**SQL:**
```sql
-- Complex subquery: source = outer a = [ source = inner | where b = d | stats max(c) ]
```

### Example 34

**PPL:**
```ppl
source = outer [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a
```

**SQL:**
```sql
-- Complex subquery: source = outer [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats
```

### Example 35

**PPL:**
```ppl
source = outer | where a = [ source = inner | stats max(c) | sort c ] OR b = [ source = inner | where c = 1 | stats min(d) | sort d ]
```

**SQL:**
```sql
-- Complex subquery: source = outer | where a = [ source = inner | stats max(c) | sort c ] OR b = [ s
```

### Example 36

**PPL:**
```ppl
source = outer | where a = [ source = inner | where c = [ source = nested | stats max(e) by f | sort f ] | stats max(d) by c | sort c | head 1 ]
```

**SQL:**
```sql
-- Complex subquery: source = outer | where a = [ source = inner | where c = [ source = nested | stat
```

### Example 37

**PPL:**
```ppl
source = table1 | join left = l right = r on condition [ source = table2 | where d > 10 | head 5 ]
```

**SQL:**
```sql
SELECT * FROM <left>
JOIN (SELECT ... FROM <right> WHERE ...) AS <alias>
ON <condition>
```

### Example 38

**PPL:**
```ppl
source = [ source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ] | stats count(a) by b ] as outer | head 1
```

**SQL:**
```sql
SELECT * FROM <left>
JOIN (SELECT ... FROM <right> WHERE ...) AS <alias>
ON <condition>
```

### Example 39

**PPL:**
```ppl
source = supplier | join ON s_nationkey = n_nationkey nation | where n_name = 'CANADA'
```

**SQL:**
```sql
SELECT * FROM <left>
JOIN (SELECT ... FROM <right> WHERE ...) AS <alias>
ON <condition>
```

### Example 40

**PPL:**
```ppl
source = partsupp | where ps_partkey in [
```

**SQL:**
```sql
SELECT * FROM <outer>
WHERE <field> IN (SELECT <field> FROM <inner> WHERE ...)
```

### Example 41

**PPL:**
```ppl
source = part | where like(p_name, 'forest%') | fields p_partkey
```

**SQL:**
```sql
-- Complex subquery: source = part | where like(p_name, 'forest%') | fields p_partkey
```

### Example 42

**PPL:**
```ppl
source = lineitem | where l_partkey = ps_partkey
```

**SQL:**
```sql
-- Complex subquery: source = lineitem | where l_partkey = ps_partkey
```

### Example 43

**PPL:**
```ppl
source = customer | where substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
```

**SQL:**
```sql
-- Complex subquery: source = customer | where substring(c_phone, 1, 2) in ('13', '31', '23', '29', '
```

### Example 44

**PPL:**
```ppl
source = customer | where c_acctbal > 0.00
```

**SQL:**
```sql
-- Complex subquery: source = customer | where c_acctbal > 0.00
```

### Example 45

**PPL:**
```ppl
source = orders | where o_custkey = c_custkey
```

**SQL:**
```sql
-- Complex subquery: source = orders | where o_custkey = c_custkey
```

---

## table

**Reference:** [docs/user/ppl/cmd/table.md](../../user/ppl/cmd/table.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | table firstname lastname age
```

**SQL:**
```sql
SELECT <fields> FROM <source>
```

---

## timechart

**Reference:** [docs/user/ppl/cmd/timechart.md](../../user/ppl/cmd/timechart.md)
**SQL Feature:** Standard / TVF (PIVOT)

### Example 1

**PPL:**
```ppl
source=events | timechart span=1h count() by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 2

**PPL:**
```ppl
source=events | timechart span=1m count() by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 3

**PPL:**
```ppl
source=events | timechart span=1m avg(packets)
```

**SQL:**
```sql
SELECT <agg>, SPAN(@timestamp, '<span>') AS ts
FROM <source>
GROUP BY SPAN(@timestamp, '<span>')
```

### Example 4

**PPL:**
```ppl
source=events | timechart span=20m avg(packets) by status
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 5

**PPL:**
```ppl
source=events | timechart span=1h count() by category
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 6

**PPL:**
```ppl
source=events | timechart span=1m limit=2 count() by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 7

**PPL:**
```ppl
source=events_many_hosts | timechart span=1h limit=0 count() by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 8

**PPL:**
```ppl
source=events_many_hosts | timechart span=1h useother=false count() by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 9

**PPL:**
```ppl
source=events_many_hosts | timechart span=1h limit=3 avg(cpu_usage) by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 10

**PPL:**
```ppl
source=events_many_hosts | timechart span=1h limit=3 useother=false avg(cpu_usage) by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 11

**PPL:**
```ppl
source=events_null | timechart span=1h count() by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

### Example 12

**PPL:**
```ppl
source=events | timechart span=30m per_second(packets) by host
```

**SQL:**
```sql
TVF (PIVOT): SELECT <agg>, SPAN(@timestamp, '<span>') FROM <source> GROUP BY SPAN(...)
-- PIVOT by <field> produces dynamic columns
```

---

## top

**Reference:** [docs/user/ppl/cmd/top.md](../../user/ppl/cmd/top.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | top gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count DESC
LIMIT <N>
```

### Example 2

**PPL:**
```ppl
source=accounts | top showcount=false gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count DESC
LIMIT <N>
```

### Example 3

**PPL:**
```ppl
source=accounts | top countfield='cnt' gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count DESC
LIMIT <N>
```

### Example 4

**PPL:**
```ppl
source=accounts | top 1 showcount=false gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count DESC
LIMIT <N>
```

### Example 5

**PPL:**
```ppl
source=accounts | top 1 showcount=false age by gender
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count DESC
LIMIT <N>
```

### Example 6

**PPL:**
```ppl
source=accounts | top usenull=false email
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count DESC
LIMIT <N>
```

### Example 7

**PPL:**
```ppl
source=accounts | top usenull=true email
```

**SQL:**
```sql
SELECT <field>, COUNT(*) AS count
FROM <source>
GROUP BY <field>
ORDER BY count DESC
LIMIT <N>
```

---

## transpose

**Reference:** [docs/user/ppl/cmd/transpose.md](../../user/ppl/cmd/transpose.md)
**SQL Feature:** TVF

### Example 1

**PPL:**
```ppl
source=accounts | head 5 | fields account_number, firstname, lastname, balance | transpose
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_TRANSPOSE(TABLE(<source>))
```

### Example 2

**PPL:**
```ppl
source=accounts | head 5 | fields account_number, firstname, lastname, balance | transpose 4
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_TRANSPOSE(TABLE(<source>))
```

### Example 3

**PPL:**
```ppl
source=accounts | head 5 | fields account_number, firstname, lastname, balance | transpose 4 column_name='column_names'
```

**SQL:**
```sql
TVF: SELECT * FROM PPL_TRANSPOSE(TABLE(<source>))
```

---

## trendline

**Reference:** [docs/user/ppl/cmd/trendline.md](../../user/ppl/cmd/trendline.md)
**SQL Feature:** Standard (Window)

### Example 1

**PPL:**
```ppl
source=accounts | trendline sma(2, account_number) as an | fields an
```

**SQL:**
```sql
SELECT *, AVG(<field>) OVER (
  ORDER BY <sort_field>
  ROWS BETWEEN <N-1> PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 2

**PPL:**
```ppl
source=accounts | trendline sma(2, account_number) as an sma(2, age) as age_trend | fields an, age_trend
```

**SQL:**
```sql
SELECT *, AVG(<field>) OVER (
  ORDER BY <sort_field>
  ROWS BETWEEN <N-1> PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 3

**PPL:**
```ppl
source=accounts | trendline sma(2, account_number) | fields account_number_trendline
```

**SQL:**
```sql
SELECT *, AVG(<field>) OVER (
  ORDER BY <sort_field>
  ROWS BETWEEN <N-1> PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

### Example 4

**PPL:**
```ppl
source=accounts | trendline wma(2, account_number) | fields account_number_trendline
```

**SQL:**
```sql
SELECT *, AVG(<field>) OVER (
  ORDER BY <sort_field>
  ROWS BETWEEN <N-1> PRECEDING AND CURRENT ROW
) AS <alias>
FROM <source>
```

---

## where

**Reference:** [docs/user/ppl/cmd/where.md](../../user/ppl/cmd/where.md)
**SQL Feature:** Standard

### Example 1

**PPL:**
```ppl
source=accounts | where balance > 30000 | fields account_number, balance
```

**SQL:**
```sql
SELECT account_number, balance
FROM accounts
WHERE balance > 30000
```

### Example 2

**PPL:**
```ppl
source=accounts | where age > 30 AND gender = 'M' | fields account_number, age, gender
```

**SQL:**
```sql
SELECT account_number, age, gender
FROM accounts
WHERE age > 30 AND gender = 'M'
```

### Example 3

**PPL:**
```ppl
source=accounts | where account_number=1 or gender="F" | fields account_number, gender
```

**SQL:**
```sql
SELECT account_number, gender
FROM accounts
WHERE account_number=1 or gender="F"
```

### Example 4

**PPL:**
```ppl
source=accounts | where LIKE(state, 'M_') | fields account_number, state
```

**SQL:**
```sql
SELECT account_number, state
FROM accounts
WHERE LIKE(state, 'M_')
```

### Example 5

**PPL:**
```ppl
source=accounts | where LIKE(state, 'V%') | fields account_number, state
```

**SQL:**
```sql
SELECT account_number, state
FROM accounts
WHERE LIKE(state, 'V%')
```

### Example 6

**PPL:**
```ppl
source=accounts | where NOT state = 'CA' | fields account_number, state
```

**SQL:**
```sql
SELECT account_number, state
FROM accounts
WHERE NOT state = 'CA'
```

### Example 7

**PPL:**
```ppl
source=accounts | where state IN ('IL', 'VA') | fields account_number, state
```

**SQL:**
```sql
SELECT account_number, state
FROM accounts
WHERE state IN ('IL', 'VA')
```

### Example 8

**PPL:**
```ppl
source=accounts | where ISNULL(employer) | fields account_number, employer
```

**SQL:**
```sql
SELECT account_number, employer
FROM accounts
WHERE ISNULL(employer)
```

### Example 9

**PPL:**
```ppl
source=accounts | where (balance > 40000 OR age > 35) AND gender = 'M' | fields account_number, balance, age, gender
```

**SQL:**
```sql
SELECT account_number, balance, age, gender
FROM accounts
WHERE (balance > 40000 OR age > 35) AND gender = 'M'
```

---
