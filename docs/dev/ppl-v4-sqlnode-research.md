# PPL V4 Architecture Research: SqlNode-Centered Execution Path

## 1. Executive Summary

**Can PPL V4 reasonably center on SqlNode?** Yes — with high confidence.

Based on analysis of all 48 PPL commands, the existing V4 prototype code (CalciteSqlNodeVisitor, SqlNodeDsl), Calcite's SqlNode extension mechanisms, and Splunk SPL semantic comparison:

- **~85% of PPL commands** map naturally to standard SQL/SqlNode constructs (SELECT, WHERE, GROUP BY, ORDER BY, JOIN, WINDOW, UNNEST, UNION, CASE, UDFs)
- **~10% require moderate Calcite extensions** — custom SqlOperator/SqlFunction wrappers, but remain within SqlNode semantics (rex, parse, grok, patterns, spath, bin, chart/timechart, trendline, replace, addtotals)
- **~5% are genuinely hard** — require either custom SqlNode subclasses or represent V4 breaking change candidates (transpose, addcoltotals row-append semantics, appendcol positional join, graphlookup recursive traversal, ML commands)

**Major blockers:** None that are architectural. The hard cases are isolated commands, not systemic problems.

**Likely breaking changes:**
1. `transpose` — row-to-column pivot with dynamic column names; needs schema-aware PIVOT or custom node
2. `addcoltotals` — appending a summary row to results is non-relational (UNION with aggregation works but changes row count semantics)
3. `appendcol` — positional join (row-by-row zip) has no SQL equivalent; needs custom operator or LATERAL with ROW_NUMBER
4. `graphlookup` — recursive graph traversal; needs recursive CTE or custom node
5. `kmeans/ad/ml` — ML commands are inherently non-SQL; table function wrappers are the best fit
6. `consecutive` option in `dedup` — order-dependent duplicate removal requires stateful processing
7. `streamstats` `reset_before`/`reset_after` — conditional window reset has no SQL equivalent

**The existing V4 prototype (CalciteSqlNodeVisitor + SqlNodeDsl) is architecturally sound.** It already handles 13 commands correctly and establishes the right patterns. The remaining 35 commands can be added incrementally using the same patterns.

---

## 2. Command Inventory

### Category: Relational Core (Easy → SQL)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 1 | `search`/`source` | Search/Relation | `search` | `FROM table` | Easy |
| 2 | `where` | Filter | `where`/`search` | `WHERE condition` | Easy |
| 3 | `fields` (+include) | Project | `fields`/`table` | `SELECT col1, col2` | Easy |
| 4 | `fields` (-exclude) | Project | `fields -` | `SELECT * EXCEPT(col)` — needs schema | Moderate |
| 5 | `table` | Project | `table` | `SELECT col1, col2` | Easy |
| 6 | `sort` | Sort | `sort` | `ORDER BY` | Easy |
| 7 | `head` | Head/Limit | `head` | `LIMIT N OFFSET M` | Easy |
| 8 | `rename` | Rename | `rename` | `SELECT col AS alias, ...` — needs schema for `*` expansion | Moderate |
| 9 | `join` | Join | `join` | `JOIN ... ON ...` | Easy |
| 10 | `lookup` | Lookup | `lookup` | `LEFT JOIN` with COALESCE for REPLACE mode | Moderate |

### Category: Scalar Expression (Easy → SQL)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 11 | `eval` | Eval | `eval` | `SELECT *, expr AS alias` | Easy |
| 12 | `fieldformat` | Eval | N/A | Same as eval (display-time formatting) | Easy |
| 13 | `fillnull` | FillNull | `fillnull` | `SELECT COALESCE(col, value) AS col, ...` — needs schema | Moderate |
| 14 | `replace` | Replace | `replace` | `SELECT REGEXP_REPLACE(col, pattern, repl) AS col, ...` | Moderate |

### Category: Aggregation / Window (Easy-Moderate → SQL)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 15 | `stats` | Aggregation | `stats` | `SELECT agg(x), ... GROUP BY ...` | Easy |
| 16 | `eventstats` | Window | `eventstats` | `SELECT *, agg(x) OVER (PARTITION BY ...)` | Easy |
| 17 | `streamstats` | StreamWindow | `streamstats` | Window with `ROWS BETWEEN N PRECEDING AND CURRENT ROW` | Moderate-Hard |
| 18 | `top` | RareTopN | `top` | `GROUP BY + ORDER DESC + LIMIT` | Easy |
| 19 | `rare` | RareTopN | `rare` | `GROUP BY + ORDER ASC + LIMIT` | Easy |
| 20 | `trendline` | Trendline | `trendline` | Window AVG with `ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW` | Moderate |
| 21 | `chart` | Chart | `chart` | `GROUP BY span, BY_field` + PIVOT | Moderate |
| 22 | `timechart` | Chart | `timechart` | Same as chart with time-based span | Moderate |
| 23 | `bin`/`bucket` | Bin (5 subclasses) | `bin`/`bucket` | `FLOOR(field / span) * span` or `WIDTH_BUCKET(...)` | Moderate |

### Category: Deduplication / Filtering (Moderate → SQL)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 24 | `dedup` | Dedupe | `dedup` | `ROW_NUMBER() OVER (PARTITION BY ...) <= N` | Moderate |
| 25 | `dedup consecutive` | Dedupe | `dedup consecutive=true` | LAG-based comparison — no standard SQL | Hard |
| 26 | `regex` | Regex | `regex` | `WHERE REGEXP_LIKE(field, pattern)` | Easy |

### Category: Text Extraction (Moderate → UDF)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 27 | `parse` (regex) | Parse | `rex` | `SELECT *, REGEXP_EXTRACT(field, pattern, group) AS name` | Moderate |
| 28 | `grok` | Parse | `rex` (grok patterns) | Same as parse with grok-to-regex conversion | Moderate |
| 29 | `rex` (extract) | Rex | `rex` | `SELECT *, REGEXP_EXTRACT(field, pattern, group) AS name` | Moderate |
| 30 | `rex` (sed) | Rex | `rex mode=sed` | `SELECT REGEXP_REPLACE(field, pattern, repl) AS field` | Moderate |
| 31 | `spath` | SPath | `spath` | `SELECT JSON_VALUE(field, path) AS output` | Moderate |
| 32 | `patterns` | Patterns | N/A (PPL-specific) | Custom UDAF for pattern recognition | Hard |

### Category: Multi-Value / Expansion (Moderate → SQL)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 33 | `expand` | Expand | `mvexpand` | `CROSS JOIN UNNEST(field) AS t(val)` | Moderate |
| 34 | `mvexpand` | MvExpand | `mvexpand` | Same as expand with LIMIT on UNNEST | Moderate |
| 35 | `flatten` | Flatten | N/A | `LATERAL` + struct field expansion — needs schema | Moderate |
| 36 | `mvcombine` | MvCombine | `mvcombine` | `ARRAY_AGG(field)` or `LISTAGG(field, delim)` | Moderate |
| 37 | `nomv` | NoMv | `nomv` | `ARRAY_TO_STRING(field, ' ')` or first element | Moderate |

### Category: Pipeline / Set Operations (Moderate → SQL)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 38 | `append` | Append | `append` | `UNION ALL` | Easy |
| 39 | `multisearch` | Multisearch | `multisearch` | `UNION ALL` of multiple sources | Easy |
| 40 | `appendpipe` | AppendPipe | `appendpipe` | `UNION ALL` (self + sub-pipeline on self) | Moderate |
| 41 | `appendcol` | AppendCol | `appendcols` | Positional join via `ROW_NUMBER()` — non-standard | Hard |

### Category: Reshaping / Meta (Hard → Custom)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 42 | `transpose` | Transpose | `transpose` | UNPIVOT + PIVOT — needs schema for column names | Hard |
| 43 | `reverse` | Reverse | `reverse` | `ORDER BY` with reversed sort (needs original order) | Moderate |
| 44 | `addtotals` (row) | AddTotals | `addtotals` | `SELECT *, (col1+col2+...) AS Total` — needs schema | Moderate |
| 45 | `addcoltotals` | AddColTotals | `addcoltotals` | `UNION ALL` with `SELECT SUM(col1), SUM(col2), ...` | Moderate |
| 46 | `describe` | DescribeRelation | `fieldsummary` | `INFORMATION_SCHEMA` query or metadata function | Moderate |
| 47 | `show datasources` | (custom) | N/A | Metadata query | Easy |

### Category: ML / Graph (Not SQL)

| # | PPL Command | AST Class | SPL Equivalent | SQL Mapping | Difficulty |
|---|-------------|-----------|----------------|-------------|------------|
| 48 | `kmeans` | Kmeans | `fit kmeans` | Table function: `TABLE(KMEANS(input, params))` | Hard |
| 49 | `ad` | AD | `fit DensityFunction` | Table function: `TABLE(ANOMALY_DETECT(input, params))` | Hard |
| 50 | `ml` | ML | `fit`/`apply` | Table function: `TABLE(ML(input, params))` | Hard |
| 51 | `graphlookup` | GraphLookup | N/A | Recursive CTE or custom node | Hard |

---

## 3. Translation Patterns

### Pattern 1: Direct Clause Mapping (Easy)
PPL commands that map 1:1 to SQL clauses.

```
source=t | where x > 1 | fields a, b | sort a | head 10
→
SELECT a, b FROM t WHERE x > 1 ORDER BY a LIMIT 10
```

Commands: `source`, `where`, `fields`, `sort`, `head`, `table`

### Pattern 2: Projection Extension (Easy)
PPL commands that add computed columns to the SELECT list.

```
source=t | eval bonus = sal * 0.1
→
SELECT *, sal * 0.1 AS bonus FROM t
```

Commands: `eval`, `fieldformat`

### Pattern 3: Aggregation (Easy)
PPL stats maps directly to GROUP BY.

```
source=t | stats count() as c, avg(sal) as avg_sal by dept
→
SELECT COUNT(*) AS c, AVG(sal) AS avg_sal, dept FROM t GROUP BY dept
```

Commands: `stats`, `top`, `rare`

### Pattern 4: Window Function (Moderate)
PPL eventstats/streamstats map to window functions.

```
source=t | eventstats avg(sal) as avg_sal by dept
→
SELECT *, AVG(sal) OVER (PARTITION BY dept) AS avg_sal FROM t

source=t | streamstats count() as running_count
→
SELECT *, COUNT(*) OVER (ORDER BY _seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
         AS running_count FROM t
```

Commands: `eventstats`, `streamstats`, `trendline`

### Pattern 5: Subquery Wrapping for Pipe Chaining (Core Pattern)
When a PPL pipe adds a clause incompatible with the current SELECT (e.g., WHERE after GROUP BY), wrap the current query as a subquery.

```
source=t | stats count() as c by dept | where c > 5
→
SELECT * FROM (SELECT COUNT(*) AS c, dept FROM t GROUP BY dept) AS t0 WHERE c > 5
```

This is already implemented in `SelectBuilder.pipe()` and `withWhere()` — the builder auto-wraps when needed.

### Pattern 6: ROW_NUMBER Window for Dedup (Moderate)
```
source=t | dedup 1 dept
→
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY dept) AS _rn_
  FROM t WHERE dept IS NOT NULL
) AS t0 WHERE _rn_ <= 1
```

Already implemented in CalciteSqlNodeVisitor.

### Pattern 7: UDF/Custom Function (Moderate)
PPL functions that don't exist in standard SQL become SqlUnresolvedFunction calls.

```
source=t | eval extracted = regexp_extract(msg, '(\d+)', 1)
→
SELECT *, REGEXP_EXTRACT(msg, '(\d+)', 1) AS extracted FROM t
```

The function is represented as `SqlUnresolvedFunction("REGEXP_EXTRACT", ...)` in the SqlNode tree. Calcite's validator resolves it against the registered operator table.

### Pattern 8: UNNEST for Expansion (Moderate)
```
source=t | expand tags
→
SELECT t.*, u.val FROM t CROSS JOIN UNNEST(t.tags) AS u(val)
```

### Pattern 9: JOIN for Lookup (Moderate)
```
source=t | lookup vendors product_id replace dept as department
→
SELECT t.*, COALESCE(v.dept, t.department) AS department
FROM t LEFT JOIN vendors v ON t.product_id = v.product_id
```

### Pattern 10: UNION ALL for Append/Multisearch (Easy)
```
source=t | append [source=t2 | where x > 1]
→
SELECT * FROM t UNION ALL (SELECT * FROM t2 WHERE x > 1)
```

---

## 4. Analysis/Binding Requirements

### What Needs Schema Information

| Command | Schema Need | Why |
|---------|------------|-----|
| `fields -` (exclude) | Column list | Need all columns to produce `SELECT col1, col3, ...` excluding col2 |
| `rename` | Column list | Need `SELECT * EXCEPT renamed` + `renamed AS new_name` |
| `fillnull` (all fields) | Column list + types | Need to know which columns to COALESCE |
| `flatten` | Struct field types | Need to know nested field names to expand |
| `addtotals` (row) | Numeric columns | Need to know which columns to sum across |
| `addcoltotals` | Numeric columns | Need to know which columns to aggregate |
| `transpose` | All columns | Need column names to become row values |
| `replace` (all fields) | Column list | Need to know which fields to apply replacement to |
| `lookup` (REPLACE mode) | Output columns | Need to know which columns exist for COALESCE logic |
| `bin` (auto/default) | Field type + range | Need min/max for auto-bucketing |

### Proposed Minimal Analysis Phase

The V4 analysis phase should be a **lightweight symbol table + type catalog**, not a full semantic analyzer:

```
PPL AST
  → SymbolResolver (resolve table names → schema, resolve field names → types)
  → CalciteSqlNodeVisitor (build SqlNode tree using resolved schema where needed)
  → Calcite SqlValidator (full type checking + validation)
  → Calcite SqlToRelConverter (SqlNode → RelNode)
```

**SymbolResolver responsibilities:**
1. Resolve table/index names to their schemas (column names + types)
2. Provide column list for `fields -`, `rename`, `fillnull`, `addtotals`, `flatten`
3. Provide type information for `bin` auto-bucketing
4. Track field additions/removals through the pipeline (eval adds fields, fields removes them)

**Key design principle:** Build SqlNode with unresolved identifiers wherever possible. Only use the SymbolResolver for commands that structurally require schema knowledge (field exclusion, struct expansion, etc.). Let Calcite's SqlValidator handle the rest.

**Can unresolved SqlNode be built first?** Yes, for ~80% of commands. `SqlUnresolvedFunction` already demonstrates this pattern — the function name is recorded in the SqlNode tree and resolved during validation. Field references work the same way — `SqlIdentifier("col")` doesn't need type information at construction time.

**Which commands require bound references?**
- `fields -` (exclude mode) — must know all columns to exclude some
- `rename` — must know all columns to pass through non-renamed ones
- `fillnull` without explicit field list — must know all columns
- `flatten` — must know struct field names
- `addtotals`/`addcoltotals` — must know numeric columns

For these commands, the SymbolResolver provides the column list, and the visitor generates the appropriate SELECT list.

### Nested Field / MAP Path Resolution

OpenSearch fields can be nested (e.g., `address.city`). In SqlNode:
- Simple nested: `SqlIdentifier(["address", "city"])` — Calcite handles this as a compound identifier
- JSON path: `JSON_VALUE(field, '$.address.city')` — use JSON function
- MAP access: `field['key']` — use `SqlStdOperatorTable.ITEM` operator

The SymbolResolver should classify field references:
1. Top-level field → `SqlIdentifier("field")`
2. Nested object field → `SqlIdentifier(["parent", "child"])`
3. Dynamic/MAP field → `ITEM(field, 'key')` call

---

## 5. Function Strategy

### Classification of PPL Functions

Based on the PPLFuncImpTable analysis (~200+ functions):

**Bucket 1: Standard SQL functions (direct SqlStdOperatorTable mapping)**
- Math: `ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POW`, `MOD`, `LOG`, `LOG10`, `LOG2`, `EXP`, `SIGN`
- String: `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`, `LENGTH`, `SUBSTRING`, `CONCAT`, `REPLACE`, `REVERSE`, `POSITION`
- Comparison: `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`
- Aggregate: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`
- Date: `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `EXTRACT`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`
- Conditional: `CASE`, `IF` (→ CASE), `IFNULL` (→ COALESCE)

**Bucket 2: Maps to existing Calcite operator with name mapping**
- `LEN` → `CHAR_LENGTH`
- `SUBSTR` → `SUBSTRING`
- `ISNULL` → `IS NULL`
- `ISNOTNULL` → `IS NOT NULL`
- `DC` / `DISTINCT_COUNT` → `COUNT(DISTINCT ...)`

**Bucket 3: Needs SqlOperator wrapper (PPL-specific but expressible)**
- `REGEXP_EXTRACT` — custom SqlFunction wrapping regex extraction
- `REGEXP_REPLACE` — exists in some Calcite dialects
- `JSON_EXTRACT` / `JSON_VALUE` — Calcite has JSON support
- `STRFTIME` / `DATE_FORMAT` — custom SqlFunction
- `EARLIEST` / `LATEST` — custom time filter functions
- `SPAN` — custom binning function
- `LIKE` / `MATCH` / `QUERY_STRING` — OpenSearch-specific search functions

**Bucket 4: Custom scalar UDF needed**
- `CIDRMATCH` — IP CIDR matching
- `MD5`, `SHA1`, `SHA2` — cryptographic hashes
- `TYPEOF` — runtime type inspection
- `GROK` — grok pattern matching
- Multi-value functions: `MV_FIRST`, `MV_LAST`, `MV_COUNT`, `MV_SORT`, `MV_SLICE`, `MV_ZIP`, `MV_APPEND`, `MV_DEDUP`

**Bucket 5: Custom UDAF needed**
- `PERCENTILE_APPROX` — approximate percentile (already exists as custom UDAF)
- `FIRST` / `LAST` — first/last value in group (already exists)
- `LIST` / `VALUES` — collect values into array (already exists)
- `TAKE` — sample N values (already exists)
- `LOG_PATTERN_AGG` — pattern recognition aggregate (already exists)

**Bucket 6: Table function / UDTF**
- `KMEANS(input, params)` → `TABLE(KMEANS(...))`
- `ANOMALY_DETECT(input, params)` → `TABLE(AD(...))`
- `ML(input, params)` → `TABLE(ML(...))`

### V4 Function Registration Strategy

For the SqlNode path, functions should be registered as `SqlOperator` instances in a custom `SqlOperatorTable`:

```java
public class PplSqlOperatorTable extends ListSqlOperatorTable {
  // Bucket 1: delegate to SqlStdOperatorTable (already there)
  // Bucket 2: create aliases
  public static final SqlFunction LEN = SqlBasicFunction.create("LEN",
      ReturnTypes.INTEGER, OperandTypes.STRING, SqlFunctionCategory.STRING);
  // Bucket 3-4: create custom SqlFunction
  public static final SqlFunction REGEXP_EXTRACT = SqlBasicFunction.create("REGEXP_EXTRACT",
      ReturnTypes.VARCHAR_2000, OperandTypes.STRING_STRING_INTEGER,
      SqlFunctionCategory.USER_DEFINED_FUNCTION);
  // Bucket 5: create SqlAggFunction
  public static final SqlAggFunction PERCENTILE_APPROX =
      SqlBasicAggFunction.create("PERCENTILE_APPROX", ...);
}
```

The CalciteSqlExpressionVisitor already uses `SqlUnresolvedFunction` for unknown functions — this is the right approach. During Calcite validation, unresolved functions are matched against the operator table.

---

## 6. Hard Cases / Breaking Changes

### 6.1 `dedup consecutive=true`
**Semantics:** Remove consecutive duplicate events (not all duplicates — only adjacent ones).
**Why hard:** Requires comparing each row with its predecessor, which is order-dependent stateful processing.
**SqlNode approach:** Use `LAG()` window function:
```sql
SELECT * FROM (
  SELECT *, LAG(field) OVER (ORDER BY _seq) AS _prev_field FROM t
) WHERE field != _prev_field OR _prev_field IS NULL
```
**Assessment:** Expressible with window functions, but requires a defined ordering column. If no ordering is defined, this is a **V4 breaking change** — consecutive dedup requires explicit sort order.

### 6.2 `streamstats` with `reset_before`/`reset_after`
**Semantics:** Reset the running window when a condition is met.
**Why hard:** SQL window functions don't support conditional frame resets. The window frame is static.
**SqlNode approach:** Two-pass:
1. Compute a "group ID" using `SUM(CASE WHEN reset_condition THEN 1 ELSE 0 END) OVER (ORDER BY _seq)` to partition the stream into segments
2. Compute the window aggregate within each segment
**Assessment:** Expressible but complex. **Moderate V4 extension.**

### 6.3 `transpose`
**Semantics:** Convert rows to columns and columns to rows.
**Why hard:** Output column names depend on data values, not schema. Standard SQL PIVOT/UNPIVOT requires known column names at query time.
**Assessment:** **V4 breaking change candidate.** Dynamic transpose is fundamentally non-SQL.

### 6.4 `appendcol`
**Semantics:** Run a sub-pipeline and join its columns positionally (by row number) to the main results.
**Why hard:** SQL has no concept of "positional join."
**SqlNode approach:**
```sql
SELECT a.*, b.* FROM
  (SELECT *, ROW_NUMBER() OVER () AS _rn FROM main_query) a
  LEFT JOIN
  (SELECT *, ROW_NUMBER() OVER () AS _rn FROM sub_query) b
  ON a._rn = b._rn
```
**Assessment:** Expressible but semantically fragile — ROW_NUMBER() without ORDER BY is non-deterministic.

### 6.5 `graphlookup`
**Semantics:** Recursive graph traversal.
**SqlNode approach:** Recursive CTE:
```sql
WITH RECURSIVE graph AS (
  SELECT start_field AS node, 0 AS depth FROM source
  UNION ALL
  SELECT lookup.to_field, graph.depth + 1
  FROM graph JOIN lookup ON graph.node = lookup.from_field
  WHERE graph.depth < max_depth
)
SELECT * FROM source JOIN graph ON ...
```
**Assessment:** Basic cases work via recursive CTE. Advanced options (direction=BI, supportArray) need custom handling.

### 6.6 ML Commands (`kmeans`, `ad`, `ml`)
**Semantics:** Run machine learning algorithms on the data.
**SqlNode approach:** Table functions: `SELECT * FROM TABLE(KMEANS(SELECT * FROM source, centroids => 3))`
**Assessment:** Table function wrappers are the standard Calcite pattern. **V4 extension point.**

### 6.7 `patterns` (brain/simple_pattern)
**Semantics:** Log pattern recognition.
**SqlNode approach:**
- Label mode: `SELECT *, LOG_PATTERN(field) AS patterns_field FROM t` — custom UDF
- Aggregation mode: `SELECT LOG_PATTERN_AGG(field) AS pattern, COUNT(*) FROM t GROUP BY LOG_PATTERN_AGG(field)` — custom UDAF
**Assessment:** Existing `LogPatternAggFunction` UDAF handles this. **Moderate extension.**

---

## 7. Compatibility Matrix

| PPL Command | SPL Equivalent | SQL Shape | Schema Needed? | Custom Op/UDF? | Breaking Risk | Confidence | V4 Status |
|-------------|---------------|-----------|----------------|-----------------|---------------|------------|-----------|
| source/search | search | FROM | No | No | None | ★★★★★ | ✅ Done |
| where | where | WHERE | No | No | None | ★★★★★ | ✅ Done |
| fields + | fields/table | SELECT list | No | No | None | ★★★★★ | ✅ Done |
| fields - | fields - | SELECT (all except) | Yes (columns) | No | None | ★★★★☆ | Needs schema |
| eval | eval | SELECT *, expr AS alias | No | No | None | ★★★★★ | ✅ Done |
| fieldformat | N/A | Same as eval | No | No | None | ★★★★★ | Easy |
| stats | stats | GROUP BY + aggs | No | No | None | ★★★★★ | ✅ Done |
| eventstats | eventstats | Window agg (OVER) | No | No | None | ★★★★★ | Easy |
| streamstats | streamstats | Window with frame | No | Maybe (reset) | Low-Med | ★★★★☆ | Moderate |
| sort | sort | ORDER BY | No | No | None | ★★★★★ | ✅ Done |
| head | head | LIMIT/OFFSET | No | No | None | ★★★★★ | ✅ Done |
| dedup | dedup | ROW_NUMBER window | No | No | Low | ★★★★★ | ✅ Done |
| dedup consecutive | dedup consecutive | LAG window | No | No | Medium | ★★★☆☆ | Hard |
| top | top | GROUP BY + ORDER DESC + LIMIT | No | No | None | ★★★★★ | Easy |
| rare | rare | GROUP BY + ORDER ASC + LIMIT | No | No | None | ★★★★★ | Easy |
| rename | rename | SELECT col AS alias | Yes (columns) | No | None | ★★★★☆ | Needs schema |
| join | join | JOIN ... ON | No | No | None | ★★★★★ | ✅ Done |
| lookup | lookup | LEFT JOIN + COALESCE | Yes (output cols) | No | None | ★★★★☆ | Moderate |
| parse (regex) | rex | REGEXP_EXTRACT UDF | No | Yes (UDF) | None | ★★★★☆ | Moderate |
| grok | rex | GROK_EXTRACT UDF | No | Yes (UDF) | None | ★★★★☆ | Moderate |
| rex (extract) | rex | REGEXP_EXTRACT UDF | No | Yes (UDF) | None | ★★★★☆ | Moderate |
| rex (sed) | rex mode=sed | REGEXP_REPLACE | No | Yes (UDF) | None | ★★★★☆ | Moderate |
| spath | spath | JSON_VALUE | No | Yes (UDF) | None | ★★★★☆ | Moderate |
| patterns | N/A | Custom UDAF | No | Yes (UDAF) | None | ★★★☆☆ | Hard |
| regex | regex | WHERE REGEXP_LIKE | No | Yes (UDF) | None | ★★★★★ | Easy |
| expand | mvexpand | CROSS JOIN UNNEST | No | No | None | ★★★★☆ | Moderate |
| mvexpand | mvexpand | CROSS JOIN UNNEST + LIMIT | No | No | None | ★★★★☆ | Moderate |
| flatten | N/A | LATERAL + struct expand | Yes (struct fields) | No | None | ★★★☆☆ | Needs schema |
| mvcombine | mvcombine | ARRAY_AGG / LISTAGG | No | Maybe | None | ★★★★☆ | Moderate |
| nomv | nomv | ARRAY_TO_STRING | No | Maybe | None | ★★★★☆ | Moderate |
| fillnull | fillnull | COALESCE per column | Yes (columns) | No | None | ★★★★☆ | Needs schema |
| replace | replace | REGEXP_REPLACE per column | Yes (columns) | Yes (UDF) | None | ★★★★☆ | Needs schema |
| bin/bucket | bin/bucket | FLOOR/WIDTH_BUCKET | Maybe (auto) | Yes (UDF) | None | ★★★★☆ | Moderate |
| append | append | UNION ALL | No | No | None | ★★★★★ | Easy |
| multisearch | multisearch | UNION ALL | No | No | None | ★★★★★ | Easy |
| appendpipe | appendpipe | UNION ALL (self + sub) | No | No | None | ★★★★☆ | Moderate |
| appendcol | appendcols | ROW_NUMBER + JOIN | No | No | Medium | ★★★☆☆ | Hard |
| chart | chart | GROUP BY + PIVOT | No | Maybe | Low | ★★★★☆ | Moderate |
| timechart | timechart | GROUP BY time + PIVOT | No | Maybe | Low | ★★★★☆ | Moderate |
| trendline | trendline | Window AVG | No | No | None | ★★★★☆ | Moderate |
| addtotals (row) | addtotals | SELECT *, sum-of-cols | Yes (numeric cols) | No | None | ★★★★☆ | Needs schema |
| addcoltotals | addcoltotals | UNION ALL + SUM | Yes (numeric cols) | No | Low | ★★★☆☆ | Needs schema |
| transpose | transpose | UNPIVOT + PIVOT | Yes (all cols) | No | High | ★★☆☆☆ | Breaking |
| reverse | reverse | ORDER BY (reversed) | No | No | Low | ★★★★☆ | Moderate |
| describe | fieldsummary | INFORMATION_SCHEMA | Yes (metadata) | No | None | ★★★★☆ | Moderate |
| show datasources | N/A | Metadata query | No | No | None | ★★★★★ | Easy |
| kmeans | fit kmeans | TABLE(KMEANS(...)) | No | Yes (UDTF) | Low | ★★★☆☆ | Extension |
| ad | fit DensityFunction | TABLE(AD(...)) | No | Yes (UDTF) | Low | ★★★☆☆ | Extension |
| ml | fit/apply | TABLE(ML(...)) | No | Yes (UDTF) | Low | ★★★☆☆ | Extension |
| graphlookup | N/A | Recursive CTE | No | Maybe | Medium | ★★★☆☆ | Hard |

---

## 8. Recommended V4 Architecture

### Proposed End-to-End Pipeline

```
PPL Text
  → PPL Parser (ANTLR)
  → PPL AST (UnresolvedPlan tree)
  → [Optional] SymbolResolver (lightweight schema binding for ~8 commands that need it)
  → CalciteSqlNodeVisitor (PPL AST → SqlNode tree)
  → Calcite SqlValidator (type checking, function resolution)
  → Calcite SqlToRelConverter (SqlNode → RelNode)
  → Calcite Optimizer (rule-based + cost-based optimization)
  → Physical Plan → Execution
```

### Key Extension Points

1. **PplSqlOperatorTable** — register all PPL functions as SqlOperator/SqlFunction/SqlAggFunction
2. **Custom SqlToRelConverter** — override `convertExtendedExpression()` for PPL-specific SqlNode types
3. **Custom SqlValidator** — extend validation for PPL-specific semantics
4. **SymbolResolver** — lightweight schema catalog for the ~8 commands that need column lists
5. **SqlNodeDsl** — continue extending the fluent builder for new patterns (UNNEST, PIVOT, recursive CTE)

### Implementation Priority

**Phase 1 — Core (already mostly done):**
source, where, fields+, eval, stats, sort, head, dedup, join

**Phase 2 — Window & Aggregation:**
eventstats, streamstats (basic), trendline, top, rare, bin

**Phase 3 — Schema-Dependent Commands:**
fields-, rename, fillnull, replace, addtotals, lookup (REPLACE), flatten

**Phase 4 — Text Extraction & Expansion:**
parse, grok, rex, spath, regex, expand, mvexpand, mvcombine, nomv, patterns

**Phase 5 — Pipeline Operations:**
append, multisearch, appendpipe, appendcol, chart, timechart

**Phase 6 — Hard Cases:**
transpose, graphlookup, ML commands, dedup consecutive, streamstats reset

---

## 9. Example Mappings

### 9.1 source + where + fields + sort + head
```
PPL:    source=EMP | where SAL > 1000 | fields EMPNO, ENAME | sort EMPNO | head 5
SQL:    SELECT EMPNO, ENAME FROM EMP WHERE SAL > 1000 ORDER BY EMPNO LIMIT 5
```

### 9.2 eval (computed columns)
```
PPL:    source=EMP | eval bonus = SAL * 0.1, total = SAL + bonus
SQL:    SELECT *, SAL * 0.1 AS bonus, SAL + bonus AS total FROM EMP
```

### 9.3 stats (aggregation)
```
PPL:    source=EMP | stats count() as cnt, avg(SAL) as avg_sal by DEPTNO
SQL:    SELECT COUNT(*) AS cnt, AVG(SAL) AS avg_sal, DEPTNO FROM EMP GROUP BY DEPTNO
```

### 9.4 eventstats (window aggregation)
```
PPL:    source=EMP | eventstats avg(SAL) as dept_avg by DEPTNO
SQL:    SELECT *, AVG(SAL) OVER (PARTITION BY DEPTNO) AS dept_avg FROM EMP
```

### 9.5 streamstats (running aggregate)
```
PPL:    source=EMP | sort HIREDATE | streamstats count() as running_count
SQL:    SELECT *, COUNT(*) OVER (ORDER BY HIREDATE
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
        FROM EMP
```

### 9.6 dedup
```
PPL:    source=EMP | dedup 1 DEPTNO
SQL:    SELECT * FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY DEPTNO) AS _rn_
          FROM EMP WHERE DEPTNO IS NOT NULL
        ) t0 WHERE _rn_ <= 1
```

### 9.7 top / rare
```
PPL:    source=EMP | top 3 DEPTNO
SQL:    SELECT DEPTNO, COUNT(*) AS count FROM EMP
        GROUP BY DEPTNO ORDER BY count DESC LIMIT 3

PPL:    source=EMP | rare 3 JOB
SQL:    SELECT JOB, COUNT(*) AS count FROM EMP
        GROUP BY JOB ORDER BY count ASC LIMIT 3
```

### 9.8 trendline (moving average)
```
PPL:    source=EMP | sort HIREDATE | trendline sma(3, SAL) as moving_avg
SQL:    SELECT *, AVG(SAL) OVER (ORDER BY HIREDATE
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
        FROM EMP
```

### 9.9 rex (extract mode)
```
PPL:    source=logs | rex msg "(?<code>\d{3}) (?<text>\w+)"
SQL:    SELECT *, REGEXP_EXTRACT(msg, '(?<code>\d{3}) (?<text>\w+)', 1) AS code,
                  REGEXP_EXTRACT(msg, '(?<code>\d{3}) (?<text>\w+)', 2) AS text
        FROM logs
```

### 9.10 expand (multi-value expansion)
```
PPL:    source=t | expand tags
SQL:    SELECT t.*, u.val AS tags FROM t CROSS JOIN UNNEST(t.tags) AS u(val)
```

### 9.11 append / multisearch
```
PPL:    source=t1 | append [source=t2 | where x > 1]
SQL:    SELECT * FROM t1 UNION ALL (SELECT * FROM t2 WHERE x > 1)

PPL:    source=t1 | source=t2 | source=t3
SQL:    SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION ALL SELECT * FROM t3
```

### 9.12 join / lookup
```
PPL:    source=orders | join left=o right=c ON o.cust_id = c.id customers
SQL:    SELECT * FROM orders o LEFT JOIN customers c ON o.cust_id = c.id

PPL:    source=orders | lookup products product_id
SQL:    SELECT o.*, p.* FROM orders o LEFT JOIN products p ON o.product_id = p.product_id
```

### 9.13 fillnull (with explicit fields)
```
PPL:    source=EMP | fillnull with 0 in SAL, COMM
SQL:    SELECT *, COALESCE(SAL, 0) AS SAL, COALESCE(COMM, 0) AS COMM FROM EMP
        -- Note: needs schema to know which other columns to pass through
```

### 9.14 rename
```
PPL:    source=EMP | rename SAL as SALARY, DEPTNO as DEPARTMENT
SQL:    SELECT EMPNO, ENAME, JOB, MGR, HIREDATE, SAL AS SALARY, COMM, DEPTNO AS DEPARTMENT
        FROM EMP
        -- Note: needs schema to enumerate all columns
```

### 9.15 chart / timechart
```
PPL:    source=logs | timechart span=1h count() by status
SQL:    SELECT FLOOR(EXTRACT(EPOCH FROM timestamp) / 3600) * 3600 AS time_bucket,
               status, COUNT(*) AS count
        FROM logs GROUP BY time_bucket, status
        -- PIVOT to get status values as columns (optional)
```

---

## 10. Deep Dive: Schema-Dependent Commands and the `SELECT * EXCEPT/REPLACE` Pattern

### The Core Question

Eight PPL commands require schema information during translation. Are they all essentially doing `SELECT * EXCEPT(col)` or `SELECT * REPLACE(expr AS col)` — patterns that could be served by a single SQL extension?

### Analysis of Each Schema-Dependent Command

After examining the V3 CalciteRelNodeVisitor implementations, three distinct sub-patterns emerge:

#### Sub-Pattern A: `SELECT * EXCEPT(columns)` — Drop specific columns

**Commands:** `fields -` (exclude), `lookup` (remove duplicate mapping fields)

These commands need the column list only to **remove** specific columns. The V3 implementation uses `relBuilder.projectExcept()` directly.

```java
// V3 visitProject (exclude mode):
context.relBuilder.projectExcept(expandedFields);

// V3 visitLookup (remove mapping fields after join):
context.relBuilder.projectExcept(toBeRemovedFields);
```

**V4 SqlNode approach:** This maps directly to BigQuery-style `SELECT * EXCEPT(col1, col2)`. Calcite does NOT natively support `EXCEPT` in SELECT at the SqlNode level, but there are two clean options:

1. **Custom SqlSelectExcept node** — a SqlNode subclass that records the exclusion list and expands during validation
2. **Defer to SqlValidator** — build `SELECT *` and let a custom validator expand `*` while excluding specified columns

**Verdict: These do NOT need schema at SqlNode construction time if we use a deferred-expansion approach.**

#### Sub-Pattern B: `SELECT * REPLACE(f(col) AS col)` — Transform specific columns in-place

**Commands:** `fillnull`, `replace`, `rename`

These commands iterate ALL columns and conditionally transform some while passing others through unchanged. The V3 pattern is:

```java
// V3 visitFillNull:
for (RelDataTypeField field : fieldsList) {
  if (matchesTargetField(field)) {
    projects.add(COALESCE(fieldRef, replacement));  // transform
  } else {
    projects.add(fieldRef);  // pass through
  }
}
context.relBuilder.project(projects);

// V3 visitReplace:
for (String fieldName : allFieldNames) {
  if (matchesTargetField(fieldName)) {
    projects.add(REGEXP_REPLACE(fieldRef, pattern, repl));  // transform
  } else {
    projects.add(fieldRef);  // pass through
  }
}
context.relBuilder.project(projects);

// V3 visitRename:
List<String> newNames = new ArrayList<>(originalNames);
for (Map renameMap : node.getRenameList()) {
  int idx = newNames.indexOf(sourceName);
  newNames.set(idx, targetName);
}
context.relBuilder.rename(newNames);
```

**V4 SqlNode approach:** This maps to BigQuery-style `SELECT * REPLACE(COALESCE(col, 0) AS col)`. Again, Calcite doesn't natively support this, but the same two options apply:

1. **Custom SqlSelectReplace node** — records the replacement map `{col → expr}` and expands during validation
2. **Defer to SqlValidator** — build a placeholder and expand `*` with replacements during validation

For `rename`, it's even simpler — it's `SELECT * REPLACE(col AS new_name)`, which is just aliasing.

**Verdict: These do NOT need schema at SqlNode construction time if we use a deferred-expansion approach.**

#### Sub-Pattern C: Schema-Dependent Structural Expansion — Need actual type/field knowledge

**Commands:** `flatten`, `addtotals`, `addcoltotals`

These commands need to **inspect the actual schema structure** to determine what to generate:

```java
// V3 visitFlatten — needs to discover struct sub-fields:
List<RelDataTypeField> fieldsToExpand = relBuilder.peek().getRowType().getFieldList()
    .stream()
    .filter(f -> f.getName().startsWith(fieldName + "."))
    .toList();
// Then creates aliased references for each discovered sub-field

// V3 visitAddTotals — needs to identify numeric columns:
// Iterates all fields, checks if type is numeric, builds SUM expression across numeric cols
// SELECT *, (numeric_col1 + numeric_col2 + ...) AS Total FROM t
```

For `flatten`, the output columns depend entirely on the struct's internal field names — you can't know what `flatten address` produces without knowing that `address` has sub-fields `city`, `state`, `zip`.

For `addtotals`, you need to know which columns are numeric to build the row-total expression `col1 + col2 + col3`.

**Verdict: These genuinely need schema information. They cannot be deferred.**

### Summary: The `SELECT * EXCEPT/REPLACE` Unification

| Command | Pattern | Needs Schema at SqlNode Time? | Can Defer? |
|---------|---------|-------------------------------|------------|
| `fields -` | `SELECT * EXCEPT(col1, col2)` | No | ✅ Yes — custom node or validator expansion |
| `rename` | `SELECT * REPLACE(col AS new_name)` | No | ✅ Yes — custom node or validator expansion |
| `fillnull` (explicit fields) | `SELECT * REPLACE(COALESCE(col, val) AS col)` | No | ✅ Yes — custom node or validator expansion |
| `fillnull` (all fields) | `SELECT * REPLACE(COALESCE(col, val) AS col)` for ALL cols | Yes (need column list) | ⚠️ Partially — need to know "all columns" |
| `replace` (explicit fields) | `SELECT * REPLACE(REGEXP_REPLACE(col, p, r) AS col)` | No | ✅ Yes — custom node or validator expansion |
| `replace` (wildcard fields) | Same but with wildcard matching | Yes (need column list for wildcard) | ⚠️ Partially |
| `lookup` (REPLACE mode) | `JOIN` + `SELECT * EXCEPT(dup_cols)` | No | ✅ Yes — EXCEPT handles it |
| `flatten` | Struct field expansion | **Yes** (need struct field names) | ❌ No |
| `addtotals` (row) | `SELECT *, (num_col1 + num_col2 + ...) AS Total` | **Yes** (need numeric column list) | ❌ No |
| `addcoltotals` | `UNION ALL` with `SELECT SUM(num_col1), ...` | **Yes** (need numeric column list) | ❌ No |

### Key Insight

**6 out of 10 schema-dependent cases are indeed `SELECT * EXCEPT/REPLACE` patterns** that can be handled without schema at SqlNode construction time, using deferred expansion during Calcite validation.

Only 3 commands (`flatten`, `addtotals`, `addcoltotals`) genuinely need schema information upfront. And 1 command (`fillnull` with "all fields" mode) needs it only in the "apply to all columns" variant.

### Proposed V4 Design: `SqlPplStar` Custom Node

Instead of resolving schema before SqlNode construction, introduce a custom SqlNode that captures the intent and defers expansion:

```java
/**
 * A PPL-aware star expansion that supports EXCEPT and REPLACE semantics.
 * Expanded during Calcite validation (SqlValidator) into concrete column references.
 *
 * Examples:
 *   fields - col1, col2       → SqlPplStar.except(["col1", "col2"])
 *   rename col1 as new_name   → SqlPplStar.replace({col1 → AS(col1, "new_name")})
 *   fillnull with 0 in col1   → SqlPplStar.replace({col1 → COALESCE(col1, 0)})
 *   replace "a" with "b" in f → SqlPplStar.replace({f → REGEXP_REPLACE(f, "a", "b")})
 */
public class SqlPplStar extends SqlCall {
    private final List<String> exceptColumns;           // columns to exclude
    private final Map<String, SqlNode> replaceExprs;    // column → replacement expression

    // During validation, SqlValidator expands this into concrete SELECT list
    // by reading the input relation's row type
}
```

This approach:
1. Keeps SqlNode construction schema-free for 6/10 commands
2. Leverages Calcite's existing validation phase for schema resolution
3. Follows the same pattern as Flink's `ExtendedSqlNode` for custom validation
4. Reduces the SymbolResolver to only handle `flatten`, `addtotals`, `addcoltotals`

### For the 3 Truly Schema-Dependent Commands

For `flatten`, `addtotals`, and `addcoltotals`, the SymbolResolver must provide:
- `flatten`: struct field names for the target field
- `addtotals`: list of numeric column names
- `addcoltotals`: list of numeric column names

These can be resolved with a single schema lookup per command — the SymbolResolver doesn't need to be a full semantic analyzer. It just needs:

```java
interface SchemaProvider {
    List<String> getColumnNames(String tableName);
    List<String> getNumericColumnNames(String tableName);
    Map<String, List<String>> getStructFields(String tableName, String fieldName);
}
```

This is a thin wrapper around Calcite's `SchemaPlus` / `Table.getRowType()` that can be queried on-demand during SqlNode construction for just these 3 commands.
