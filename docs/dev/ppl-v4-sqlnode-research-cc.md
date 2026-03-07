# PPL V4 Architecture Research: Reintroducing SqlNode into the PPL Execution Path

## Context

The current PPL V3 execution path is:
```
PPL query → ANTLR Parser → PPL AST → CalciteRelNodeVisitor → Calcite RelNode → Execution
```

The `CalciteRelNodeVisitor` (3871 lines, 43 visit methods) directly converts PPL AST to RelNode with no intermediate SqlNode layer and no dedicated analysis phase. This monolithic visitor has grown complex and makes architectural evolution difficult.

The project **already has** a working SqlNode→RelNode path for ANSI SQL mode via `UnifiedQueryPlanner.planWithCalcite()` (lines 119-130):
```java
Planner planner = Frameworks.getPlanner(config);
SqlNode parsed = planner.parse(query);
SqlNode validated = planner.validate(parsed);
RelRoot relRoot = planner.rel(validated);
```

All 200+ PPL functions are **already registered** as `SqlOperator` instances via `PPLBuiltinOperators` (extends `ReflectiveSqlOperatorTable`), chained with `SqlStdOperatorTable` in `UnifiedQueryContext.buildFrameworkConfig()`.

**Goal:** Determine whether PPL V4 can center on `PPL AST → SqlNode → RelNode`, reusing the existing Calcite validation and conversion infrastructure.

---

## 1. Executive Summary

**Can PPL V4 reasonably center on SqlNode? Yes, with high confidence.**

- **83% of commands (40/48)** are Easy or Moderate difficulty with 85%+ confidence
- **8% (4/48)** are Hard but fully expressible in SQL (chart, timechart, transpose, streamstats-reset)
- **8% (4/48)** need table-valued function (TVF) representation (graphlookup, kmeans, ad, ml)
- **Zero commands** are fundamentally impossible to represent in SqlNode

The infrastructure is ready: all PPL functions are already SqlOperators, the SqlNode→RelNode pipeline exists, and Calcite's SqlValidator already knows how to validate PPL function calls.

**Major blockers:** None that are true blockers. GraphLookup and ML commands need TVF wrappers. Some commands need a lightweight schema tracker for field enumeration.

**Likely breaking changes:** ML commands (currently unsupported in V3 too), consecutive dedup (currently unsupported), and some edge cases in chart/timechart column-split limiting.

---

## 2. Command Inventory & Compatibility Matrix

| # | Command | Category | SQL Shape | Difficulty | Schema Dep | Custom Op Needed | Breaking Risk | Confidence |
|---|---------|----------|-----------|------------|------------|------------------|---------------|------------|
| 1 | search/source | Core | `FROM table [WHERE query_string()]` | Easy | Table name | query_string (exists) | None | 99% |
| 2 | where/filter | Core | `WHERE clause` | Easy | None | None | None | 99% |
| 3 | fields | Core | `SELECT col_list` | Easy | Wildcard expansion | None | None | 95% |
| 4 | table | Core | `SELECT col_list` | Easy | Same as fields | None | None | 95% |
| 5 | rename | Core | `SELECT col AS alias` | Easy | Field list | None | None | 95% |
| 6 | eval | Core | `SELECT *, expr AS alias` | Easy | Override detection | None | None | 95% |
| 7 | sort | Core | `ORDER BY` | Easy | None | None | None | 99% |
| 8 | head | Core | `LIMIT/OFFSET` | Easy | None | None | None | 99% |
| 9 | reverse | Core | `ORDER BY rn DESC` | Moderate | Prior sort | ROW_NUMBER | None | 90% |
| 10 | stats | Aggregation | `SELECT agg GROUP BY` | Moderate | Span resolution | SPAN (exists) | None | 95% |
| 11 | eventstats | Aggregation | `Window agg OVER (PARTITION BY)` | Moderate | Group fields | None | None | 95% |
| 12 | streamstats | Aggregation | `Window ROWS BETWEEN` + segments | Hard | Window frame, reset | ROW_NUMBER | Low | 85% |
| 13 | bin | Aggregation | `SPAN/WIDTH_BUCKET expression` | Easy | Field type | SPAN (exists) | None | 95% |
| 14 | chart | Aggregation | `GROUP BY + PIVOT pattern` | Hard | Agg func, splits | ROW_NUMBER | Low | 80% |
| 15 | timechart | Aggregation | `Time-bucket GROUP BY` | Hard | Time field type | SPAN (exists) | Low | 80% |
| 16 | trendline | Aggregation | `Window AVG + CASE` | Moderate | Data field | None | None | 90% |
| 17 | dedup | Dedup/TopN | `ROW_NUMBER() OVER PARTITION` | Moderate | Dedup fields | ROW_NUMBER | None | 95% |
| 18 | top | Dedup/TopN | `GROUP BY + COUNT + ROW_NUMBER` | Moderate | Fields | ROW_NUMBER | None | 95% |
| 19 | rare | Dedup/TopN | `GROUP BY + COUNT + ROW_NUMBER ASC` | Moderate | Fields | ROW_NUMBER | None | 95% |
| 20 | parse | Text | `SELECT *, REX_EXTRACT() AS field` | Moderate | Pattern fields | REX_EXTRACT (exists) | None | 90% |
| 21 | grok | Text | `SELECT *, GROK() AS field` | Moderate | Pattern fields | GROK (exists) | None | 90% |
| 22 | rex | Text | `SELECT *, REX_EXTRACT() AS field` | Moderate | Pattern fields | REX_EXTRACT (exists) | None | 90% |
| 23 | regex | Text | `WHERE REGEXP()` | Easy | Field type | REGEXP (exists) | None | 95% |
| 24 | spath | Text | Rewrites to eval + JSON_EXTRACT | Easy | None | JSON_EXTRACT (exists) | None | 95% |
| 25 | patterns | Text | Agg/Window + PATTERN UDF | Hard | Pattern mode | INTERNAL_PATTERN (exists) | Low | 75% |
| 26 | join | Join | `SQL JOIN (all types)` | Moderate | Column dedup | None | None | 90% |
| 27 | lookup | Join | `LEFT JOIN + COALESCE` | Moderate | Lookup schema | None | None | 90% |
| 28 | graphlookup | Join | `TABLE(GRAPH_LOOKUP(...))` TVF | Hard/TVF | Both schemas | GRAPH_LOOKUP TVF (new) | Medium | 70% |
| 29 | append | Combination | `UNION ALL` | Moderate | Schema merge | None | None | 90% |
| 30 | appendcol | Combination | `JOIN on ROW_NUMBER` | Moderate | Both schemas | ROW_NUMBER | None | 85% |
| 31 | appendpipe | Combination | `UNION ALL (self-ref)` | Moderate | Schema merge | None | None | 85% |
| 32 | multisearch | Combination | `UNION ALL of SELECTs` | Moderate | Schema merge | None | None | 90% |
| 33 | expand | Multi-Value | `CROSS JOIN UNNEST` | Moderate | Array type | UNNEST (Calcite built-in) | None | 90% |
| 34 | mvexpand | Multi-Value | `CROSS JOIN UNNEST + LIMIT` | Moderate | Array type | UNNEST | None | 90% |
| 35 | mvcombine | Multi-Value | `GROUP BY + ARRAY_AGG` | Moderate | All field names | ARRAY_AGG | None | 85% |
| 36 | nomv | Multi-Value | Rewrites to eval + MVJOIN | Easy | None | MVJOIN (exists) | None | 95% |
| 37 | flatten | Reshape | `SELECT struct.* expansion` | Moderate | Struct fields | ITEM (exists) | None | 85% |
| 38 | transpose | Reshape | `UNPIVOT + PIVOT` | Hard | All fields | ROW_NUMBER | Low | 80% |
| 39 | addtotals | Reshape | `UNION ALL with totals row` | Hard | Numeric fields | SUM | None | 80% |
| 40 | addcoltotals | Reshape | `Column sum expression` | Moderate | Numeric fields | None | None | 85% |
| 41 | replace | Transform | `REPLACE() projection` | Easy | None | REPLACE (SQL std) | None | 95% |
| 42 | fillnull | Transform | `COALESCE() projection` | Easy | Field types | COALESCE (SQL std) | None | 95% |
| 43 | fieldformat | Transform | `FORMAT() projection` | Easy | Field type | FORMAT UDF | None | 90% |
| 44 | kmeans | ML | `TABLE(KMEANS(...))` TVF | TVF | Input schema | KMEANS TVF (new) | Medium | 65% |
| 45 | ad | ML | `TABLE(AD(...))` TVF | TVF | Input schema | AD TVF (new) | Medium | 65% |
| 46 | ml | ML | `TABLE(ML(...))` TVF | TVF | Input schema | ML TVF (new) | Medium | 65% |
| 47 | describe | Metadata | `INFORMATION_SCHEMA query` | Easy | Metadata | None | None | 90% |
| 48 | explain | Metadata | `SqlExplain` wrapping | Easy | None | None | None | 95% |

---

## 3. Pipe Chaining Strategy

PPL's pipe semantics (`source=t | where x>1 | eval y=x*2 | fields y`) translate to nested subqueries:

```sql
SELECT y FROM (
  SELECT *, x * 2 AS y FROM (
    SELECT * FROM t WHERE x > 1
  )
)
```

**Strategy: Always nest, let Calcite optimize.** Each pipe stage wraps the previous as a derived table. Calcite's optimizer already eliminates redundant projections and merges compatible filters.

**Field pass-through:** Use `SELECT *` (SqlNode `SqlNodeList.STAR`). Calcite's SqlValidator expands `*` to actual fields. For commands that modify fields (eval override, rename, fields exclusion), enumerate fields explicitly using the schema tracker.

**Merge opportunities (optional optimization):**
- Consecutive WHERE → AND in same SqlSelect
- Sort + Head → single SqlOrderBy with LIMIT
- Consecutive projections → merged SELECT list

---

## 4. Analysis/Binding Phase Design: Can We Avoid a Schema Tracker?

### What Schema-Dependent Commands Are Really Doing

In V3, the `CalciteRelNodeVisitor` gets schema info from `context.relBuilder.peek().getRowType()`. In V4, SqlNode is built before RelNode exists. The question is: **do we actually need to resolve field lists before SqlNode construction, or can we defer?**

Let's classify what each command truly needs:

#### Category A: "SELECT * EXCEPT" — exclusion target known from AST

These commands know WHAT to exclude from the AST. They just need "all remaining fields." This is conceptually `SELECT * EXCEPT(known_cols)`.

| Command | Pattern | What AST Provides |
|---------|---------|-------------------|
| fields exclusion (`fields - a, b`) | `SELECT * EXCEPT(a, b)` | Excluded field names |
| eval override (`eval age = age+1`) | `SELECT * EXCEPT(age), age+1 AS age` | Override field name |
| mvcombine (`mvcombine target`) | `GROUP BY * EXCEPT(target)` | Target field name |

#### Category B: "SELECT * REPLACE" — replacement known from AST

| Command | Pattern | What AST Provides |
|---------|---------|-------------------|
| rename (`rename a AS b`) | `SELECT * REPLACE(a AS b)` | Rename mapping |

#### Category C: Truly need schema info (type or struct knowledge)

These commands depend on field **properties** that can't be derived from AST alone:

| Command | What It Needs | Why |
|---------|---------------|-----|
| flatten | Struct sub-field names | Must discover `address.street`, `address.city` from schema |
| addtotals | Numeric field detection | Must know which fields are numeric to SUM |
| addcoltotals | Numeric field detection | Same |
| transpose | All field names | Must enumerate all fields for UNPIVOT |

#### Category D: Schema merging — could be deferred

| Command | Pattern | Could Defer? |
|---------|---------|--------------|
| append | UNION ALL with NULL padding | Needs both schemas to pad missing columns |
| appendcol | JOIN with duplicate detection | Could use aliasing strategy |

### Key Insight: Calcite Does NOT Support SELECT * EXCEPT at SqlNode Level

Calcite 1.41.0 has `RelBuilder.projectExcept()` at the RelNode layer, but **no** `SqlSelectStarExcept` or BigQuery-style `SELECT * EXCEPT(col)` at the SqlNode level. The V3 patterns (lines 411, 1013, etc.) all work because RelBuilder has full schema access.

### Three Possible Approaches

#### Approach 1: Custom SqlNode Extensions (EXCEPT/REPLACE as first-class SqlNode types)

**Idea:** Create custom SqlNode subclasses that carry the "except/replace" semantics and let `SqlToRelConverter` resolve them (it has schema access).

```java
// Custom SqlNode types
class SqlSelectStarExcept extends SqlSelect {
    List<SqlIdentifier> excludedColumns;  // [a, b]
}

class SqlSelectStarReplace extends SqlSelect {
    List<Pair<SqlNode, SqlIdentifier>> replacements;  // [(age+1, age), ...]
}
```

Custom `SqlToRelConverter` rules would expand `*` minus excluded columns during conversion.

**Pros:** No schema tracker needed for Category A/B commands. Clean separation. Defers resolution to the layer that has schema info.
**Cons:** Requires extending Calcite's SqlToRelConverter (medium complexity). SqlValidator may not validate these correctly without custom logic. Untrodden path — few Calcite projects do this.

#### Approach 2: Lightweight Schema Tracker (original proposal, simplified)

**Idea:** Track field names inline during SqlNode construction. But simplified: only track for the ~4 commands that truly need it (Category C), and use `SELECT *` + post-hoc RelNode fixup for Category A/B.

**Hybrid pipeline:**
```
PPL AST → SqlNode (using SELECT * freely)
        → SqlValidator (expands *)
        → SqlToRelConverter → RelNode
        → Post-hoc RelNode adjustments for EXCEPT/REPLACE patterns
```

**Pros:** Minimal new code. Most commands use clean SqlNode with SELECT *.
**Cons:** "Post-hoc RelNode adjustment" blurs the clean SqlNode→RelNode boundary. Becomes V3.5 rather than V4.

#### Approach 3: Inline Schema Tracker — Minimal Version

**Idea:** Keep the schema tracker but make it much thinner. Since Category A/B commands only need **field names** (not types), and the exclusion targets come from the AST, the tracker is just a `List<String>` that tracks current field names through pipe stages.

**Type info only needed for:** flatten (struct sub-fields), addtotals (numeric detection) — 3 commands out of 48.

```java
public class PPLFieldTracker {
    private List<FieldEntry> fields;  // Just name + basic type category

    record FieldEntry(String name, boolean isNumeric, boolean isStruct, List<String> structSubFields) {}
}
```

**Operations needed:**
- `init(tableName)` — load from schema, one-time
- `getNames()` → `List<String>`
- `getNamesExcluding(Set<String>)` → for fields exclusion, eval override, mvcombine
- `getNumericNames()` → for addtotals (only 2 commands)
- `getStructSubFields(name)` → for flatten (only 1 command)
- `add(name)`, `remove(name)`, `rename(old, new)`, `replaceAll(names)` — mutations

**This is ~150-200 lines** — much simpler than the 470-570 line design.

### Recommended: Approach 3 (Minimal Inline Tracker)

The tracker is essentially a `List<String>` with a thin type overlay for 3 commands. Here's why:

1. **Category A/B commands** (fields exclusion, eval override, rename, mvcombine) need the tracker's field name list to build explicit `SELECT` clauses. This is unavoidable since Calcite SqlNode doesn't support EXCEPT/REPLACE syntax. But it's just **list subtraction** — trivial.

2. **Category C commands** (flatten, addtotals, transpose) need schema lookup. This is 3-4 commands and is unavoidable regardless of approach.

3. **Category D** (append, appendcol) can be handled by building each branch's SqlNode independently, then merging at the tracker level before constructing the UNION ALL SqlNode. The tracker's snapshot/merge is ~30 lines.

4. **Custom SqlNode extensions (Approach 1)** would be cleaner architecturally but risky — Calcite's SqlValidator and SqlToRelConverter expect standard SqlNode types, and extending them is fragile. The tracker is simpler and safer.

### Minimal Tracker Design

```java
/**
 * Lightweight field tracker for PPL→SqlNode construction.
 * Tracks current field names and basic type info through pipe stages.
 * ~150-200 lines total.
 */
public class PPLFieldTracker {
    private final List<FieldEntry> fields = new ArrayList<>();

    public record FieldEntry(String name, FieldKind kind) {}
    public enum FieldKind { NUMERIC, STRING, STRUCT, ARRAY, OTHER }

    // --- Init from Calcite schema (one-time per source table) ---
    void init(RelDataType tableRowType) {
        fields.clear();
        for (RelDataTypeField f : tableRowType.getFieldList()) {
            fields.add(new FieldEntry(f.getName(), classifyType(f.getType())));
        }
    }

    // --- Queries (used during SqlNode construction) ---
    List<String> names()                              // All field names
    List<String> namesExcluding(Set<String> exclude)  // For fields-, eval override, mvcombine
    List<String> numericNames()                        // For addtotals (2 commands)
    List<String> structSubFields(String prefix)        // For flatten (1 command)
    boolean has(String name)                           // For eval override detection

    // --- Mutations (called after building each pipe stage's SqlNode) ---
    void add(String name, FieldKind kind)        // eval new field, rex/parse fields
    void remove(String name)                     // fields exclusion
    void rename(String old, String newName)       // rename
    void replaceAll(List<FieldEntry> newFields)  // stats resets schema entirely
    PPLFieldTracker snapshot()                   // For append branch isolation

    private static FieldKind classifyType(RelDataType type) {
        return switch (type.getSqlTypeName().getFamily()) {
            case NUMERIC -> FieldKind.NUMERIC;
            case CHARACTER -> FieldKind.STRING;
            default -> {
                if (type instanceof ArraySqlType) yield FieldKind.ARRAY;
                if (type instanceof MapSqlType) yield FieldKind.STRUCT;
                yield FieldKind.OTHER;
            }
        };
    }
}
```

### Commands That Don't Need the Tracker At All

These commands use `SELECT *` or construct SqlNode purely from AST-provided field names:

where, sort, head, search, regex, join, stats, eventstats, streamstats, dedup, top, rare, bin, parse, grok, rex, spath, expand, mvexpand, nomv, fillnull, replace, lookup, describe, explain, chart, timechart, trendline, graphlookup, kmeans, ad, ml

**That's 35+ out of 48 commands** — the vast majority don't touch the tracker.

### Commands That Use the Tracker

| Command | Tracker Method | Why It Can't Be Avoided |
|---------|---------------|------------------------|
| fields (exclusion) | `namesExcluding()` | Must enumerate remaining fields for SELECT list |
| rename | `names()` + `rename()` | Must build full renamed SELECT list |
| eval (override) | `has()` + `namesExcluding()` | Must detect + remove shadowed field |
| flatten | `structSubFields()` | Must discover nested field names |
| mvcombine | `namesExcluding()` | Must build GROUP BY all-except-target |
| addtotals | `numericNames()` | Must identify numeric columns for SUM |
| addcoltotals | `numericNames()` | Same |
| transpose | `names()` | Must enumerate all fields for UNPIVOT |
| append | `snapshot()` + merge | Must align schemas for UNION ALL |
| appendcol | `names()` on both sides | Must detect duplicate columns |

**10 out of 48 commands** use the tracker — and most only call `namesExcluding()` or `names()`.

### Calcite Upstream: SELECT * EXCEPT/EXCLUDE Is Coming in 1.42.0

**Critical finding:** Calcite has added native `SELECT * EXCLUDE/EXCEPT` support at the SqlNode level:

- **CALCITE-7310** (resolved): Added `SELECT * EXCLUDE(columns)` syntax — new `SqlStarExclude` SqlNode class
- **CALCITE-7331** (resolved): Added `SELECT * EXCEPT(columns)` as alias for EXCLUDE
- **Fix version: Calcite 1.42.0**
- **Implementation:** New `SqlStarExclude` class extending `SqlCall`, with SqlValidator expansion and validation
- **Enablement:** Available via Babel parser / `includeStarExclude` parser config
- **PR:** apache/calcite#4662

**Current project uses Calcite 1.41.0** (`core/build.gradle` line 61).

### Impact on Schema Tracker

**If the project upgrades to Calcite 1.42.0**, the Category A/B commands can use native SqlNode EXCEPT:

| Command | Current Need | With Calcite 1.42.0 |
|---------|-------------|---------------------|
| fields exclusion | Tracker `namesExcluding()` | `SELECT * EXCEPT(a, b)` — **no tracker needed** |
| eval override | Tracker `has()` + `namesExcluding()` | `SELECT * EXCEPT(age), age+1 AS age` — **no tracker needed** |
| mvcombine | Tracker `namesExcluding()` | Still needs tracker (GROUP BY doesn't support EXCEPT) |
| rename | Tracker `names()` | Still needs tracker (REPLACE not yet in Calcite) |

**Commands still needing a tracker even with 1.42.0:**
- **rename** — no REPLACE syntax yet (only EXCLUDE/EXCEPT)
- **mvcombine** — GROUP BY * EXCEPT not supported
- **flatten** — needs struct sub-field discovery from schema
- **addtotals/addcoltotals** — needs numeric field detection
- **transpose** — needs full field enumeration for UNPIVOT
- **append** — needs schema merging

So upgrading to 1.42.0 would **reduce tracker usage from 10 commands to ~7**, and eliminate the two most common cases (fields exclusion, eval override).

### Contributing to Calcite: Is It Worth It?

**For SELECT * REPLACE** (needed for rename):
- Not yet in Calcite. DuckDB and BigQuery both support it.
- A contribution following the CALCITE-7310/7331 pattern would be feasible.
- Would eliminate tracker need for rename.

**For GROUP BY * EXCEPT** (needed for mvcombine):
- Non-standard SQL. No database supports this.
- Not worth contributing. Keep tracker for this case.

**Recommendation:**
1. **Short-term:** Use the minimal ~150-line tracker with Calcite 1.41.0.
2. **Medium-term:** Upgrade to Calcite 1.42.0 to leverage native `SELECT * EXCEPT`. Tracker shrinks to ~100 lines covering only rename/flatten/addtotals/mvcombine/transpose/append.
3. **Optional:** Contribute `SELECT * REPLACE` to Calcite to further reduce tracker to ~80 lines (flatten/addtotals/mvcombine/transpose/append only).

The tracker can never be **fully** eliminated because flatten/addtotals genuinely need schema type information. But it can shrink to a very thin layer.

---

## 5. Function Strategy

**No new function registration needed.** All 200+ PPL functions are already `SqlOperator` instances:

- `PPLBuiltinOperators` (512 lines) — extends `ReflectiveSqlOperatorTable`
- `PPLFuncImpTable` (1538 lines) — maps function names to implementations
- Operator table chained: `SqlOperatorTables.chain(PPLBuiltinOperators.instance(), SqlStdOperatorTable.instance())`

**Category breakdown:**

| Category | Count | Representation | Example |
|----------|-------|----------------|---------|
| SQL standard | ~60 | SqlStdOperatorTable | ABS, UPPER, COUNT, COALESCE |
| Custom scalar UDF | ~100 | PPLBuiltinOperators SqlOperator | JSON_EXTRACT, ADDDATE, CIDRMATCH |
| Custom aggregate | ~15 | PPLBuiltinOperators SqlAggFunction | PERCENTILE_APPROX, LIST, TAKE |
| Relevance/FTS | ~7 | RelevanceQueryFunction SqlOperator | MATCH, QUERY_STRING |
| Collection/Lambda | ~10 | PPLBuiltinOperators SqlOperator | FILTER, TRANSFORM, FORALL |

**SqlNode construction for functions:**
```
PPL Function("json_extract", [field, path])
  → SqlCall(PPLBuiltinOperators.JSON_EXTRACT, [SqlIdentifier("field"), SqlLiteral(path)])
```

---

## 6. Hard Cases & Breaking Changes

### 6.1 GraphLookup — BFS graph traversal (non-SQL)
- **Resolution:** Register as TVF `TABLE(GRAPH_LOOKUP(source, table, start, from, to, ...))`. Custom `SqlToRelConverter` rule produces `LogicalGraphLookup` RelNode.

### 6.2 ML Commands (kmeans, ad, ml) — no SQL equivalent
- **Resolution:** Same TVF pattern. Already unsupported in V3 (throws `CalciteUnsupportedException`), so no regression.

### 6.3 Consecutive Dedup — stateful row comparison
- **Resolution:** Expressible via `LAG()` window function + segment ID computation. Currently unsupported in V3, so this would be a V4 improvement.

### 6.4 StreamStats with reset_before/reset_after — segment-based windows
- **Resolution:** Multi-step SqlNode: (1) compute reset flags, (2) cumulative SUM for segment IDs, (3) window functions partitioned by segment. Complex but expressible.

### 6.5 Chart column-split limiting — pivot with TOP-N
- **Resolution:** Nested subqueries: rank column values → filter to top N → pivot aggregation. Deeply nested but valid SqlNode.

### 6.6 Fields exclusion (`fields - a, b`) — requires schema
- **Resolution:** PPLSchemaTracker enumerates current fields, excludes specified ones, builds explicit SELECT list. Normal pattern.

### 6.7 Transpose — dynamic column generation
- **Resolution:** Calcite has `SqlUnpivot` and `SqlPivot` SqlNode types. Map directly.

---

## 7. Translation Patterns

### Pattern 1: Simple Transform (where, fields, rename, eval, fillnull, replace)
```
PPL: source=t | where x>1
SQL: SELECT * FROM t WHERE x > 1
SqlNode: SqlSelect(from=SqlIdentifier("t"), where=SqlCall(>, x, 1))
```

### Pattern 2: Aggregation (stats, bin)
```
PPL: source=t | stats count() as cnt by state
SQL: SELECT state, COUNT(*) AS cnt FROM t GROUP BY state
SqlNode: SqlSelect(selectList=[state, SqlCall(COUNT, STAR) AS cnt], from=t, groupBy=[state])
```

### Pattern 3: Window Function (eventstats, trendline)
```
PPL: source=t | eventstats avg(bytes) as avg_bytes by host
SQL: SELECT *, AVG(bytes) OVER (PARTITION BY host) AS avg_bytes FROM t
SqlNode: SqlSelect(selectList=[STAR, SqlCall(AVG, bytes, OVER(PARTITION BY host)) AS avg_bytes])
```

### Pattern 4: Dedup via ROW_NUMBER (dedup, top, rare)
```
PPL: source=t | dedup 1 host
SQL: SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY host) AS rn FROM t) WHERE rn <= 1
SqlNode: SqlSelect(where=rn<=1, from=SqlSelect(ROW_NUMBER() OVER ... AS rn))
```

### Pattern 5: UNION ALL (append, appendpipe, multisearch)
```
PPL: source=a | fields x | append [source=b | fields x]
SQL: SELECT x, NULL AS y FROM a UNION ALL SELECT x, NULL AS y FROM b
SqlNode: SqlCall(UNION ALL, [SqlSelect(a), SqlSelect(b)])
```

### Pattern 6: JOIN (join, lookup)
```
PPL: source=orders | join ON o.id = c.id [source=customers]
SQL: SELECT * FROM orders o JOIN customers c ON o.id = c.id
SqlNode: SqlJoin(left=orders, right=customers, condition=ON...)
```

### Pattern 7: UNNEST (expand, mvexpand)
```
PPL: source=t | expand tags
SQL: SELECT t.*, u.tag AS tags FROM t CROSS JOIN UNNEST(t.tags) AS u(tag)
SqlNode: SqlSelect(from=SqlJoin(CROSS, t, SqlCall(UNNEST, tags)))
```

### Pattern 8: TVF (graphlookup, ML)
```
PPL: source=t | kmeans centroids=3
SQL: SELECT * FROM TABLE(KMEANS((SELECT * FROM t), 3))
SqlNode: SqlSelect(from=SqlCall(TABLE, SqlCall(KMEANS, subquery, 3)))
```

---

## 8. Recommended V4 Architecture

### End-to-End Pipeline

```
PPL Query String
    │
    ▼
[ANTLR Parser] ─── existing PPLSyntaxParser
    │
    ▼
PPL AST ─── existing AST nodes (unchanged)
    │
    ▼
[PPLSchemaTracker] ─── NEW (~300-500 lines)
    │  Walks AST, tracks field list per pipe stage
    │  Uses OpenSearchSchema for initial table metadata
    │
    ▼
[PPLSqlNodeVisitor] ─── NEW (~2000-3000 lines, replaces 3871-line CalciteRelNodeVisitor)
    │  Extends AbstractNodeVisitor<SqlNode, SqlNodeBuildContext>
    │  ~48 visit methods, each producing SqlNode
    │  References PPLBuiltinOperators for function SqlOperators
    │
    ▼
SqlNode (unvalidated)
    │
    ▼
[Calcite SqlValidator] ─── existing, via planner.validate()
    │  Type checking, function resolution, SELECT * expansion
    │  Already knows all PPL operators via chained operator table
    │
    ▼
SqlNode (validated)
    │
    ▼
[Calcite SqlToRelConverter] ─── existing, via planner.rel()
    │  + custom conversion rules for TVFs (graphlookup, ML)
    │
    ▼
RelNode (logical plan)
    │
    ▼
[Optimizer / Physical Planning] ─── existing
```

### Key New Components

| Component | Est. Size | Purpose |
|-----------|-----------|---------|
| `PPLSqlNodeVisitor` | ~2000-3000 lines | Core: PPL AST → SqlNode conversion |
| `PPLSchemaTracker` | ~300-500 lines | Lightweight field-list tracking for schema-dependent commands |
| `SqlNodeBuildContext` | ~100 lines | Context for SqlNode construction (operator table, schema tracker) |
| TVF registrations | ~200 lines | GRAPH_LOOKUP, KMEANS, AD, ML as table-valued functions |

### Integration Point

In `UnifiedQueryPlanner`, add a V4 path that reuses `planWithCalcite()`'s backend:

```java
// V4 path (NEW)
UnresolvedPlan ast = parse(query);
PPLSchemaTracker tracker = new PPLSchemaTracker(context.getSchema());
PPLSqlNodeVisitor sqlVisitor = new PPLSqlNodeVisitor(tracker, context.getOperatorTable());
SqlNode sqlNode = sqlVisitor.convert(ast);

// Reuse existing Calcite pipeline
Planner planner = Frameworks.getPlanner(config);
SqlNode validated = planner.validate(sqlNode);
RelRoot relRoot = planner.rel(validated);
return relRoot.rel;
```

### Implementation Phases

**Phase 1 — Foundation (Easy commands):** search, where, fields, eval, sort, head, rename, fillnull, replace, regex, nomv, spath, describe, explain, bin. Target: ~16 commands.

**Phase 2 — Aggregation & Window:** stats, eventstats, dedup, top, rare, trendline, streamstats. Target: ~23 commands.

**Phase 3 — Joins & Combination:** join, lookup, append, appendcol, appendpipe, multisearch. Target: ~29 commands.

**Phase 4 — Text & Multi-Value:** parse, grok, rex, patterns, expand, mvexpand, mvcombine, flatten. Target: ~37 commands.

**Phase 5 — Complex & TVF:** chart, timechart, reverse, transpose, addtotals, addcoltotals, fieldformat, table, graphlookup, kmeans, ad, ml. Target: all 48 commands.

---

## 9. Representative Example Mappings

### Ex 1: Filter + Projection
```
source=accounts | where age > 30 | fields firstname, age
```
```sql
SELECT firstname, age FROM (SELECT * FROM accounts WHERE age > 30)
```

### Ex 2: Eval with Override
```
source=t | eval age = age + 1, name2 = upper(name)
```
```sql
SELECT age + 1 AS age, name, UPPER(name) AS name2 FROM t
```

### Ex 3: Stats with Span
```
source=t | stats count() as cnt by span(timestamp, 1h) as time_bucket
```
```sql
SELECT SPAN(timestamp, '1h') AS time_bucket, COUNT(*) AS cnt FROM t GROUP BY SPAN(timestamp, '1h')
```

### Ex 4: Dedup
```
source=t | dedup 2 host keepempty=true
```
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY host) AS rn FROM t WHERE host IS NOT NULL
) WHERE rn <= 2
UNION ALL
SELECT *, NULL AS rn FROM t WHERE host IS NULL
```

### Ex 5: Top N by Group
```
source=t | top 3 city by state
```
```sql
SELECT city, state FROM (
  SELECT city, state, COUNT(*) AS cnt,
    ROW_NUMBER() OVER (PARTITION BY state ORDER BY COUNT(*) DESC) AS rn
  FROM t GROUP BY city, state
) WHERE rn <= 3
```

### Ex 6: Eventstats (Window)
```
source=t | eventstats avg(bytes) as avg_bytes by host
```
```sql
SELECT *, AVG(bytes) OVER (PARTITION BY host) AS avg_bytes FROM t
```

### Ex 7: Join
```
source=orders | join left=o right=c ON o.cid = c.id [source=customers]
```
```sql
SELECT o.*, c.* FROM orders AS o JOIN customers AS c ON o.cid = c.id
```

### Ex 8: Lookup with APPEND
```
source=events | lookup users uid AS id APPEND name, email
```
```sql
SELECT e.*, COALESCE(e.name, u.name) AS name, COALESCE(e.email, u.email) AS email
FROM events e LEFT JOIN users u ON e.id = u.uid
```

### Ex 9: Trendline (SMA)
```
source=t | trendline sort timestamp sma(3, bytes) as trend
```
```sql
SELECT *,
  CASE WHEN COUNT(*) OVER (ORDER BY timestamp ROWS 2 PRECEDING) >= 3
    THEN AVG(bytes) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
    ELSE NULL END AS trend
FROM t
```

### Ex 10: Expand (Array Unnest)
```
source=t | expand tags
```
```sql
SELECT t2.*, u.tag AS tags FROM t AS t2 CROSS JOIN UNNEST(t2.tags) AS u(tag)
```

### Ex 11: Append (UNION ALL)
```
source=a | fields x, y | append [source=b | fields x, z]
```
```sql
SELECT x, y, NULL AS z FROM a UNION ALL SELECT x, NULL AS y, z FROM b
```

### Ex 12: Fillnull
```
source=t | fillnull with 0 in age, score
```
```sql
SELECT COALESCE(age, 0) AS age, COALESCE(score, 0) AS score, name FROM t
```

---

## 10. Key Files Reference

| File | Role | Lines |
|------|------|-------|
| `core/.../calcite/CalciteRelNodeVisitor.java` | Current V3 visitor (reference, to be replaced) | 3871 |
| `api/.../api/UnifiedQueryPlanner.java` | Integration point; has `planWithCalcite()` | 144 |
| `api/.../api/UnifiedQueryContext.java` | Config: schema, conformance, operator tables | ~250 |
| `core/.../expression/function/PPLBuiltinOperators.java` | PPL functions as SqlOperators (reuse as-is) | 512 |
| `core/.../expression/function/PPLFuncImpTable.java` | Function implementations (reuse as-is) | 1538 |
| `core/.../calcite/CalcitePlanContext.java` | Current context (reference for SqlNodeBuildContext) | ~200 |
| `core/.../calcite/OpenSearchSchema.java` | Lazy table discovery (reuse for schema tracker) | ~100 |
| `ppl/src/main/antlr/OpenSearchPPLParser.g4` | PPL grammar (unchanged) | 1736 |
| `core/.../ast/AbstractNodeVisitor.java` | Visitor interface (unchanged) | ~200 |

---

## 11. Verification Strategy

1. **Unit tests:** For each visit method in `PPLSqlNodeVisitor`, verify generated SqlNode matches expected SQL shape using `SqlNode.toString()`
2. **Integration tests:** Reuse existing 58 Calcite PPL test files in `ppl/src/test/java/.../calcite/` — they test end-to-end query results, so swapping the internal path from V3 visitor to V4 SqlNode should produce identical results
3. **Comparison testing:** Run both V3 (CalciteRelNodeVisitor → RelNode) and V4 (PPLSqlNodeVisitor → SqlNode → RelNode) paths on the same queries and compare resulting RelNode trees
4. **Progressive rollout:** Feature flag to switch between V3 and V4 paths, enabling incremental migration command-by-command
