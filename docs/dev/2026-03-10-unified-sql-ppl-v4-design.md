# Unified SQL Support & PPL V4 (PPL-on-SQL) — Design Summary

> Date: 2026-03-10
> Branch: `poc/ppl-to-sqlnode-unified-api`
> Status: PoC validated. This document captures design decisions for production-quality implementation.

---

## Part 1: Unified SQL Support

### 1.1 UnifiedQueryPlanner Abstraction

**Motivation:** The current `plan()` method has 3 branches (Calcite native SQL, ANTLR SQL, ANTLR PPL) interleaved in one class. As paths evolve independently, this becomes hard to maintain.

**Challenge:** The 3 paths share no common AST — PPL/SQL use ANTLR ParseTree, ANSI SQL uses Calcite SqlNode. The public API (`new UnifiedQueryPlanner(ctx)` + `plan(String) → RelNode`) must not break.

**Design options:**
| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| A. Internal strategy pattern | Private `PlanningStrategy` interface with 3 implementations. UnifiedQueryPlanner becomes a facade. | Zero public API change. Each path is its own class. Easy to test in isolation. | Strategies are private — not extensible by external consumers. |
| B. UnifiedQueryPlanner as interface + factory | `UnifiedQueryPlanner.create(ctx)` returns the right implementation. | Clearest separation. Top-level classes. | Breaks public API (`new` → factory). Interface + class naming awkwardness. |

**Preferred option: A — Internal strategy pattern.**

UnifiedQueryPlanner becomes a thin facade responsible for:
- Strategy selection based on context (query type + conformance)
- Uniform error handling (try/catch wrapping)
- Cross-cutting concerns: logging, profiling, metrics

Each `PlanningStrategy` implementation owns its own parsing, validation, optimization, and collation preservation. The ANTLR PPL and ANTLR SQL strategies share the `parse() → analyze() → preserveCollation()` skeleton but differ in parser/AstBuilder construction.

```
UnifiedQueryPlanner (facade)
    ├── CalciteNativeSqlStrategy   (Calcite SqlParser → validate → rel → optimize)
    ├── AntlrSqlStrategy           (SQLSyntaxParser → AST → CalciteRelNodeVisitor → RelNode)
    └── AntlrPplStrategy           (PPLSyntaxParser → AST → CalciteRelNodeVisitor → RelNode)
        (future: PplV4Strategy — see Part 2)
```

### 1.2 UnifiedQueryParser

**Motivation:** Multiple unified query API consumers need parse-level operations without full planning:
- Extract table names → decide execution routing (OpenSearch vs Spark)
- Detect query complexity (JOIN, subquery) → decide execution path
- Custom validation visitors

**Challenge:** The 3 language paths produce different parse tree types (ANTLR ParseTree vs Calcite SqlNode). Defining a common visitor interface across both is awkward.

**Key insight:** If PPL V4 succeeds, all 3 paths produce SqlNode:
- PPL V4: `PPL string → transpile → SQL string → Calcite SqlParser → SqlNode`
- SQL (ANSI): `SQL string → Calcite SqlParser → SqlNode`
- SQL (OpenSearch): `SQL string → Calcite SqlParser → SqlNode` (once migrated)

**Preferred option: Design for SqlNode-only (post-V4 world).**

```java
public class UnifiedQueryParser {
    public UnifiedQueryParser(QueryType type, SqlConformance conformance);

    // All paths produce SqlNode
    public SqlNode parse(String query);

    // Users bring their own SqlVisitor/SqlShuttle
    public <R> R parse(String query, SqlVisitor<R> visitor);

    // Convenience extractors (implemented as SqlVisitors internally)
    public List<String> extractTableNames(String query);
    public QueryShape getQueryShape(String query);
}
```

During the transition period (PPL V3 ANTLR coexists with V4 SqlNode), the ANTLR PPL path simply doesn't go through `UnifiedQueryParser`. Once V4 is complete, everything converges.

**Relationship to UnifiedQueryPlanner:** Siblings, not parent-child. But `PlanningStrategy` implementations reuse `UnifiedQueryParser` for the parse step to avoid duplication:

```
UnifiedQueryContext
    ├── UnifiedQueryParser          (parse → SqlNode)
    │       ├── used by PlanningStrategy  (parse → validate → plan → RelNode)
    │       └── used by consumers directly (parse → inspect via SqlVisitor)
    ├── UnifiedQueryPlanner         (facade → delegates to PlanningStrategy)
    ├── UnifiedQueryCompiler        (RelNode → PreparedStatement)
    └── UnifiedQueryTranspiler      (RelNode → SQL text for Spark/Trino)
```

### 1.3 OpenSearch Schema (AnsiSQLOpenSearchSchema)

**Status: Done in PoC.**

A clean schema without UDT/UDF/metadata fields:
- `AnsiSQLOpenSearchSchema` wraps base `OpenSearchSchema`
- `AnsiSQLOpenSearchTable` maps date/time/timestamp to standard Calcite SqlTypeNames (not UDT)
- Metadata fields (`_id`, `_index`, `_score`, `_routing`) excluded
- `CalciteNoPushdownIndexScan` disables all 15 pushdown rules, registers only non-pushdown rules + `DateConvertingEnumerableScanRule`

**Design rationale:** Deliberately disable pushdown for correctness — lets Calcite handle all planning with standard operators, enabling JOINs and full SQL features. Selective pushdown re-enablement (filter, project, limit) is a future optimization.

### 1.4 REST Endpoint Integration

**Status: Done in PoC.**

`RestSqlAction` routes ALL SQL queries to `RestUnifiedSQLQueryAction`. The `mode` parameter controls behavior:
- `mode=opensearch` (default): ANTLR parser, `OpenSearchSchema`, pushdown enabled
- `mode=ansi`: Calcite native parser, `AnsiSQLOpenSearchSchema`, pushdown disabled

---

## Part 2: PPL-on-SQL (PPL V4)

### 2.1 Architecture: PPL AST → SqlNode → RelNode

**Motivation:**
1. **Tame complexity** — V3's `CalciteRelNodeVisitor` (1800+ lines) is out of control
2. **Gain analysis phase** — V3 bypasses SqlNode and misses Calcite's dedicated validation (type validation, dynamic field resolution, map path resolution problems encountered)
3. **Cross-engine portability** — Building PPL atop SQL makes it easier for other SQL engines (Spark, Trino) to integrate with PPL, either natively or via federated query

**PoC validation:** The string-based `PPLToSqlTranspiler` proved feasibility at 58.5% pass rate (1324/2264 tests). But it's schema-unaware, which limits correctness for type coercion, wildcard expansion, and column resolution.

**Production architecture:**

```
PPL string → PPLSyntaxParser → AST → PPLToSqlNodeConverter(SchemaPlus) → SqlNode
    → Calcite SqlValidator → RelNode
    → UnifiedQueryTranspiler → SQL text (for Spark/Trino/etc.)
    → UnifiedQueryCompiler → execute
```

**PPLToSqlNodeConverter** replaces both:
- `CalciteRelNodeVisitor` (V3, too complex, bypasses analysis)
- `PPLToSqlTranspiler` (PoC, schema-unaware string manipulation)

**Two-tier converter design:**

| Tier | Class | Schema | Commands |
|------|-------|--------|----------|
| **Base (static)** | `PPLToSqlNodeConverter` | No — only uses what's explicitly in the PPL AST | where, sort, head, eval (add column), stats, dedup, join, lookup, subquery, append, top, rare, trendline, streamstats |
| **Subclass (dynamic)** | `DynamicPPLToSqlNodeConverter` | Yes — resolves columns dynamically from `SchemaPlus` | All base commands + fields (include/exclude), eval (column override), rename (wildcard), fillnull (all-fields), replace, bin, appendcol, graphlookup TVF |

```java
// Base — no schema dependency, handles "static" commands
public class PPLToSqlNodeConverter extends PPLAstVisitor<SqlNode> {
    public SqlNode convert(Statement ast) { ... }
    // Handles: where, sort, head, eval, stats, dedup, join, ...
}

// Subclass — adds commands that need dynamic column resolution
public class DynamicPPLToSqlNodeConverter extends PPLToSqlNodeConverter {
    private final SchemaPlus schema;

    public DynamicPPLToSqlNodeConverter(SchemaPlus schema) {
        this.schema = schema;
    }
    // Overrides: visitProject (fields -), visitRename (wildcard),
    //            visitFillNull (all-fields), visitGraphLookup (TVF), ...
}
```

**Unified code path:**
```
PPL string → PPLSyntaxParser → AST
    → (Dynamic)PPLToSqlNodeConverter → SqlNode
    → UnifiedQueryPlanner → RelNode
    → UnifiedQueryCompiler → execute
    → UnifiedQueryTranspiler → SQL text (for Spark/Trino)
```

All PPL queries go through SqlNode. `UnifiedQueryTranspiler` consumes RelNode (downstream of the planner), never SqlNode directly. The converter choice (base vs dynamic) depends on whether schema is available:
- **With OpenSearch cluster:** `DynamicPPLToSqlNodeConverter(schema)` — full command support
- **Without cluster (SDK, testing):** `PPLToSqlNodeConverter` — static commands only, or provide an in-memory schema

**Key properties:**
- Produces `SqlNode` — feeds into Calcite's validate → rel pipeline
- Lighter dependency than V3's `CalcitePlanContext` (no RelBuilder, no RexBuilder needed)
- Does NOT need to track types or emit CASTs for coercion — that's handled by `PPLTypeCoercion` during Calcite validation (see 2.2)
- `DynamicPPLToSqlNodeConverter` uses `SchemaPlus` only for column enumeration (`fields -`, rename wildcards, `fillnull` all-fields) and TVF parameter construction

**Design principle: SqlNode DSL for clean, declarative translation.**

V3's `CalciteRelNodeVisitor` (1800+ lines) became unmanageable because it mixes low-level `RelBuilder`/`RexBuilder` imperative calls with PPL semantic logic — field index tracking, manual projection list construction, mutable context state, and ad-hoc workarounds. The result is brittle code where a small PPL command change can cascade into dozens of lines of RelBuilder plumbing.

`PPLToSqlNodeConverter` should use a **SqlNode DSL** — a thin builder layer over Calcite's `SqlNode` constructors — similar in spirit to PPL's `AstDSL` or JOOQ's query builder:

```java
// Instead of 50 lines of RelBuilder calls for: source=t | where a > 1 | fields b, c
// Write declarative SqlNode construction:

SqlNode result = select(identifiers("b", "c"))
    .from(table("t"))
    .where(gt(identifier("a"), literal(1)))
    .build();

// Pipe chaining wraps as subquery naturally:
// source=t | where a > 1 | stats avg(b) by c
SqlNode inner = select(star()).from(table("t")).where(gt(id("a"), lit(1))).build();
SqlNode result = select(agg("AVG", id("b")), id("c"))
    .from(subquery(inner))
    .groupBy(id("c"))
    .build();
```

**Why this matters:**
1. **Readability** — Each PPL command's translation reads as a SQL query shape, not as imperative builder mutations
2. **Composability** — Pipe chaining is just subquery wrapping, no mutable context threading
3. **Debuggability** — `SqlNode.toString()` produces readable SQL at any point; RelNode toString is opaque
4. **Maintainability** — Adding a new PPL command means writing one `visitXxx` method that returns a SqlNode, not threading state through a shared RelBuilder
5. **Testability** — Each command's output can be asserted as SQL text, not as RelNode plan structure

The DSL is a thin utility layer (static factory methods wrapping `SqlNode` constructors), not a framework. It keeps the converter focused on *what* SQL to produce, not *how* to construct Calcite internal objects.

**Integration with UnifiedQueryPlanner:**

```java
class PplV4PlanningStrategy implements PlanningStrategy {
    private final UnifiedQueryParser parser;
    private final DynamicPPLToSqlNodeConverter converter;

    RelNode plan(String pplQuery) {
        // 1. PPL parse → AST
        Statement ast = parsePplToAst(pplQuery);
        // 2. AST → SqlNode (declarative, schema-aware)
        SqlNode sqlNode = converter.convert(ast);
        // 3. SqlNode → validate (PPLTypeCoercion injects CASTs) → RelNode
        SqlNode validated = planner.validate(sqlNode);
        RelRoot rel = planner.rel(validated);
        return optimize(rel.rel);
    }
}
```

### 2.2 Implicit Type Coercion

**Challenge:** PPL has a rich implicit type coercion system — far more than just 2 rules:

| Layer | Rules |
|-------|-------|
| **Type hierarchy widening** | BYTE→SHORT→INTEGER→LONG→FLOAT→DOUBLE (numeric chain), STRING→BOOLEAN, STRING→DATE/TIME/IP, DATE+TIME→TIMESTAMP |
| **COMMON_COERCION_RULES** | DATE+TIME→TIMESTAMP, STRING+NUMBER→DOUBLE |
| **STRING→DOUBLE shortcut** | Hardcoded distance=1 in `CoercionUtils.distance()` (not derivable from hierarchy) |
| **Boolean conversion** | String `'1'`→true, `'0'`→false, numeric≠0→true (in ExtendedRexBuilder) |
| **UDT casts** | STRING→DATE/TIME/TIMESTAMP/IP via PPLBuiltinOperators |
| **Float→String** | NUMBER_TO_STRING operator (avoids scientific notation) |
| **Comparator widening** | Find widest common type among operands, cast all to it |
| **Function signature matching** | Try all signatures, pick closest by distance, cast args |
| **IN/BETWEEN** | `leastRestrictive()` across all values, cast to common type |
| **COALESCE** | Runtime coercion with string fallback |
| **ADD operator** | STRING+STRING→CONCAT, NUMBER+NUMBER→PLUS, STRING+NUMBER→DOUBLE via coercion |

V3 handles this via `CoercionUtils` + `PPLFuncImpTable.resolveWithCoercion()` at the RexNode level. The ANSI SQL path uses Calcite's default `TypeCoercionImpl` which doesn't have PPL rules.

**Key constraint:** PPL pipe chaining produces nested subqueries. Identifiers like `avg(a)` where `a` is a computed column from an inner subquery cannot have their types resolved before Calcite validation — type resolution requires the full validator infrastructure (scope chain, lazy namespace validation). A pre-validate `SqlShuttle` is fundamentally unable to resolve types for derived expressions. The existing `StarExceptReplaceRewriter` confirms this — it explicitly skips subqueries.

**Design options:**
| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| A. Custom TypeCoercionFactory | Extend `TypeCoercionImpl`, override coercion methods, inject via `FrameworkConfig.sqlValidatorConfig()` | Fires during validation when all types are known (including derived/subquery columns). Injects explicit CAST SqlNodes that propagate to RelNode and transpiled SQL. Standard Calcite extension point. | Need to understand ~15 overridable methods. |
| B. PPLToSqlNodeConverter emits explicit CASTs | Converter tracks types and emits CASTs during AST→SqlNode conversion | No Calcite customization. | Cannot resolve types for derived expressions from nested subqueries. Would need a parallel type tracking system. |
| C. Pre-validate SqlShuttle | SqlShuttle rewrites SqlNode tree to inject CASTs before validation | Follows StarExceptReplaceRewriter pattern. | SqlShuttle has zero access to type information. Cannot resolve subquery-derived columns. Dead end for anything beyond base table columns. |

**Preferred option: A — Custom TypeCoercionFactory.**

**Rationale:** TypeCoercion fires *during* validation when Calcite already knows all identifier types — including computed expressions, function return types, and subquery-derived columns. Critically, TypeCoercion does NOT just "relax" validation — it physically mutates the SqlNode tree by injecting explicit CAST nodes:

```
coerceOperandType()
  → castTo() → SqlStdOperatorTable.CAST.createCall()  // creates CAST SqlNode
  → call.setOperand(index, castNode)                   // MUTATES the SqlNode tree
```

These CASTs propagate through the full pipeline:
```
CAST SqlNode → SqlToRelConverter → rexBuilder.makeCast() → RexCall(CAST) in RelNode
    → UnifiedQueryTranspiler → explicit CAST(...) in transpiled SQL text
```

This means any downstream SQL engine (Spark, Trino) that consumes the RelNode or transpiled SQL gets explicit CASTs — achieving cross-engine portability without requiring PPL-specific coercion in each engine.

**Implementation:**

```java
class PPLTypeCoercion extends TypeCoercionImpl {
    public PPLTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
        super(typeFactory, validator);
    }

    @Override
    public boolean binaryComparisonCoercion(SqlCallBinding binding) {
        // Add PPL rules: STRING+NUMBER→DOUBLE, DATE+TIME→TIMESTAMP
        // coerceOperandType() injects CAST nodes automatically
        // Fall back to super for standard SQL coercion
    }

    @Override
    public boolean builtinFunctionCoercion(SqlCallBinding binding,
            List<RelDataType> operandTypes, List<SqlTypeFamily> expectedFamilies) {
        // Add PPL function signature coercion (closest-match by distance)
        // Fall back to super for standard SQL coercion
    }
}
```

Registration in `UnifiedQueryContext.buildFrameworkConfig()`:
```java
.sqlValidatorConfig(SqlValidator.Config.DEFAULT
    .withTypeCoercionFactory((typeFactory, validator) ->
        new PPLTypeCoercion(typeFactory, validator)))
```

**Base conformance: `SqlConformanceEnum.MYSQL_5`.**

PPL V2's implicit conversion rules were modeled after MySQL. Calcite's default `TypeCoercionImpl` was also modeled after MySQL/MS-SQL. Using `MYSQL_5` conformance gets ~80% of PPL coercion for free:

| Rule | Calcite Default (built-in) | Needs MYSQL_5 | PPL-specific (needs PPLTypeCoercion) |
|------|---------------------------|---------------|--------------------------------------|
| `1 > '1'` → coerce string to numeric | ✅ `commonTypeForBinaryComparison`: "isAtomic && isCharacter → return atomic type" | No | No |
| DATE < TIMESTAMP → TIMESTAMP | ✅ Built-in rule | No | No |
| DATETIME < CHARACTER → DATETIME | ✅ Built-in rule | No | No |
| STRING → NUMERIC in function args | ✅ `implicitCast()`: "isCharacter && numericFamily → default type" | No | No |
| STRING → DATE/TIME/TIMESTAMP in function args | ✅ `implicitCast()`: "isCharacter && dateTimeFamily → default type" | No | No |
| Numeric widening (INT → DOUBLE) | ✅ leastRestrictive / precision comparison | No | No |
| BOOLEAN ↔ NUMBER | ❌ Default off | ✅ `allowLenientCoercion()` enables it | No |
| STRING → ARRAY | ❌ Default off | ✅ `allowLenientCoercion()` enables it | No |
| DATE + TIME → TIMESTAMP | ❌ Calcite has DATE→TIMESTAMP but not DATE+TIME merge | No | ✅ Override `binaryComparisonCoercion` |
| String `'1'`/`'0'` → boolean | ❌ Calcite does numeric→boolean, not string→boolean | No | ✅ Override coercion or keep in ExtendedRexBuilder |
| Float→String via NUMBER_TO_STRING | ❌ Calcite uses standard toString | No | ✅ Keep in ExtendedRexBuilder |
| UDT casts (EXPR_DATE/TIME/TIMESTAMP/IP) | ❌ PPL-specific types | No | ✅ Keep in ExtendedRexBuilder |

**Key insight:** `SqlConformance` controls two things in Calcite's coercion:
1. `allowLenientCoercion()` — gates BOOLEAN↔NUMBER and STRING→ARRAY coercion
2. `getTypeMappingRule()` — when lenient, uses `SqlTypeCoercionRule.lenientInstance()` which adds BOOLEAN to the numeric coercion set

The `commonTypeForBinaryComparison()` rules (STRING→NUMERIC comparisons, DATE→TIMESTAMP widening) are built-in regardless of conformance — they were designed after MySQL/MS-SQL per Calcite source comments.

**What `PPLTypeCoercion` needs to add on top of `MYSQL_5`:**
1. DATE + TIME → TIMESTAMP (merge, not just widening)
2. String literal `'1'`/`'0'` → boolean (if not handled by ExtendedRexBuilder downstream)
3. Any PPL-specific function signature coercion not covered by Calcite's `implicitCast()`

**Reference logic:** The existing `CoercionUtils` rules (COMMON_COERCION_RULES, distance(), resolveCommonType()) and `ExprCoreType` hierarchy serve as the specification for what `PPLTypeCoercion` should implement. The gap is much smaller than initially expected.

**Impact on PPLToSqlNodeConverter:** The converter does NOT need to track types or emit CASTs for coercion. It focuses purely on syntax translation (PPL AST → SqlNode). Type coercion is handled by `PPLTypeCoercion` during Calcite validation — clean separation of concerns.

### 2.3 SELECT * EXCEPT/REPLACE

**Status: Done in PoC** — Custom `SqlStarExceptReplace` SqlNode + `StarExceptReplaceRewriter` SqlShuttle + FMPP/JavaCC grammar extension.

**Context:**
- Calcite 1.41.0 (current): No `SELECT * EXCEPT` at SqlNode layer. Has `RelBuilder.projectExcept()` at RelNode layer (programmatic API, not SQL syntax).
- Calcite 1.42.0 (not yet adopted): Adds native `SqlStarExclude` (CALCITE-7310/7331) — EXCEPT only, no REPLACE.
- Current PoC: Custom implementation supporting both EXCEPT and REPLACE.

**Decision: Nice-to-have.**

With schema-aware `PPLToSqlNodeConverter`, PPL V4 can enumerate columns explicitly:
- `fields - col1` → `SELECT col2, col3, col4 FROM t` (no EXCEPT needed)
- `eval x = x+1` → `SELECT col1, x+1 AS x, col3 FROM t` (no REPLACE needed)
- `rename a AS b` → `SELECT a AS b, col2, col3 FROM t`

EXCEPT/REPLACE remains useful as:
- SQL syntax extension for direct ANSI SQL users (`/_plugins/_sql?mode=ansi`)
- Cleaner transpiled SQL for readability on wide tables
- Can upgrade to Calcite 1.42.0 for native EXCEPT when ready (REPLACE still needs custom support)

### 2.4 SqlLibraryOperators Registration

**Status: Substantially done in PoC.**

`UnifiedQueryContext` registers `SqlLibrary.MYSQL`, `BIG_QUERY`, `SPARK`, `POSTGRESQL` via `SqlLibraryOperatorTableFactory`. This provides hundreds of functions: REVERSE, REGEXP_REPLACE, LEFT, RIGHT, SOUNDEX, MD5, SHA1, CONCAT_WS, SPLIT, ARRAY_AGG, ILIKE, SAFE_DIVIDE, etc.

`PPLFuncImpTable` maps 200+ PPL function names to these operators.

**Remaining gaps:** PPL-specific functions not in any SQL library — these are handled by UDFs (see 2.6).

### 2.5 TVF for PPL Commands Not Expressible in SQL

**Analysis:** Of 48 PPL commands, only **4 genuinely need TVF**:

| Command | Why TVF | SQL Alternative |
|---------|---------|-----------------|
| `kmeans` | ML clustering, no SQL equivalent | None |
| `ad` | ML anomaly detection, no SQL equivalent | None |
| `ml` | General ML fit/apply, no SQL equivalent | None |
| `graphlookup` | Recursive graph BFS with OpenSearch-specific execution | Basic cases: recursive CTE. Advanced (bidirectional, array): TVF |

**Commands that are hard but expressible as complex SQL** (no TVF needed):
| Command | SQL Strategy |
|---------|-------------|
| `appendcol` | ROW_NUMBER() + FULL JOIN |
| `transpose` | Dynamic UNPIVOT + PIVOT (hardest — column names depend on data) |
| `dedup consecutive` | LAG() window function |
| `patterns` | Custom UDAF (LOG_PATTERN_AGG, already exists) |
| `streamstats reset_before/after` | Segment-based windows (SUM of reset flags → partitioned window) |
| `chart`/`timechart` column-split | Nested GROUP BY + PIVOT + TOP-N ranking |
| `addtotals` | UNION ALL with summary row |

**Concrete TVF example — graphlookup:**

PPL:
```
source=employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name maxDepth=3 depthField=level as hierarchy
```

SQL emitted by PPLToSqlNodeConverter:
```sql
SELECT * FROM TABLE(
  GRAPH_LOOKUP(
    CURSOR(SELECT * FROM employees),
    'employees', 'reportsTo', 'reportsTo', 'name',
    'hierarchy', 3, 'level', 'UNI', FALSE, FALSE, FALSE
  )
)
```

**Registration pattern (3 pieces):**

1. **TableFunction class** — implements `eval()` method that Calcite discovers via reflection. Takes `ScannableTable` (from CURSOR) + scalar parameters. Returns `ScannableTable` that performs BFS at scan time (reuses existing `CalciteEnumerableGraphLookup` BFS logic).

2. **SqlUserDefinedTableFunction** — wraps the class via `TableFunctionImpl.create(GraphLookupTableFunction.class, "eval")`. Registered with name `GRAPH_LOOKUP`, return type `CURSOR`.

3. **Operator table registration** — added to `SqlOperatorTables.of(...)` chain in `UnifiedQueryContext.buildFrameworkConfig()`.

**How Calcite processes it:**
1. Parser: `TABLE(GRAPH_LOOKUP(...))` → `COLLECTION_TABLE` SqlCall (already supported in Parser.jj)
2. Validator: resolves `GRAPH_LOOKUP` from operator table → validates argument types
3. SqlToRelConverter: converts to `LogicalTableFunctionScan` RelNode
4. Planner rule: converts `LogicalTableFunctionScan` → physical execution node

**Same pattern applies to kmeans/ad/ml:** `TABLE(KMEANS(CURSOR(SELECT * FROM data), 3))`.

### 2.6 UDF for PPL Functions Not in SQL

**Status: Infrastructure done in PoC.**

`UserDefinedFunctionUtils` + `UserDefinedFunction`/`UserDefinedAggFunction` interfaces provide the registration mechanism. 8 UDAFs already registered: `FirstAggFunction`, `LastAggFunction`, `TakeAggFunction`, `PercentileApproxFunction`, `ListAggFunction`, `ValuesAggFunction`, `LogPatternAggFunction`, `NullableSqlAvgAggFunction`.

**For PPL V4:** UDFs must be callable from SQL text since `PPLToSqlNodeConverter` emits SqlNode. The pattern already works — `match_phrase` is registered as a UDF in `UnifiedQueryContext` and callable from SQL. Same pattern for any PPL function without a SQL equivalent.

**Registration:** `PPLBuiltinOperators` (extends `ReflectiveSqlOperatorTable`) registers ~100+ scalar UDFs. These are already in the operator table chain and resolvable by Calcite's validator.

### 2.7 Current PoC Status & Next Steps

**PoC pass rate:** 58.5% (1324/2264 tests, 144 skipped) across all 112 Calcite*IT classes.

**Top remaining failure areas:**
| Test Class | Failures | Likely Fix |
|------------|----------|------------|
| CalciteSearchCommandIT | 46 | Relevance function handling |
| CalciteDateTimeFunctionIT | 35 | Cross-type datetime coercion (addressed by 2.2) |
| CalciteMultiValueStatsIT | 31 | Multi-value field handling |
| CalciteFieldsCommandIT | 21 | Wildcard/schema-dependent fields (addressed by schema-aware converter) |
| CalcitePPLJoinIT | 18 | Join column dedup, schema merging |

**Before next Ralph loop iteration — refactoring tasks:**
1. Refactor `UnifiedQueryPlanner` with internal `PlanningStrategy` pattern (1.1)
2. Introduce `UnifiedQueryParser` as sibling to planner (1.2)
3. Begin `PPLToSqlNodeConverter` (schema-aware) to replace string-based transpiler (2.1)
4. Port `CoercionUtils` rules into converter for explicit CAST emission (2.2)
5. Register TVF pattern for graphlookup as proof-of-concept (2.5)

---

## Architecture Overview (Post-Refactoring)

```
                    ┌─────────────────────────┐
                    │   UnifiedQueryContext    │
                    │  (catalog, conformance,  │
                    │   settings, operators)   │
                    └────────┬────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
    ┌─────────▼──────┐ ┌────▼─────┐ ┌──────▼──────────┐
    │UnifiedQuery    │ │Unified   │ │UnifiedQuery     │
    │Planner(facade) │ │Query     │ │Compiler         │
    │                │ │Parser    │ │(RelNode→execute) │
    │ ┌────────────┐ │ │(→SqlNode)│ └─────────────────┘
    │ │Planning    │ │ └────┬─────┘ ┌─────────────────┐
    │ │Strategy    │ │      │       │UnifiedQuery     │
    │ │(selected   │◄──reuse│       │Transpiler       │
    │ │ by context)│ │      │       │(RelNode→SQL txt)│
    │ └────────────┘ │      │       └─────────────────┘
    └────────────────┘      │
                            │ consumers use directly
              ┌─────────────┼─────────────┐
              │             │             │
         extractTable  getQueryShape  custom
         Names()       ()             SqlVisitor

Planning Strategies:
  ┌──────────────────────────────────────────────────────┐
  │ CalciteNativeSqlStrategy                             │
  │   SqlNode → validate → RelNode → optimize            │
  ├──────────────────────────────────────────────────────┤
  │ AntlrSqlStrategy (transitional)                      │
  │   ANTLR → AST → CalciteRelNodeVisitor → RelNode      │
  ├──────────────────────────────────────────────────────┤
  │ AntlrPplStrategy (V3, transitional)                  │
  │   ANTLR → AST → CalciteRelNodeVisitor → RelNode      │
  ├──────────────────────────────────────────────────────┤
  │ PplV4Strategy (target)                               │
  │   ANTLR → AST → PPLToSqlNodeConverter(SchemaPlus)    │
  │   → SqlNode (with explicit CASTs) → validate → RelNode│
  └──────────────────────────────────────────────────────┘
```
