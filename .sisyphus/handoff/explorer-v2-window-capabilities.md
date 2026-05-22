# V2 Engine Window Function Capabilities (Pre-PR)

## 1. Registered Window Functions

### WINDOW_FUNC_MAPPING (BuiltinFunctionName.java:408-427)
These are aggregate functions that can be used in window context:
- `max` → MAX
- `min` → MIN
- `avg` → AVG
- `count` → COUNT
- `sum` → SUM
- `var_pop` / `variance` → VARPOP
- `var_samp` → VARSAMP
- `std` / `stddev` / `stddev_pop` → STDDEV_POP
- `stddev_samp` → STDDEV_SAMP
- `earliest` → EARLIEST
- `latest` → LATEST
- `distinct_count_approx` / `dc` / `distinct_count` → DISTINCT_COUNT_APPROX
- `pattern` → INTERNAL_PATTERN

### WindowFunctions.java (registered as dedicated window functions)
- `row_number()` → RowNumberFunction (ranking/RowNumberFunction.java)
- `rank()` → RankFunction (ranking/RankFunction.java)
- `dense_rank()` → DenseRankFunction (ranking/DenseRankFunction.java)
- `brain()` → BufferPatternWindowFunction (patterns/BufferPatternWindowFunction.java)

### WindowFunctionExpression implementations:
1. **RankingWindowFunction** (abstract base) → uses CurrentRowWindowFrame
   - RowNumberFunction
   - RankFunction
   - DenseRankFunction
2. **AggregateWindowFunction** → wraps any Aggregator, uses PeerRowsWindowFrame
3. **BufferPatternWindowFunction** → uses BufferPatternRowsWindowFrame

## 2. DISTINCT Inside Window Aggregate

### Grammar: YES, accepted
- `OpenSearchSQLParser.g4:186` — `windowFunction` includes `aggregateFunction`
- `OpenSearchSQLParser.g4:509` — `aggregateFunction` includes `# distinctCountFunctionCall` rule: `COUNT LR_BRACKET DISTINCT functionArg RR_BRACKET`
- So `COUNT(DISTINCT x) OVER(...)` is grammatically valid.

### Parser: YES, produces AggregateFunction with distinct=true
- `AstExpressionBuilder.java:240` — `visitDistinctCountFunctionCall` creates `new AggregateFunction(ctx.COUNT().getText(), visitFunctionArg(ctx.functionArg()), true)` (third arg = distinct=true)
- `AstExpressionBuilder.java:207` — `visitWindowFunctionClause` wraps the function result in a `WindowFunction` AST node

### Analyzer: YES, passes through
- `ExpressionAnalyzer.java:163-183` — `visitAggregateFunction` calls `aggregator.distinct(node.getDistinct())` setting the distinct flag on the Aggregator
- `ExpressionAnalyzer.java:247-249` — `visitWindowFunction` wraps the Aggregator in `new AggregateWindowFunction(aggregator)` — the aggregator already has distinct=true

### Runtime: YES, honored
- `AggregateWindowFunction.java:37-44` — calls `aggregator.create()` and `aggregator.iterate()`
- `CountAggregator.java:28-29` — `create()` returns `distinct ? new DistinctCountState() : new CountState()`
- `CountAggregator.java:63-70` — `DistinctCountState` uses a HashSet to track distinct values

### Integration tests confirm:
- `WindowFunctionIT.java:68-125` — tests for `COUNT(DISTINCT gender) OVER()`, `OVER(ORDER BY ...)`, and `OVER(PARTITION BY ... ORDER BY ...)`

**CONCLUSION: V2 DOES support COUNT(DISTINCT x) OVER(...) end-to-end.**

## 3. ORDER BY Inside OVER(...)

### Grammar: YES
- `OpenSearchSQLParser.g4:189-190` — `overClause: OVER LR_BRACKET partitionByClause? orderByClause? RR_BRACKET`

### Parser: YES
- `AstExpressionBuilder.java:218-224` — parses `orderByClause` into sortList with SortOption

### WindowDefinition: YES, sortList is populated
- `WindowDefinition.java:22` — `private final List<Pair<SortOption, Expression>> sortList`

### WindowExpressionAnalyzer: YES, uses sortList
- `WindowExpressionAnalyzer.java:70-73` — if `allSortItems` is non-empty, wraps child in `LogicalSort` before `LogicalWindow`
- The sort is used both for upstream sorting AND by PeerRowsWindowFrame to determine peer boundaries

### PeerRowsWindowFrame: YES, uses sortList for peer detection
- `PeerRowsWindowFrame.java:107-112` — `isPeer()` resolves sort fields and compares values between rows

**CONCLUSION: V2 fully supports ORDER BY inside OVER(). It inserts an upstream sort AND uses sort keys to determine peer groups for aggregate window functions.**

## 4. Window Frame Specifications (ROWS/RANGE BETWEEN)

### Grammar: NO
- `OpenSearchSQLParser.g4:189-190` — `overClause` only has `partitionByClause? orderByClause?`
- No `frameClause`, `frameBound`, `ROWS`, `RANGE`, `BETWEEN`, `UNBOUNDED`, `PRECEDING`, `FOLLOWING`, or `CURRENT ROW` rules exist in the SQL grammar for window context.

### Parser: NO — nothing to parse

### Runtime: N/A — hardcoded frame behavior
- Ranking functions: always use `CurrentRowWindowFrame` (previous + current row only)
- Aggregate functions: always use `PeerRowsWindowFrame` (accumulates all peers in partition up to current peer group)
- `WindowFunctionExpression.java:14-24` — comment explicitly states: "Aggregate window functions: frame partition into peers and sliding window is not supported"

**CONCLUSION: V2 does NOT support frame specifications. The grammar doesn't accept them, and the runtime uses hardcoded frame types. Aggregate window functions behave as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (peer-based accumulation).**

## 5. COUNT(*) OVER vs COUNT(x) OVER vs COUNT(DISTINCT x) OVER

### COUNT(*) OVER:
- Grammar: `countStarFunctionCall: COUNT LR_BRACKET STAR RR_BRACKET` (line 508)
- Parser: `visitCountStarFunctionCall` creates `AggregateFunction("count", AllFields.of())`
- Works in window context via AggregateWindowFunction wrapping

### COUNT(x) OVER:
- Grammar: `regularAggregateFunctionCall` (line 507)
- Works normally via AggregateWindowFunction

### COUNT(DISTINCT x) OVER:
- Grammar: `distinctCountFunctionCall` (line 509)
- **Fully supported** — see section 2 above
- No error; works correctly with DistinctCountState

**CONCLUSION: All three forms work in V2. No error for DISTINCT.**

## 6. Unsupported Window Functions

The V2 SQL grammar `scalarWindowFunction` rule (line 185) ONLY accepts:
- `ROW_NUMBER`
- `RANK`
- `DENSE_RANK`

Any other function used with OVER() must go through the `aggregateWindowFunction` path (i.e., must be a recognized aggregate function).

| Function | V2 Support | Evidence |
|----------|-----------|----------|
| ROW_NUMBER | YES | WindowFunctions.java:42, grammar line 185 |
| RANK | YES | WindowFunctions.java:46, grammar line 185 |
| DENSE_RANK | YES | WindowFunctions.java:50, grammar line 185 |
| LEAD | NO | Not in grammar, not in WindowFunctions, not in BuiltinFunctionName enum |
| LAG | NO | Not in grammar, not in WindowFunctions, not in BuiltinFunctionName enum |
| NTILE | NO | Not in grammar, not in WindowFunctions, not in BuiltinFunctionName enum |
| FIRST_VALUE | NO | Not in grammar, not in WindowFunctions, not in BuiltinFunctionName enum |
| LAST_VALUE | NO | Not in grammar, not in WindowFunctions, not in BuiltinFunctionName enum |
| NTH_VALUE | ENUM ONLY | BuiltinFunctionName.java:295 has enum entry, but NO implementation in WindowFunctions.java, NO grammar rule |
| PERCENT_RANK | NO | Not in grammar, not in WindowFunctions, not in BuiltinFunctionName enum |
| CUME_DIST | NO | Not in grammar, not in WindowFunctions, not in BuiltinFunctionName enum |

For unsupported functions: the ANTLR parser will reject them at parse time since they don't match `scalarWindowFunction` (only ROW_NUMBER/RANK/DENSE_RANK) or `aggregateFunction` rules. The user would see a syntax error.

## Key Architecture Notes

- **Frame type is determined by the function, not by user specification** — `WindowFunctionExpression.createWindowFrame()` is called by `WindowOperator` and each function hardcodes its frame type.
- **PeerRowsWindowFrame** accumulates all rows in a partition up to the current peer group (equivalent to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`).
- **CurrentRowWindowFrame** only tracks previous and current row (optimized for ranking).
- **No sliding window** — the comment in WindowFunctionExpression.java explicitly states this.
