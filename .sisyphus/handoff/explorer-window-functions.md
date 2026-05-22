# Window Function Implementation Map

## 1. Directory Tree of Window-Function-Related Code

```
core/src/main/java/org/opensearch/sql/
├── ast/
│   ├── expression/
│   │   ├── WindowBound.java          (AST: window frame bound types)
│   │   ├── WindowFrame.java          (AST: frame type + bounds)
│   │   └── WindowFunction.java       (AST: unresolved window function node)
│   └── tree/
│       └── Window.java               (AST: unresolved plan node for window)
├── analysis/
│   └── WindowExpressionAnalyzer.java (Bridges AST → LogicalWindow plan)
├── expression/window/
│   ├── WindowDefinition.java         (Runtime: partition-by + sort-list)
│   ├── WindowFunctionExpression.java (Runtime: interface for window funcs)
│   ├── WindowFunctions.java          (Registry: registers row_number, rank, dense_rank, brain)
│   ├── aggregation/
│   │   └── AggregateWindowFunction.java (Wraps Aggregator as window func)
│   ├── frame/
│   │   ├── WindowFrame.java              (Interface: Environment + Iterator)
│   │   ├── CurrentRowWindowFrame.java    (For ranking: prev + current row)
│   │   ├── PeerRowsWindowFrame.java      (For aggregates: groups peer rows)
│   │   └── BufferPatternRowsWindowFrame.java (For patterns: extends PeerRows)
│   ├── patterns/
│   │   └── BufferPatternWindowFunction.java (Pattern-matching window func)
│   └── ranking/
│       ├── RankingWindowFunction.java    (Abstract base for ranking funcs)
│       ├── RankFunction.java             (RANK())
│       ├── DenseRankFunction.java        (DENSE_RANK())
│       └── RowNumberFunction.java        (ROW_NUMBER())
├── planner/
│   ├── logical/
│   │   └── LogicalWindow.java        (Logical plan node)
│   ├── physical/
│   │   └── WindowOperator.java       (Physical execution operator)
│   └── DefaultImplementor.java       (LogicalWindow → WindowOperator)
│
sql/src/main/java/org/opensearch/sql/sql/parser/
│   ├── AstExpressionBuilder.java     (SQL grammar → AST WindowFunction)
│   └── context/QuerySpecification.java (Collects window function context)
│
ppl/src/main/java/org/opensearch/sql/ppl/parser/
│   ├── AstBuilder.java               (PPL grammar → AST)
│   └── AstExpressionBuilder.java     (PPL window function expressions)
```

## 2. Key Class Responsibilities

| Class | Responsibility |
|-------|---------------|
| `WindowFunction` (AST) | Unresolved AST node holding function ref, partition-by list, sort list. Created by SQL/PPL parsers. |
| `WindowFrame` (AST) | Unresolved frame specification with FrameType (ROWS/RANGE) and WindowBound (offset/current/unbounded). |
| `WindowExpressionAnalyzer` | Resolves AST `WindowFunction` into `LogicalWindow` plan node, inserting a `LogicalSort` if sort items exist. |
| `WindowDefinition` | Resolved runtime definition holding `partitionByList` and `sortList`. Provides `getAllSortItems()` combining both. |
| `WindowFunctionExpression` | Interface with single method `createWindowFrame(WindowDefinition)` — each window function creates its own frame type. |
| `WindowFrame` (runtime) | Interface extending `Environment<Expression, ExprValue>` and `Iterator<List<ExprValue>>`. Defines `isNewPartition()`, `load()`, `current()`. |
| `CurrentRowWindowFrame` | Frame for ranking functions — tracks previous and current row only. Determines partition boundaries by comparing partition-by values. |
| `PeerRowsWindowFrame` | Frame for aggregate window functions — buffers all rows with same sort key values (peers). Supports cumulative aggregation. |
| `BufferPatternRowsWindowFrame` | Extends PeerRowsWindowFrame for pattern-matching (brain/patterns command). Adds source field preprocessing. |
| `RankingWindowFunction` | Abstract base for ROW_NUMBER, RANK, DENSE_RANK. Creates `CurrentRowWindowFrame`. Implements `valueOf()` with partition reset logic. |
| `RankFunction` | Tracks total rows seen; returns rank with gaps (resets on new partition). |
| `DenseRankFunction` | Returns rank without gaps. |
| `RowNumberFunction` | Simply increments rank counter for each row. |
| `AggregateWindowFunction` | Wraps any `Aggregator` as a window function. Uses `PeerRowsWindowFrame`. Accumulates state across peers, resets on new partition. |
| `WindowFunctions` | Utility class registering `row_number`, `rank`, `dense_rank`, and `brain` into the function repository. |
| `LogicalWindow` | Logical plan node holding `NamedExpression windowFunction` + `WindowDefinition`. |
| `WindowOperator` | Physical plan operator. Creates window frame from function, iterates input via PeekingIterator, enriches each row with window function result. |
| `DefaultImplementor` | Converts `LogicalWindow` → `WindowOperator` (line 87-89). |
| `BufferPatternWindowFunction` | Pattern-matching window function for the `brain`/`patterns` command. |

## 3. Key Abstractions

### WindowFrame (runtime interface)
- Path: `core/src/main/java/org/opensearch/sql/expression/window/frame/WindowFrame.java`
- Extends `Environment<Expression, ExprValue>` (for expression evaluation) and `Iterator<List<ExprValue>>` (for row iteration)
- Key methods: `isNewPartition()`, `load(PeekingIterator)`, `current()`
- Default `resolve()` delegates to `current().bindingTuples()`

### WindowDefinition
- Path: `core/src/main/java/org/opensearch/sql/expression/window/WindowDefinition.java`
- Simple @Data class with `partitionByList` and `sortList`
- `getAllSortItems()` prepends partition-by expressions (ASC, NULL_FIRST) to sort list

### WindowFunctionExpression
- Path: `core/src/main/java/org/opensearch/sql/expression/window/WindowFunctionExpression.java`
- Single method: `WindowFrame createWindowFrame(WindowDefinition definition)`
- Strategy pattern: each function type creates its appropriate frame

### Ranking Functions
- Base: `RankingWindowFunction` → creates `CurrentRowWindowFrame`
- Template method: `rank(CurrentRowWindowFrame)` overridden by each subclass
- `isSortFieldValueDifferent()` compares previous vs current row sort values

### Aggregate Window Functions
- `AggregateWindowFunction` wraps any `Aggregator<AggregationState>`
- Creates `PeerRowsWindowFrame` — groups rows with same ORDER BY values
- Cumulative: iterates all peers, accumulates state, resets on new partition

## 4. Plan Flow (Parser → Execution)

```
SQL/PPL Parser (AstExpressionBuilder)
  → AST: WindowFunction (unresolved expression)
  → WindowExpressionAnalyzer.analyze()
    → Resolves expressions via ExpressionAnalyzer
    → Creates WindowDefinition(partitionByList, sortList)
    → Returns LogicalWindow(LogicalSort(child), namedWindowFunction, windowDefinition)
       (LogicalSort inserted only if sort items exist)
  → DefaultImplementor.visitWindow()
    → WindowOperator(input, windowFunction, windowDefinition)
      → createWindowFrame() delegates to WindowFunctionExpression.createWindowFrame()
      → Execution: load() from PeekingIterator, valueOf() on frame, enrich row
```

Key file paths:
- SQL parser: `sql/src/main/java/org/opensearch/sql/sql/parser/AstExpressionBuilder.java` (lines 207-229)
- PPL parser: `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstExpressionBuilder.java`
- Analyzer: `core/src/main/java/org/opensearch/sql/analysis/WindowExpressionAnalyzer.java`
- Logical plan: `core/src/main/java/org/opensearch/sql/planner/logical/LogicalWindow.java`
- Physical plan: `core/src/main/java/org/opensearch/sql/planner/physical/WindowOperator.java`
- Implementor: `core/src/main/java/org/opensearch/sql/planner/DefaultImplementor.java` (line 87)

## 5. Test Coverage

### Core Window Expression Tests
| Test File | Test Class |
|-----------|-----------|
| `core/src/test/java/org/opensearch/sql/expression/window/ranking/RankingWindowFunctionTest.java` | RankingWindowFunctionTest |
| `core/src/test/java/org/opensearch/sql/expression/window/aggregation/AggregateWindowFunctionTest.java` | AggregateWindowFunctionTest |
| `core/src/test/java/org/opensearch/sql/expression/window/CurrentRowWindowFrameTest.java` | CurrentRowWindowFrameTest |
| `core/src/test/java/org/opensearch/sql/expression/window/frame/PeerRowsWindowFrameTest.java` | PeerRowsWindowFrameTest |
| `core/src/test/java/org/opensearch/sql/expression/window/frame/BufferPatternRowsWindowFrameTest.java` | BufferPatternRowsWindowFrameTest |
| `core/src/test/java/org/opensearch/sql/expression/window/patterns/BufferPatternRowsWindowFunctionTest.java` | BufferPatternRowsWindowFunctionTest |

### Planner Tests
| Test File | Test Class |
|-----------|-----------|
| `core/src/test/java/org/opensearch/sql/planner/physical/WindowOperatorTest.java` | WindowOperatorTest |
| `core/src/test/java/org/opensearch/sql/planner/DefaultImplementorTest.java` | DefaultImplementorTest |
| `core/src/test/java/org/opensearch/sql/planner/logical/LogicalPlanNodeVisitorTest.java` | LogicalPlanNodeVisitorTest |
| `core/src/test/java/org/opensearch/sql/planner/physical/PhysicalPlanNodeVisitorTest.java` | PhysicalPlanNodeVisitorTest |

### Analysis Tests
| Test File | Test Class |
|-----------|-----------|
| `core/src/test/java/org/opensearch/sql/analysis/WindowExpressionAnalyzerTest.java` | WindowExpressionAnalyzerTest |
| `core/src/test/java/org/opensearch/sql/analysis/ExpressionAnalyzerTest.java` | ExpressionAnalyzerTest |
| `core/src/test/java/org/opensearch/sql/analysis/ExpressionReferenceOptimizerTest.java` | ExpressionReferenceOptimizerTest |

### Integration Tests
| Test File | Test Class |
|-----------|-----------|
| `integ-test/src/test/java/org/opensearch/sql/sql/WindowFunctionIT.java` | WindowFunctionIT |
| `integ-test/src/test/java/org/opensearch/sql/sql/PaginationWindowIT.java` | PaginationWindowIT |

### SQL/PPL Parser Tests
| Test File | Test Class |
|-----------|-----------|
| `sql/src/test/java/org/opensearch/sql/sql/parser/AstExpressionBuilderTest.java` | AstExpressionBuilderTest |
| `sql/src/test/java/org/opensearch/sql/sql/parser/context/QuerySpecificationTest.java` | QuerySpecificationTest |
| `ppl/src/test/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizerTest.java` | PPLQueryDataAnonymizerTest |

## 6. Code Smells, Duplications, and Complexity Hotspots

### Design Observations

1. **No true sliding window support**: The `WindowFunctionExpression` javadoc explicitly states "sliding window is not supported." Only cumulative (peer-based) frames exist. The AST has `WindowFrame`/`WindowBound` for ROWS/RANGE with offsets, but the runtime ignores frame bounds — `AggregateWindowFunction.createWindowFrame()` always creates `PeerRowsWindowFrame` regardless of frame spec.

2. **Frame type determined by function, not by frame spec**: The `createWindowFrame()` strategy means the frame definition in the SQL query is effectively ignored at runtime. This is a significant gap — users can write `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` but it won't be honored.

3. **Tight coupling between frame and function**: `AggregateWindowFunction.valueOf()` casts `valueEnv` to `PeerRowsWindowFrame` directly (line 38). `RankingWindowFunction.rank()` takes `CurrentRowWindowFrame` as parameter. This makes it hard to introduce new frame types.

4. **Duplicate partition-boundary logic**: Both `CurrentRowWindowFrame` and `PeerRowsWindowFrame` independently implement partition detection by resolving partition-by expressions and comparing values. This logic could be extracted to a shared base.

5. **PeerRowsWindowFrame complexity**: At 145 lines, it's the most complex frame. It manages peer grouping, partition detection, position tracking, and row buffering all in one class. The `loadAllRows` method loads the entire partition into memory.

6. **BufferPatternRowsWindowFrame extends PeerRowsWindowFrame**: Inheritance used for code reuse rather than true IS-A relationship. The pattern-matching use case is quite different from aggregate windowing.

7. **No TODO/FIXME markers**: The code has no explicit technical debt markers, but the architectural limitations (no sliding window, ignored frame specs) are significant implicit debt.

8. **WindowFunctions registry is minimal**: Only registers `row_number`, `rank`, `dense_rank`, and `brain`. Common window functions like `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` are absent from the core engine (likely pushed down to OpenSearch).

## 7. Recent Git Log (last 30 commits touching window code)

```
e6ab4fbcf Calcite patterns command brain pattern method (#3570)
788da98bb Revert stream pattern method in V2 and implement SIMPLE_PATTERN in Calcite (#3553)
44ff520f0 Improved patterns command with new algorithm (#3263)
b610ce96f [Spotless] Applying Google Code Format for core/src/main files #2 (#1931)
43ceda189 Use query execution start time as the value of now-like functions. (#1047)
b869b6a4b Refactor relevance search functions (#746)
b2d1a1630 Remove amazon license in # and /**/ comment style files
e62ad7329 Update license headers for /**/ style files
3ca2ba558 SQL/PPL and JDBC package renaming (#54)
```

Note: Only 9 commits found (fewer than 30 requested). The window function code has been relatively stable — most recent changes are related to the `patterns`/`brain` command, not core window functionality. The last change to core ranking/aggregate window logic was formatting (Spotless) in PR #1931.

---

## Streaming Windowing (Separate Subsystem)

The `core/src/main/java/org/opensearch/sql/planner/streaming/windowing/` directory contains a separate streaming windowing system (tumbling/sliding time windows) that is unrelated to SQL window functions. It has its own `Window`, `WindowAssigner`, and `WindowTrigger` abstractions for stream processing.

## Calcite Integration

There's also a `core/src/main/java/org/opensearch/sql/calcite/plan/TimeWindow.java` and related Calcite rules that handle window operations in the newer Calcite-based query engine. This is a parallel implementation path.
