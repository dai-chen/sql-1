# DatetimeUdtNormalizeRule Aggregate Bug Investigation

## 1. Pipeline Trace

### Unit Test Path
```
SQL string
→ UnifiedQueryTestBase.givenQuery(query)
  → UnifiedQueryPlanner.plan(query)                    [api/.../UnifiedQueryPlanner.java:59]
    → CustomVisitorStrategy.plan(query)                [api/.../UnifiedQueryPlanner.java:107]
      → UnifiedQueryParser.parse(query)                (SQL V2 parser)
      → CalciteRelNodeVisitor.analyze(ast, planCtx)    [core/.../CalciteRelNodeVisitor.java]
      → preserveCollation(logical)
    → postAnalysisRules loop:                          [api/.../UnifiedQueryPlanner.java:64]
      1. DatetimeUdtNormalizeRule.INSTANCE.accept()    [api/.../DatetimeUdtNormalizeRule.java]
      2. DatetimeOutputCastRule.INSTANCE.accept()      [api/.../DatetimeOutputCastRule.java]
  → QueryAssert(plan)
    → assertPlanContains("LogicalAggregate")           [api/.../QueryPlanAssertion.java:50]
```

### AE-via-REST Path
```
SQL string
→ RestUnifiedQueryAction.execute(query, queryType, ...)  [plugin/.../RestUnifiedQueryAction.java:117]
  → buildContext(queryType, profiling)                    [line 172]
    → OpenSearchSchemaBuilder.buildSchema(clusterState)   [line 176]
  → UnifiedQueryPlanner(context).plan(query)              [line 120]
    → (same as above from here)
  → analyticsEngine.execute(plan, planContext, listener)
```

Both paths use the SAME `UnifiedQueryPlanner.plan()` method and the SAME `postAnalysisRules()`.

## 2. Where DatetimeUdtNormalizeRule Runs

`DatetimeUdtNormalizeRule` is registered in:
- `DatetimeExtension.postAnalysisRules()` → `[DatetimeUdtNormalizeRule.INSTANCE, DatetimeOutputCastRule.INSTANCE]`
  - File: `api/src/main/java/org/opensearch/sql/api/spec/datetime/DatetimeExtension.java:25`

It runs as part of the `postAnalysisRules` loop in `UnifiedQueryPlanner.plan()`:
- File: `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java:64`
- Code: `for (var shuttle : context.getLangSpec().postAnalysisRules()) { plan = plan.accept(shuttle); }`

**Both `assertPlan()` and `assertPlanContains()` operate on the SAME fully-constructed plan** (returned by `planner.plan(query)`). The rule runs BEFORE either assertion. The difference between the two assertions is purely string matching — they do NOT affect which rules run.

## 3. What Changes Types (The Trigger)

The bug triggers when:
1. A datetime UDF (e.g., `TIMESTAMP()`, `DATE()`, `TIME()`) produces a **UDT return type** (e.g., `EXPR_TIMESTAMP VARCHAR`)
2. This UDT-typed expression is used as input to an aggregate function (e.g., `MAX(TIMESTAMP(name))`)
3. `DatetimeUdtNormalizeRule` visits the tree bottom-up via `RelHomogeneousShuttle`:
   - Visits the Aggregate's child (Project containing `TIMESTAMP(name)`)
   - The RexShuttle normalizes `TIMESTAMP(name)` from UDT type to standard `TIMESTAMP(9)`
   - The child Project is now modified (different output type)
4. `RelShuttleImpl.visitChildren()` detects the child changed and calls `Aggregate.copy(traitSet, [newChild])`
5. `Aggregate.copy()` → `Aggregate.<init>()` validates: `aggCall.type == inferredType`
6. **ASSERTION FAILS**: `aggCall.type` is still `EXPR_TIMESTAMP VARCHAR` (from original inference), but `inferredType` is now `TIMESTAMP(9)` (re-inferred from the normalized input)

### Where UDT types originate:
- `PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE` → `UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT`
  - File: `core/src/main/java/org/opensearch/sql/calcite/utils/PPLReturnTypes.java:28-29`
  - File: `core/src/main/java/org/opensearch/sql/calcite/utils/UserDefinedFunctionUtils.java:55-56`
- Used by `TimestampFunction.getReturnTypeInference()`
  - File: `core/src/main/java/org/opensearch/sql/expression/function/udf/datetime/TimestampFunction.java:48`

### Where the assertion fires:
- `Aggregate.typeMatchesInferred()` → `RelOptUtil.eq()`
  - Calcite: `org.apache.calcite.rel.core.Aggregate:454` / `org.apache.calcite.plan.RelOptUtil:2223`

## 4. Why the Unit Test Passes Without Fix

**`testAggregateOverDatetimeColumn` (`SELECT MAX(created_at)`) passes without the fix because:**

The test schema (`SimpleTable`) defines `created_at` with `SqlTypeName.TIMESTAMP` via Calcite's standard `RelDataTypeFactory.Builder.add()`. This produces a standard `BasicSqlType(TIMESTAMP)`, NOT a UDT type.

When `SELECT MAX(created_at)` is planned:
- The plan is: `LogicalAggregate(MAX($0)) → LogicalProject($4) → LogicalTableScan`
- The Project's output field `$4` is a simple `RexInputRef` with standard `TIMESTAMP` type
- `DatetimeUdtNormalizeRule`'s RexShuttle checks `UdtMapping.fromUdtType(call.getType())` — but `$4` is a `RexInputRef`, not a `RexCall`, so the shuttle doesn't even visit it
- Even if it did, `UdtMapping.fromUdtType()` returns `Optional.empty()` for standard types (it only matches `AbstractExprRelDataType` instances)
- **The Project doesn't change → the Aggregate is never rebuilt → no assertion fires**

**`testAggregateOverTimestampFunction` (`SELECT MAX(TIMESTAMP(name))`) FAILS without the fix because:**
- `TIMESTAMP(name)` is a `RexCall` with UDT return type `EXPR_TIMESTAMP VARCHAR`
- The RexShuttle normalizes it to standard `TIMESTAMP(9)`
- The Project changes → Aggregate is rebuilt → assertion fires

## 5. Reproducing the Bug in a Unit Test

The existing test `testAggregateOverTimestampFunction` already reproduces the bug correctly:

```java
@Test
public void testAggregateOverTimestampFunction() {
    // This triggers the Aggregate assertion when UDT normalization changes input types
    givenQuery("SELECT MAX(TIMESTAMP(name)) FROM catalog.events")
        .assertPlanContains("LogicalAggregate");
}
```

**Why this works:** `TIMESTAMP(name)` calls the `TimestampFunction` UDF which returns `NULLABLE_TIMESTAMP_UDT` (a UDT type). When `DatetimeUdtNormalizeRule` normalizes this to standard `TIMESTAMP(9)`, the Aggregate's input type changes, triggering the assertion.

**The schema does NOT need to change.** The bug is triggered by UDT-returning function calls in the aggregate's input, not by the schema column types themselves.

### Additional test variants that also trigger the bug (verified):
- `SELECT MAX(DATE(name)) FROM catalog.events` — DATE() returns UDT
- `SELECT MIN(TIMESTAMP(name)) FROM catalog.events` — same mechanism

### Tests that do NOT trigger the bug (verified):
- `SELECT MAX(created_at) FROM catalog.events` — standard type, no UDT
- `SELECT MIN(hire_date) FROM catalog.events` — standard type, no UDT
- `SELECT MAX(created_at) FROM catalog.events GROUP BY id` — standard type

## 6. Empirical Evidence

### Without Fix (FAIL):
```
DatetimeExtensionSqlTest > testAggregateOverTimestampFunction FAILED
    java.lang.AssertionError: type mismatch:
    aggCall type:
    EXPR_TIMESTAMP VARCHAR
    inferred type:
    TIMESTAMP(9)
        at org.apache.calcite.util.Litmus.lambda$static$0(Litmus.java:31)
        at org.apache.calcite.plan.RelOptUtil.eq(RelOptUtil.java:2223)
        at org.apache.calcite.rel.core.Aggregate.typeMatchesInferred(Aggregate.java:454)
        at org.apache.calcite.rel.core.Aggregate.<init>(Aggregate.java:177)
        at org.apache.calcite.rel.logical.LogicalAggregate.<init>(LogicalAggregate.java:72)
        at org.apache.calcite.rel.logical.LogicalAggregate.copy(LogicalAggregate.java:154)
        at org.apache.calcite.rel.RelShuttleImpl.visitChild(RelShuttleImpl.java:63)
        at org.apache.calcite.rel.RelShuttleImpl.visitChildren(RelShuttleImpl.java:73)
        at org.apache.calcite.rel.RelShuttleImpl.visit(RelShuttleImpl.java:151)
        at org.opensearch.sql.api.spec.datetime.DatetimeUdtNormalizeRule.visit(...)
```

### With Fix (PASS):
```
DatetimeExtensionSqlTest > testAggregateOverTimestampFunction PASSED
BUILD SUCCESSFUL in 4s
```

Full logs saved to:
- `.sisyphus/handoff/test-without-fix.log`
- `.sisyphus/handoff/test-with-fix.log`

## 7. Regarding the AE Path

The user stated that `OpenSearchSchemaBuilder` returns standard Calcite datetime types. If that's truly the case, then `SELECT MAX(created_at)` alone would NOT trigger the bug in the AE path either. The bug in AE must be triggered by one of:

1. **A query involving a datetime UDF** (e.g., `SELECT MAX(TIMESTAMP(field))`, `SELECT MAX(DATE(field))`)
2. **The schema actually using UDT types** — `OpenSearchTypeFactory.convertExprTypeToRelDataType()` (line 173-175 of `OpenSearchTypeFactory.java`) converts `ExprCoreType.TIMESTAMP` → `createUDT(EXPR_TIMESTAMP)`. If `OpenSearchSchemaBuilder` uses this method internally, the schema WOULD have UDT types despite the user's claim.

**Recommendation:** Verify with the AE team exactly which query triggered the bug and whether `OpenSearchSchemaBuilder.buildSchema()` calls `OpenSearchTypeFactory.convertExprTypeToRelDataType()` internally.

## 8. Recommended Test Configuration

The existing `testAggregateOverTimestampFunction` is the correct test for this fix. The other two aggregate tests (`testAggregateOverDatetimeColumn`, `testAggregateMinOverDateColumn`) test a different scenario (standard types) and correctly pass both with and without the fix — they serve as regression tests ensuring the Aggregate special-case doesn't break normal aggregates.

**Recommended final test set:**
```java
@Test
public void testAggregateOverDatetimeColumn() {
    // Standard datetime column — no UDT normalization needed, should always pass
    givenQuery("SELECT MAX(created_at) FROM catalog.events")
        .assertPlanContains("LogicalAggregate");
}

@Test
public void testAggregateMinOverDateColumn() {
    // Standard date column — no UDT normalization needed, should always pass
    givenQuery("SELECT MIN(hire_date) FROM catalog.events")
        .assertPlanContains("LogicalAggregate");
}

@Test
public void testAggregateOverTimestampFunction() {
    // UDT-returning function in aggregate input — triggers the Aggregate.<init> assertion
    // without the Aggregate special-case in DatetimeUdtNormalizeRule
    givenQuery("SELECT MAX(TIMESTAMP(name)) FROM catalog.events")
        .assertPlanContains("LogicalAggregate");
}
```
