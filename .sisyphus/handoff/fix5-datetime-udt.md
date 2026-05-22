# Fix 5: DatetimeUdtNormalizeRule Aggregate Handling

## 1. Root Cause

**File:** `api/src/main/java/org/opensearch/sql/api/spec/datetime/DatetimeUdtNormalizeRule.java`  
**Original code (commit 3ed472ea7):** Lines 32-57

The original `DatetimeUdtNormalizeRule` extends `RelHomogeneousShuttle` and overrides `visit(RelNode)`. It calls `super.visit(other)` which recursively visits children and rebuilds the tree via `Aggregate.copy()`. The RexShuttle then normalizes `RexCall` return types from UDT (e.g., `EXPR_TIMESTAMP` → `TIMESTAMP(9)`).

**The problem:** When an aggregate function like `MAX(CAST(datetime0 AS timestamp))` is used, the aggregate's input Project gets its RexCall types normalized (e.g., `CAST` return type changes from UDT VARCHAR to TIMESTAMP(9)). However, `AggregateCall.getType()` still holds the **old** type (VARCHAR) because `AggregateCall` is not a `RexNode` — it's never visited by the RexShuttle.

**Failing assertion:** `Aggregate.<init>` at line 177 of `org/apache/calcite/rel/core/Aggregate.java` (Calcite 1.41.0):
```java
assert typeMatchesInferred(aggCall, Litmus.THROW);
```
This calls `typeMatchesInferred()` (line 447-458) which uses `aggFunction.inferReturnType(callBinding)` to re-infer the type from the **new** input row type. Since the input's column is now `TIMESTAMP(9)` (after normalization), `MAX` infers `TIMESTAMP(9)`, but `aggCall.type` is still `VARCHAR`. Result:
```
aggCall type: VARCHAR != inferred type: TIMESTAMP(9)
```

The assertion fires inside `LogicalAggregate.copy()` → `new LogicalAggregate(...)` → `super(Aggregate.<init>)`.

## 2. Verdict on Temp Patch (commit 0f40eadfc)

### Bugs in the patch:

1. **Duplicate import** (confirmed): Line 12-13 of the diff both import `org.apache.calcite.rel.core.AggregateCall;`. This compiles (Java ignores duplicate imports) but is a code quality issue.

2. **Uses deprecated `AggregateCall.create` overload**: The 8-arg overload `create(aggFunction, distinct, approximate, argList, filterArg, collation, type, name)` is `@Deprecated` (line 293 of AggregateCall.java). It also **drops** `ignoreNulls`, `rexList`, and `distinctKeys` fields (hardcodes `ignoreNulls=false`, `rexList=[]`, `distinctKeys=null`). This could lose information for `IGNORE NULLS` aggregates or aggregates with `WITHIN GROUP`.

3. **Approach is correct but incomplete**: The patch intercepts `Aggregate` before `super.visit()`, manually visits children, normalizes `AggregateCall` types, and rebuilds via `agg.copy()`. This avoids the assertion. However:
   - It **skips** the RexShuttle for the Aggregate node itself (returns early). If the Aggregate had any RexNode expressions with UDT types (unlikely but possible in HAVING filters stored as RexNodes), they wouldn't be normalized.
   - `agg.copy()` correctly preserves `traitSet`, `groupSet`, `groupSets`, and hints (via `LogicalAggregate.copy` which passes `hints` through).

### Comparison with commit 7be95751e (the more complete version):

The earlier commit on a feature branch had a more thorough approach:
- Manually visits ALL children (not just for Aggregate)
- Handles `LogicalProject` (refreshes `RexInputRef` types from new child row type)
- Uses the non-deprecated `AggregateCall.create` with `ignoreNulls`, `rexList`, `distinctKeys`, `groupCount`, and `input` (which re-infers type from the new input)
- That version passes `null` for type, letting Calcite re-infer it — **this is the cleanest approach** because it guarantees the assertion will pass.

### `Aggregate.copy` vs `LogicalAggregate.create`:

- `agg.copy(traitSet, input, groupSet, groupSets, aggCalls)` is **correct** and **idiomatic**. It preserves traits and hints (LogicalAggregate.copy passes `this.hints`).
- `LogicalAggregate.create(...)` would also work but doesn't preserve hints from the original node.
- Verdict: **`agg.copy()` is the right choice**.

## 3. Recommended Fix

The cleanest minimal fix uses `AggregateCall.create` with `groupCount` and `input` parameters, passing `null` for type so Calcite re-infers it. This avoids hardcoding the type and guarantees consistency with the assertion.

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping;

/**
 * Temporary patch that rewrites datetime UDT return types on RexCall nodes to standard Calcite
 * types.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class DatetimeUdtNormalizeRule extends RelHomogeneousShuttle {

  static final DatetimeUdtNormalizeRule INSTANCE = new DatetimeUdtNormalizeRule();

  @Override
  public RelNode visit(RelNode other) {
    // For Aggregate nodes, normalize children first, then let Calcite re-infer
    // AggregateCall types from the updated input. This avoids the assertion in
    // Aggregate.<init> that checks aggCall.type == inferredType.
    if (other instanceof Aggregate agg) {
      RelNode newInput = agg.getInput().accept(this);
      List<AggregateCall> newCalls =
          agg.getAggCallList().stream()
              .map(
                  call ->
                      AggregateCall.create(
                          call.getAggregation(),
                          call.isDistinct(),
                          call.isApproximate(),
                          call.ignoreNulls(),
                          call.rexList,
                          call.getArgList(),
                          call.filterArg,
                          call.distinctKeys,
                          call.collation,
                          agg.getGroupCount(),
                          newInput,
                          null, // let Calcite re-infer type from new input
                          call.getName()))
              .toList();
      return agg.copy(
          agg.getTraitSet(), newInput, agg.getGroupSet(), agg.getGroupSets(), newCalls);
    }

    RelNode visited = super.visit(other);
    RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

    return visited.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            call = (RexCall) super.visitCall(call);
            Optional<UdtMapping> mapping = UdtMapping.fromUdtType(call.getType());
            if (mapping.isEmpty()) {
              return call;
            }

            // Normalize UDT return type to standard Calcite DATE/TIME/TIMESTAMP
            RelDataType stdType = toStdType(mapping.get(), typeFactory, call.getType().isNullable());
            return call.clone(stdType, call.getOperands());
          }
        });
  }

  private static RelDataType toStdType(
      UdtMapping mapping, RelDataTypeFactory typeFactory, boolean nullable) {
    SqlTypeName stdTypeName = mapping.getStdType();
    RelDataType baseType =
        stdTypeName.allowsPrec()
            ? typeFactory.createSqlType(
                stdTypeName, typeFactory.getTypeSystem().getMaxPrecision(stdTypeName))
            : typeFactory.createSqlType(stdTypeName);
    return typeFactory.createTypeWithNullability(baseType, nullable);
  }
}
```

### Key differences from temp patch:
1. **No duplicate import** — single `import org.apache.calcite.rel.core.AggregateCall;`
2. **Uses non-deprecated `AggregateCall.create` with `groupCount` + `input` + `null` type** — Calcite re-infers the return type from the normalized input, guaranteeing the assertion passes
3. **Preserves `ignoreNulls`, `rexList`, `distinctKeys`** — no information loss
4. **Keeps `toStdType` helper** — good refactoring from the temp patch
5. **`super.visit()` still runs for non-Aggregate nodes** — the RexShuttle handles all other node types (Project, Filter, etc.) correctly since they don't have the AggregateCall assertion issue

### Should `super.visit()` run after Aggregate handling?

**No.** The Aggregate early-return is correct because:
- `super.visit()` would call `agg.getInput().accept(this)` again (double-visiting children)
- The RexShuttle on the rebuilt Aggregate is unnecessary — Aggregate nodes don't contain RexCall expressions directly (their expressions are in AggregateCall, not in the node's RexNode list)
- The `accept(RexShuttle)` on an Aggregate is a no-op in Calcite's default implementation

## 4. Test Plan

### Existing test files:
- `api/src/test/java/org/opensearch/sql/api/spec/datetime/DatetimeExtensionTest.java` (PPL path)
- `api/src/test/java/org/opensearch/sql/api/spec/datetime/DatetimeExtensionSqlTest.java` (SQL path)

### No existing aggregate test — needs to be added.

### Recommended test location:
`api/src/test/java/org/opensearch/sql/api/spec/datetime/DatetimeExtensionSqlTest.java`

### Test cases to add:

```java
@Test
public void testAggregateWithDatetimeCast() {
  // This was the failing query: MAX(CAST(datetime AS timestamp))
  givenQuery("SELECT MAX(created_at) FROM catalog.events")
      .assertPlan(
          """
          LogicalProject(EXPR$0=[CAST($0):VARCHAR])
            LogicalAggregate(group=[{}], EXPR$0=[MAX($4)])
              LogicalTableScan(table=[[catalog, events]])
          """);
}

@Test
public void testAggregateWithDatetimeUdf() {
  // Aggregate over a datetime UDF result
  givenQuery("SELECT MAX(TIMESTAMP(name)) FROM catalog.events")
      .assertReturnType("TIMESTAMP", TIMESTAMP, 9);
}

@Test
public void testAggregateWithGroupByAndDatetime() {
  givenQuery("SELECT id, MIN(created_at) FROM catalog.events GROUP BY id")
      .assertReturnType("TIMESTAMP", TIMESTAMP, 9);
}
```

### Integration test (if applicable):
Search for `calcs` table in integration tests:
- Query: `SELECT MAX(CAST(datetime0 AS timestamp)) FROM calcs`
- This exercises the full path: SQL parse → CalciteRelNodeVisitor → DatetimeUdtNormalizeRule → execution
