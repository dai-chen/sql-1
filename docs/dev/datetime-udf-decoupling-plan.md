# Plan: Decouple PPL Datetime Functions from OpenSearch UDT Types

## Problem

PPL datetime functions in `PPLBuiltinOperators` and `PPLFuncImpTable` are tightly coupled to
OpenSearch-specific UDT types (`EXPR_TIMESTAMP`, `EXPR_DATE`, `EXPR_TIME`) backed by `VARCHAR`.
This leaks OpenSearch adapter concerns into the unified query API (PPL parser + Calcite planner),
which should be engine-agnostic.

When a data source like S3 via Spark PPL connects through the unified query API, it shouldn't need
to know about OpenSearch's `ExprValue`/UDT machinery. Datetime fields from any engine should map to
standard Calcite `TIMESTAMP`/`DATE`/`TIME` types, and PPL datetime functions should work on them.

## Current Architecture

### Type Flow

```
OpenSearch date field
  → OpenSearchTypeFactory.convertExprTypeToRelDataType()
  → ExprCoreType.TIMESTAMP → EXPR_TIMESTAMP(VARCHAR)     ← UDT wrapping VARCHAR
  → PPLBuiltinOperators UDFs accept EXPR_TIMESTAMP(VARCHAR)
  → Runtime: String → ExprValue.fromObjectValue() → compute → .valueForCalcite() → String
```

### Why UDTs Exist

OpenSearch `date_nanos` has nanosecond precision. The UDT-as-VARCHAR approach preserves arbitrary
precision by keeping datetime values as formatted strings throughout the Calcite pipeline.

### Where UDT Types Are Consumed

| Location | How UDT is used |
|----------|----------------|
| `OpenSearchTypeFactory.convertExprTypeToRelDataType()` | Schema: maps `ExprCoreType.TIMESTAMP` → `EXPR_TIMESTAMP(VARCHAR)` |
| `PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE` etc. | Return types: UDFs return UDT types |
| `PPLOperandTypes.DATETIME_OR_STRING` etc. | Type checkers: accept UDT or VARCHAR |
| `UserDefinedFunctionUtils.convertToExprValues()` | Runtime bridge: converts Calcite values → `ExprValue` |
| `ExtendedRexBuilder.makeCast()` | Casting: `CAST(x AS TIMESTAMP)` → calls `PPLBuiltinOperators.TIMESTAMP` UDF |
| `OpenSearchTypeFactory.isTimeBasedType()` | Checks both standard SQL datetime types AND UDT types |
| `BinnableField` / binning handlers | Checks `isTimeBasedType()` to decide time-based binning |
| `PlanUtils` | Checks `isTimeBasedType()` for sort/comparison handling |
| `PredicateAnalyzer` (opensearch module) | Checks `ExprUDT.EXPR_TIMESTAMP` for pushdown |
| `AggregateAnalyzer` (opensearch module) | Converts RelDataType → ExprType for aggregation pushdown |
| `ExtendedRelJson` (opensearch module) | Serializes/deserializes UDT type info |

### Key Observation

The **schema** is the boundary. `UnifiedQueryContext.Builder.catalog(name, schema)` accepts any
Calcite `Schema`. The `UnifiedQueryTestBase` already uses standard `SqlTypeName` (no UDTs). UDTs
only enter through `AbstractOpenSearchTable.getRowType()` → `OpenSearchTypeFactory.convertSchema()`.

The problem: `PPLFuncImpTable.populate()` registers UDT-aware UDFs as the **only** implementation.
When a non-OpenSearch schema provides `SqlTypeName.TIMESTAMP`, the UDF return types are still UDT,
and the runtime still does string roundtrips.

## Design: Type-Polymorphic UDFs

### Core Principle

PPL datetime functions are **language-spec functions** — they belong in the unified query API, not
in the OpenSearch adapter. They should remain as single registrations in `PPLFuncImpTable`. But
their type handling must be polymorphic: work correctly whether the input is a UDT (VARCHAR-backed)
or a standard Calcite datetime type.

The change is at the **boundary**, not inside each UDF's logic.

### Approach: Polymorphic Boundary Conversion

```
Standard Calcite path:
  Input: long (epoch millis) → ExprValue.fromObjectValue() → compute → .valueForCalcite() → long
  
OpenSearch UDT path (unchanged):
  Input: String → ExprValue.fromObjectValue() → compute → .valueForCalcite() → String
```

The UDF implementations (`DatePartFunction`, `CurrentFunction`, etc.) continue to use `ExprValue`
internally. The boundary methods — `convertToExprValues()` (input) and `valueForCalcite()` (output)
— become type-aware.

### Why This Works

1. **Type checkers already accept both.** `PPLOperandTypes.DATETIME_OR_STRING` uses
   `OperandTypes.DATE_OR_TIMESTAMP.or(OperandTypes.CHARACTER)`, which matches both standard
   `TIMESTAMP`/`DATE` and UDT (VARCHAR).

2. **`ExprValue` already handles multiple input types.** `ExprValueUtils.fromObjectValue()` can
   construct `ExprTimestampValue` from strings, longs, `Instant`, `LocalDateTime`, etc.

3. **Individual UDF implementations don't change.** They all work through `ExprValue` internally.
   Only the boundary conversion needs to know whether the Calcite runtime representation is
   `String` (UDT) or `long`/`int` (standard).

### What Changes

Only 3 areas need modification:

#### 1. Return Type Inference (~15 lambdas)

Make return types match the input type representation:

```java
// Before (always returns UDT):
public static final SqlReturnTypeInference TIMESTAMP_FORCE_NULLABLE =
    ReturnTypes.explicit(UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT);

// After (returns UDT if input is UDT, standard if input is standard):
public static final SqlReturnTypeInference TIMESTAMP_FORCE_NULLABLE =
    opBinding -> {
      if (hasUDTInput(opBinding)) {
        return UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
      }
      return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, true);
    };
```

For zero-arg functions (`NOW()`, `UTC_TIMESTAMP()`, etc.), the return type can't be inferred from
inputs. These need a context flag or always return standard types (the OpenSearch adapter overrides
them anyway via `ExtendedRexBuilder.makeCast()` when needed).

#### 2. Input Boundary: `convertToExprValues()` (1 method)

Currently converts Calcite runtime values to `ExprValue` assuming string representation for
datetime types. Needs to also handle standard Calcite representations:

```java
// In UserDefinedFunctionUtils.convertToExprValues():
// Current: always wraps as Object → ExprValueUtils.fromObjectValue(obj, exprType)
// This already works for both paths because fromObjectValue handles:
//   - String "2024-01-01 00:00:00" → ExprTimestampValue (UDT path)
//   - Long 1704067200000L → ExprTimestampValue (standard path, if fromObjectValue supports it)
```

The key question: does `ExprValueUtils.fromObjectValue(long, ExprCoreType.TIMESTAMP)` work today?
If not, add that case. Calcite represents:
- `TIMESTAMP` as `long` (epoch millis)
- `DATE` as `int` (days since epoch)
- `TIME` as `int` (millis since midnight)

#### 3. Output Boundary: `valueForCalcite()` (1 method or new method)

Currently `ExprTimestampValue.valueForCalcite()` returns `String` (via `value()`). For standard
Calcite types, it should return `long` (epoch millis).

Option A — Make `valueForCalcite()` context-aware (needs to know target type).
Option B — Add a parallel method `valueForCalciteStandard()` that returns the native representation.
Option C — Handle the conversion in the UDF implementor's code generation, not in `ExprValue`.

**Option C is cleanest**: In the `NotNullImplementor.implement()` code generation, after calling the
ExprValue-based computation, check the return type and convert:

```java
// In the generated code (pseudo):
ExprValue result = computeFunction(...);
if (returnType is standard TIMESTAMP) {
    return result.timestampValue().toEpochMilli();  // long
} else {
    return result.valueForCalcite();  // String (UDT path)
}
```

This can be done once in `UserDefinedFunctionUtils.adaptExprMethodToUDF()` rather than in each UDF.

## Function Categories

### Category A: Can Map to Calcite Standard Operators (~13 functions)

These have exact equivalents in `SqlStdOperatorTable`. In the unified query API, they *could*
resolve to standard operators when inputs are standard types. However, with the polymorphic approach,
this is an **optimization, not a requirement** — the UDFs work on standard types too.

| PPL Function | Calcite Standard Operator | Aliases |
|---|---|---|
| `YEAR(x)` | `EXTRACT(YEAR FROM x)` | |
| `QUARTER(x)` | `EXTRACT(QUARTER FROM x)` | |
| `MONTH(x)` | `EXTRACT(MONTH FROM x)` | `MONTH_OF_YEAR` |
| `DAY(x)` | `EXTRACT(DAY FROM x)` | `DAYOFMONTH`, `DAY_OF_MONTH` |
| `HOUR(x)` | `EXTRACT(HOUR FROM x)` | `HOUR_OF_DAY` |
| `MINUTE(x)` | `EXTRACT(MINUTE FROM x)` | `MINUTE_OF_HOUR` |
| `SECOND(x)` | `EXTRACT(SECOND FROM x)` | `SECOND_OF_MINUTE` |
| `MICROSECOND(x)` | `EXTRACT(MICROSECOND FROM x)` | |
| `NOW()` | `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `LOCALTIME` |
| `CURRENT_DATE()` | `CURRENT_DATE` | `CURDATE` |
| `CURRENT_TIME()` | `CURRENT_TIME` | `CURTIME` |
| `EXTRACT(unit, x)` | `EXTRACT(unit FROM x)` | |
| `LAST_DAY(x)` | `SqlStdOperatorTable.LAST_DAY` | |

**Decision**: Keep as UDFs for now (polymorphic approach handles them). Optionally replace with
standard operators in a follow-up for cleaner RelNode plans (better for Calcite optimizer rules).

### Category B: PPL-Specific UDFs (~30 functions)

No Calcite standard equivalent. Remain as UDFs. With the polymorphic boundary, they work on both
standard and UDT types without code changes.

Full list: `DATE_FORMAT`, `TIME_FORMAT`, `ADDDATE`, `SUBDATE`, `DATE_ADD`, `DATE_SUB`, `ADDTIME`,
`SUBTIME`, `DATEDIFF`, `TIMESTAMPDIFF`, `TIMESTAMPADD`, `CONVERT_TZ`, `YEARWEEK`, `WEEKDAY`,
`WEEK`, `DAY_OF_WEEK`, `DAY_OF_YEAR`, `DAYNAME`, `MONTHNAME`, `FROM_DAYS`, `TO_DAYS`,
`TO_SECONDS`, `FROM_UNIXTIME`, `UNIX_TIMESTAMP`, `MAKEDATE`, `MAKETIME`, `PERIOD_ADD`,
`PERIOD_DIFF`, `STR_TO_DATE`, `SEC_TO_TIME`, `TIME_TO_SEC`, `TIMEDIFF`, `GET_FORMAT`, `SYSDATE`,
`UTC_DATE`, `UTC_TIME`, `UTC_TIMESTAMP`, `STRFTIME`, `DATETIME`, `TIMESTAMP`, `DATE`, `TIME`,
`MINUTE_OF_DAY`.

## Implementation Plan

### Phase 1: Make `ExprValueUtils.fromObjectValue()` Handle Standard Calcite Representations

Ensure `fromObjectValue(Object, ExprType)` handles:
- `Long` + `TIMESTAMP` → `ExprTimestampValue` (from epoch millis)
- `Integer` + `DATE` → `ExprDateValue` (from days since epoch)
- `Integer` + `TIME` → `ExprTimeValue` (from millis since midnight)

These cases may already work or need small additions. This is the input boundary.

**Files**: `core/.../data/model/ExprValueUtils.java`
**Risk**: Low — additive, doesn't change existing behavior.

### Phase 2: Make Output Boundary Type-Aware

In `UserDefinedFunctionUtils.adaptExprMethodToUDF()` and `adaptExprMethodWithPropertiesToUDF()`,
the generated code currently calls `exprValue.valueForCalcite()` which returns `String` for
datetime types. Add a conversion step that checks the declared return type:

```java
// Pseudo-code for generated implementor:
Expression result = callExprMethod(...);  // returns ExprValue
Expression calciteValue = Expressions.call(result, "valueForCalcite");

// NEW: if return type is standard datetime, convert String → native representation
if (isStandardDatetime(call.getType())) {
    calciteValue = convertToStandardRepresentation(calciteValue, call.getType());
}
return calciteValue;
```

Alternatively, add `ExprValue.valueForCalcite(RelDataType targetType)` that returns the appropriate
representation based on whether the target is UDT or standard.

**Files**: `core/.../calcite/utils/UserDefinedFunctionUtils.java`
**Risk**: Low-Medium — changes code generation path, but only for standard-type returns.

### Phase 3: Make Return Type Inference Polymorphic

Update `PPLReturnTypes` to return standard types when inputs are standard:

```java
public static SqlReturnTypeInference polymorphicTimestamp() {
    return opBinding -> {
        for (int i = 0; i < opBinding.getOperandCount(); i++) {
            if (OpenSearchTypeFactory.isUserDefinedType(opBinding.getOperandType(i))) {
                return NULLABLE_TIMESTAMP_UDT;
            }
        }
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, true);
    };
}
```

For zero-arg functions (`NOW()`, `UTC_TIMESTAMP()`, etc.), default to standard types. The OpenSearch
adapter path already wraps results in UDT via `ExtendedRexBuilder.makeCast()` when the target
column type is UDT.

**Files**: `core/.../calcite/utils/PPLReturnTypes.java`, individual UDF classes that override
`getReturnTypeInference()`.
**Risk**: Medium — changes return types in the RelNode plan, which affects downstream type checks.

### Phase 4: Audit Downstream UDT Type Checks

Ensure code that checks for datetime types handles both representations:

| Check | Current | After |
|-------|---------|-------|
| `OpenSearchTypeFactory.isTimeBasedType()` | Checks both standard SQL types AND UDT | ✅ Already works |
| `BinnableField` | Calls `isTimeBasedType()` | ✅ Already works |
| `PlanUtils` | Calls `isTimeBasedType()` | ✅ Already works |
| `ExtendedRexBuilder.makeCast()` | Checks `isUserDefinedType(type)` | Only triggers for UDT targets — OK |
| `PredicateAnalyzer` (opensearch) | Checks `ExprUDT.EXPR_TIMESTAMP` | Only runs in OpenSearch path — OK |
| `AggregateAnalyzer` (opensearch) | `convertRelDataTypeToExprType()` | Handles both via `convertSqlTypeNameToExprType` — OK |

**Risk**: Low — `isTimeBasedType()` already handles both. OpenSearch-specific checks only run in
the OpenSearch adapter path where UDTs are present.

### Phase 5: Verify

1. Run existing PPL Calcite ITs (OpenSearch path) — should pass unchanged since OpenSearch schema
   still provides UDT types, so the UDT code path is exercised.
2. Add new standalone Calcite tests using `UnifiedQueryTestBase` with standard `SqlTypeName.TIMESTAMP`
   columns — verify datetime functions work on standard types.
3. Verify the Spark PPL integration path (if available) works with standard types.

## Execution Order

| Step | Change | Risk | Validates |
|------|--------|------|-----------|
| 1 | `ExprValueUtils.fromObjectValue()`: handle long/int datetime inputs | Low | Input boundary |
| 2 | `UserDefinedFunctionUtils`: type-aware output conversion | Low-Med | Output boundary |
| 3 | `PPLReturnTypes`: polymorphic return type inference | Medium | Type propagation |
| 4 | Audit `isTimeBasedType()` usages | Low | No regressions |
| 5 | Run existing ITs | — | OpenSearch path unchanged |
| 6 | Add standard-type ITs in `UnifiedQueryTestBase` | Low | New path works |

## Rejected Approaches

### Post-RelNode UDT Unwrapping (RelShuttle)

Idea: After `UnifiedQueryPlanner.plan()` produces a RelNode, walk it with a RelShuttle and replace
`EXPR_TIMESTAMP(VARCHAR)` → `SqlTypeName.TIMESTAMP`.

**Rejected because**: UDT types aren't just metadata — they determine runtime code generation. The
UDF implementors generate Janino code that receives `String` (VARCHAR) and calls
`ExprValueUtils.fromObjectValue(string, TIMESTAMP)`. Changing the type in the RelNode without
changing the generated code causes runtime type mismatches (Calcite would pass `long` but the code
expects `String`). You'd need to rewrite every RexCall too, which is equivalent to the full
rewrite approach but more fragile.

### Dual Registration (Two Implementations Per Function)

Idea: Register each function twice — once with standard types, once with UDT types.

**Rejected because**: Doubles the implementation surface. The polymorphic boundary approach achieves
the same result by changing ~3 shared utility methods instead of ~43 individual UDF classes.

### External Registry for OpenSearch Overrides

Idea: Default registrations use standard types; OpenSearch adapter overrides via
`externalFunctionRegistry`.

**Rejected because**: These are PPL language-spec functions, not engine-specific. Having the
"real" implementations in the external registry inverts the dependency — the language spec would
depend on the adapter to provide working datetime functions. The polymorphic approach keeps a single
implementation that works everywhere.

## Open Questions

1. **Zero-arg function return types**: `NOW()`, `UTC_TIMESTAMP()`, `SYSDATE()`, etc. have no input
   types to infer from. Should they default to standard `TIMESTAMP` (and let OpenSearch adapter
   cast to UDT downstream), or use a context flag from `CalcitePlanContext`?

2. **`TIMESTAMP(9)` precision**: Should the unified query API use `TIMESTAMP(3)` (millis, Calcite
   default) or `TIMESTAMP(9)` (nanos)? This affects whether nanosecond precision is preserved in
   the standard-type path. Each engine adapter controls schema precision independently.

3. **`DAY_OF_WEEK` semantics**: PPL follows MySQL (1=Sunday). Calcite's `EXTRACT(DOW)` follows ISO
   (1=Monday). The UDF preserves MySQL semantics regardless of type representation, so this is not
   affected by the decoupling — but worth documenting as a PPL language decision.

4. **Incremental rollout**: Can this be done function-by-function? Yes — the boundary changes
   (Phases 1-2) are shared infrastructure. Once those are in place, individual UDFs can be made
   polymorphic by updating their return type inference (Phase 3) one at a time. Functions that
   return `INTEGER`/`BIGINT`/`VARCHAR` (e.g., `YEAR`, `DATEDIFF`, `DATE_FORMAT`) need no return
   type changes at all — only functions that return datetime types need Phase 3.
