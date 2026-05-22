# Fix 3: FROM-less Queries (SELECT without FROM)

## 1. Root Cause

**AST Construction** (`sql/src/main/java/org/opensearch/sql/sql/parser/AstBuilder.java:106`):
When the SQL parser encounters `SELECT 1+1` (no FROM clause), it creates:
```java
Values emptyValue = new Values(ImmutableList.of(emptyList()));
return project.attach(emptyValue);
```
This produces an AST: `Project([1+1]) → Values([[]])` — one row, zero columns.

**RelNode Translation** (`core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java:4183-4191`):
The `visitValues()` method detects this pattern (`rows.size() == 1 && rows.get(0).isEmpty()`) and calls:
```java
context.relBuilder.values(context.relBuilder.getTypeFactory().builder().build());
```

**The Bug**: `RelBuilder.values(RelDataType)` (Calcite `RelBuilder.java:3645`) creates a `LogicalValues` with **zero tuples** (empty tuple list) and zero columns. This is semantically an empty relation — zero rows. When `visitProject` subsequently calls `relBuilder.project(expandedFields)`, the Project computes expressions over zero input rows → zero output rows.

**DataFusion sees 0 rows** because the serialized plan contains a Values node with no tuples, which DataFusion correctly interprets as producing no rows.

## 2. Verdict on Temp Patch

The temp patch at commit `0f40eadfc`:
```java
context.relBuilder.push(LogicalValues.createOneRow(context.relBuilder.getCluster()));
```

**Verdict: CORRECT and IDIOMATIC.**

### Why it's correct:
- `LogicalValues.createOneRow(cluster)` creates a single row with one column `ZERO INTEGER NOT NULL` containing value `0` (Calcite source: `LogicalValues.java:131-142`).
- The `visitProject` method (line 458) then calls `relBuilder.project(expandedFields)` where `expandedFields` contains only the user's expressions (e.g., `RexLiteral(1)`, `RexCall(MOD, 3, 2)`). These expressions don't reference the `ZERO` column — they're all constants/function calls.
- The resulting `LogicalProject` replaces the output schema entirely with the computed columns. The `ZERO` column does NOT leak into the output.
- Calcite's optimizer (`ProjectValuesTransposeRule`) folds `Project([1]) over Values([[0]])` into `Values([[1]])` — confirmed by the existing test at `api/src/test/java/org/opensearch/sql/api/UnifiedSqlSpecTest.java:75` which asserts `LogicalValues(tuples=[[{ 1 }]])`.

### Side effects:
- **None.** The `ZERO` column is an internal implementation detail consumed only by the Project above it. It never appears in the final output schema.
- This is the same pattern used elsewhere in the repo for UNNEST/uncollect (`CalciteRelNodeVisitor.java:4449`): `push(LogicalValues.createOneRow(cluster))` as a "fake input" for correlated subqueries.

### Current state:
The temp patch was **reverted** in a later commit (current HEAD `8113f69d3`). The current code is back to the broken `relBuilder.values(typeFactory.builder().build())`. The fix needs to be re-applied.

## 3. Recommended Fix

**Location**: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java:4187`

**Change**: Replace the zero-row Values with Calcite's canonical one-row dual table.

```java
// BEFORE (broken — produces 0 rows):
context.relBuilder.values(context.relBuilder.getTypeFactory().builder().build());

// AFTER (correct — produces 1 row for Project to evaluate over):
context.relBuilder.push(LogicalValues.createOneRow(context.relBuilder.getCluster()));
```

**No new imports needed** — `org.apache.calcite.rel.logical.LogicalValues` is already imported at line 59.

### Rationale:
1. `LogicalValues.createOneRow` is the canonical Calcite idiom for a "dual table" (one-row source for expression evaluation). It's used in Calcite's own `SqlToRelConverter` for similar purposes.
2. Already used in this repo at line 4449 for UNNEST correlation.
3. The `ZERO` column is stripped by the downstream `LogicalProject` which only projects user-requested expressions.
4. After optimization, `Project([expr]) over Values([[0]])` is folded to `Values([[result]])` by Calcite's built-in rules.

### Alternative considered and rejected:
- `relBuilder.values(new String[]{"ZERO"}, 0)` — functionally equivalent but less readable and not the canonical API.
- Special-casing in the AST→RelNode bridge (e.g., making the parser emit a different AST node) — unnecessary complexity; the fix belongs in `visitValues` where the semantic gap exists.

### Follow-up needed: NONE
The `LogicalProject` above always strips the dummy column. No schema leakage occurs.

## 4. Test Plan

### Unit Test (plan verification)

**File**: `api/src/test/java/org/opensearch/sql/api/UnifiedSqlSpecTest.java`
**Note**: This class is currently `@Ignore`d. When re-enabled, the existing test covers this:

```java
@Test
public void selectWithoutFrom() {
    givenQuery("SELECT 1").assertPlanContains("LogicalValues(tuples=[[{ 1 }]])");
}
```

**Additional test methods to add** (same class or a new non-ignored test class):
```java
@Test
public void selectExpressionWithoutFrom() {
    givenQuery("SELECT 1 + 1").assertPlanContains("LogicalValues(tuples=[[{ 2 }]])");
}

@Test
public void selectFunctionWithoutFrom() {
    givenQuery("SELECT MOD(3, 2)").assertPlanContains("LogicalValues(tuples=[[{ 1 }]])");
}

@Test
public void selectMultipleExpressionsWithoutFrom() {
    givenQuery("SELECT 1, 2, 3").assertPlanContains("LogicalValues(tuples=[[{ 1, 2, 3 }]])");
}
```

### Integration Tests (end-to-end)

**File**: `integ-test/src/test/java/org/opensearch/sql/sql/ArithmeticFunctionIT.java:106`
- `testMod()` — already tests `select mod(3, 2)` and expects `rows(3 % 2)` = `rows(1)`.

**File**: `integ-test/src/test/java/org/opensearch/sql/sql/PaginationIT.java:215`
- `testQueryWithoutFrom()` — tests `SELECT 1` and asserts `total == 1` and `datarows[0][0] == 1`.

**File**: `integ-test/src/test/java/org/opensearch/sql/sql/MathematicalFunctionIT.java:122`
- Tests `select mod(3, 2)` in the mathematical functions context.

### Suggested new integration test (for the unified/analytics path specifically):

**File**: `integ-test/src/test/java/org/opensearch/sql/api/UnifiedQueryOpenSearchIT.java` (or new file)

```java
@Test
public void selectLiteralWithoutFrom() throws Exception {
    JSONObject result = executeQuery("SELECT 1");
    assertEquals(1, result.getInt("total"));
    assertEquals(1, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
}

@Test
public void selectArithmeticWithoutFrom() throws Exception {
    JSONObject result = executeQuery("SELECT 1 + 1");
    assertEquals(1, result.getInt("total"));
    assertEquals(2, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
}

@Test
public void selectFunctionWithoutFrom() throws Exception {
    JSONObject result = executeQuery("SELECT MOD(3, 2)");
    assertEquals(1, result.getInt("total"));
    assertEquals(1, result.getJSONArray("datarows").getJSONArray(0).getInt(0));
}
```

### Summary of test locations:
| Type | File | Status |
|------|------|--------|
| Unit (plan) | `api/src/test/java/org/opensearch/sql/api/UnifiedSqlSpecTest.java:74` | Exists but @Ignored |
| Integration | `integ-test/src/test/java/org/opensearch/sql/sql/PaginationIT.java:215` | Exists, covers SELECT 1 |
| Integration | `integ-test/src/test/java/org/opensearch/sql/sql/ArithmeticFunctionIT.java:106` | Exists, covers mod(3,2) |
| Integration | `integ-test/src/test/java/org/opensearch/sql/api/UnifiedQueryOpenSearchIT.java` | Needs new tests for unified path |
