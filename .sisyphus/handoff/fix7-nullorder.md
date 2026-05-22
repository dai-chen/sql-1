# Fix 7: NullOrder NPE in Window Functions

## 1. Root Cause

**File:** `sql/src/main/java/org/opensearch/sql/sql/parser/ParserUtils.java:44-50`

```java
public static NullOrder createNullOrder(TerminalNode first, TerminalNode last) {
    if (first != null) {
      return NullOrder.NULL_FIRST;
    } else if (last != null) {
      return NullOrder.NULL_LAST;
    } else {
      return null;  // <-- ROOT CAUSE: returns null when NULLS FIRST/LAST not specified
    }
}
```

**Call chain:**
1. `AstExpressionBuilder.visitWindowFunctionClause()` (line 222) calls `createSortOption(item)` for each `orderByElement` in the window's ORDER BY clause.
2. `ParserUtils.createSortOption()` (line 30) calls `createNullOrder(orderBy.FIRST(), orderBy.LAST())`.
3. When user writes `ORDER BY age` without explicit `NULLS FIRST/LAST`, both tokens are null → `createNullOrder` returns `null`.
4. `SortOption` is constructed with `nullOrder = null`.

**Crash site:** `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java:654`
```java
return switch (opt.getNullOrder()) {  // NPE: switch on null
```

**Why top-level ORDER BY doesn't crash:** `CalciteRelNodeVisitor.java:746` uses `==` comparison:
```java
if (sortOption.getNullOrder() == NULL_LAST) {       // null-safe: false when null
    sortField = context.relBuilder.nullsLast(sortField);
} else {
    sortField = context.relBuilder.nullsFirst(sortField);  // default: NULLS FIRST
}
```
This is null-safe because `null == NULL_LAST` evaluates to `false`, falling through to the else branch (nullsFirst).

## 2. Verdict on Temp Patch

The temp patch:
```java
if (opt.getNullOrder() == null) {
    return field;
}
```

**Problems:**
- Returns `field` without any null ordering → Calcite uses its own default (NULLS LAST for ASC, NULLS FIRST for DESC in most SQL dialects).
- This is **inconsistent** with top-level ORDER BY behavior which defaults to **NULLS FIRST always** (see `CalciteRelNodeVisitor.java:748`).
- The `SortOption` class itself defines `DEFAULT_ASC = new SortOption(ASC, NULL_FIRST)` and `DEFAULT_DESC = new SortOption(DESC, NULL_LAST)` (Sort.java:66-69), confirming the intended defaults.

**Verdict:** The temp patch prevents the crash but introduces semantic inconsistency. The proper fix should either:
- Fix upstream (parser) to always populate NullOrder, OR
- Apply the same default logic as top-level ORDER BY at the call site.

## 3. Recommended Fix

### Option A (Preferred): Fix upstream in `ParserUtils.createSortOption`

Populate NullOrder based on SortOrder when not explicitly specified, matching `SortOption.DEFAULT_ASC`/`DEFAULT_DESC` semantics.

**File:** `sql/src/main/java/org/opensearch/sql/sql/parser/ParserUtils.java`

```java
/** Create sort option from syntax tree node. */
public static SortOption createSortOption(OrderByElementContext orderBy) {
    SortOrder sortOrder = createSortOrder(orderBy.order);
    NullOrder nullOrder = createNullOrder(orderBy.FIRST(), orderBy.LAST());
    if (nullOrder == null) {
        nullOrder = sortOrder == SortOrder.DESC ? NullOrder.NULL_LAST : NullOrder.NULL_FIRST;
    }
    return new SortOption(sortOrder, nullOrder);
}
```

This ensures ALL consumers of `SortOption` (window functions, top-level ORDER BY, any future use) get a non-null `NullOrder`. It matches the defaults defined in `SortOption.DEFAULT_ASC` and `SortOption.DEFAULT_DESC`.

**Impact:** This also fixes `createSortOrder` returning null for implicit ASC. The `getSortOrder() == SortOrder.DESC` check is already null-safe (returns false → treated as ASC), so no additional change needed there.

### Option B (Defensive): Guard at call site with correct defaults

**File:** `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java` (in `translateOrderKeys`)

Replace the switch block (lines 653-657):
```java
              NullOrder nullOrder = opt.getNullOrder();
              if (nullOrder == null) {
                nullOrder = opt.getSortOrder() == SortOrder.DESC
                    ? NullOrder.NULL_LAST : NullOrder.NULL_FIRST;
              }
              return switch (nullOrder) {
                case NULL_LAST -> b.nullsLast(field);
                case NULL_FIRST -> b.nullsFirst(field);
              };
```

**Why Option A is preferred:**
- Fixes the bug at the source — no other consumer can hit the same NPE.
- Consistent with `SortOption.DEFAULT_ASC`/`DEFAULT_DESC` constants already defined.
- Single point of change; no need to audit all `getNullOrder()` call sites.

**Why Option B may still be warranted:**
- Defense-in-depth: even if parser is fixed, a null-safe call site prevents future regressions.
- Consider applying BOTH: Option A as the primary fix, Option B as a belt-and-suspenders guard.

## 4. Test Plan

### Unit Test Location
- `sql/src/test/java/org/opensearch/sql/sql/parser/ParserUtilsTest.java` (if exists, otherwise create)
- Verify `createSortOption` returns non-null NullOrder for all combinations

### Integration Test Location
- **File:** `integ-test/src/test/java/org/opensearch/sql/sql/WindowFunctionIT.java`

### Test Cases to Add

```java
@Test
public void testWindowOrderByWithoutNullsSpec() {
    // Previously caused NPE - implicit NULLS FIRST for ASC
    JSONObject response = new JSONObject(executeQuery(
        "SELECT age, ROW_NUMBER() OVER(ORDER BY age) FROM "
            + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES, "jdbc"));
    // Expect null values first (default for ASC)
    verifyDataRowsInOrder(response,
        rows(null, 1),
        rows(28, 2),
        rows(32, 3),
        rows(33, 4),
        rows(34, 5),
        rows(36, 6),
        rows(36, 7));
}

@Test
public void testWindowOrderByDescWithoutNullsSpec() {
    // Implicit NULLS LAST for DESC
    JSONObject response = new JSONObject(executeQuery(
        "SELECT age, RANK() OVER(ORDER BY age DESC) FROM "
            + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES, "jdbc"));
    // Expect null values last (default for DESC)
    verifyDataRowsInOrder(response,
        rows(36, 1),
        rows(36, 1),
        rows(34, 3),
        rows(33, 4),
        rows(32, 5),
        rows(28, 6),
        rows(null, 7));
}

@Test
public void testWindowOrderByWithExplicitNullsFirst() {
    // Explicit NULLS FIRST should still work
    JSONObject response = new JSONObject(executeQuery(
        "SELECT age, ROW_NUMBER() OVER(ORDER BY age DESC NULLS FIRST) FROM "
            + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES, "jdbc"));
    verifyDataRowsInOrder(response,
        rows(null, 1),
        rows(36, 2),
        rows(36, 3),
        rows(34, 4),
        rows(33, 5),
        rows(32, 6),
        rows(28, 7));
}

@Test
public void testWindowOrderByWithExplicitNullsLast() {
    // Explicit NULLS LAST should still work
    JSONObject response = new JSONObject(executeQuery(
        "SELECT age, ROW_NUMBER() OVER(ORDER BY age NULLS LAST) FROM "
            + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES, "jdbc"));
    verifyDataRowsInOrder(response,
        rows(28, 1),
        rows(32, 2),
        rows(33, 3),
        rows(34, 4),
        rows(36, 5),
        rows(36, 6),
        rows(null, 7));
}
```

### Parser Unit Test (for Option A)

```java
@Test
public void createSortOption_implicitAsc_defaultsNullFirst() {
    // Mock OrderByElementContext with no ASC/DESC token and no FIRST/LAST
    OrderByElementContext ctx = mock(OrderByElementContext.class);
    when(ctx.order).thenReturn(null);
    when(ctx.FIRST()).thenReturn(null);
    when(ctx.LAST()).thenReturn(null);
    SortOption opt = ParserUtils.createSortOption(ctx);
    assertEquals(NullOrder.NULL_FIRST, opt.getNullOrder());
}

@Test
public void createSortOption_explicitDesc_defaultsNullLast() {
    OrderByElementContext ctx = mock(OrderByElementContext.class);
    Token descToken = mock(Token.class);
    when(descToken.getText()).thenReturn("DESC");
    when(ctx.order).thenReturn(descToken);
    when(ctx.FIRST()).thenReturn(null);
    when(ctx.LAST()).thenReturn(null);
    SortOption opt = ParserUtils.createSortOption(ctx);
    assertEquals(NullOrder.NULL_LAST, opt.getNullOrder());
}
```
