# Fix 6: HAVING / Aggregate Expression Resolution

## 1. Root Cause

**File:** `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`  
**Default in:** `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java:220`

```java
public T visitAggregateFunction(AggregateFunction node, C context) {
    return visitChildren(node, context);  // returns null for CalciteRexNodeVisitor
}
```

**Trigger scenario:** When a SQL query contains an `AggregateFunction` AST node in a context where the rex visitor is called *after* the aggregate RelNode has already been built:

1. **HAVING clause:** `SELECT name FROM account GROUP BY name HAVING COUNT(*) > 1`
   - The `Filter` node (HAVING) is visited after `Aggregation`. The condition `COUNT(*) > 1` contains `AggregateFunction("COUNT", AllFields)`. The rex visitor is called to translate this to a `RexNode`, but `visitAggregateFunction` was not overridden → returns null → NPE.

2. **SELECT with aggregate wrapped in scalar function:** `SELECT abs(MAX(age)) FROM account`
   - In `visitProject` (CalciteRelNodeVisitor.java:549), the special-case handles `Alias` whose `delegated` is directly an `AggregateFunction`. But when the aggregate is wrapped (e.g., `abs(MAX(age))`), the `Alias.delegated` is a `Function("abs", [AggregateFunction])`. It falls through to `rexVisitor.analyze(alias, context)`, which recursively visits the `Function` args, hitting `visitAggregateFunction` → null → NPE.

**AST structure for `SELECT abs(MAX(age)) FROM account`:**
```
Project
  └─ Alias(name="abs(MAX(age))", delegated=Function("abs", [AggregateFunction("MAX", QualifiedName("age"))]))
Aggregation(aggExprList=[Alias("MAX(age)", AggregateFunction("MAX", QualifiedName("age")))], groupExprList=[])
```

After `Aggregation` is built, the Calcite row type has field `MAX(age)` at index 0. The rex visitor needs to resolve `AggregateFunction("MAX", QualifiedName("age"))` to `RexInputRef(0)`.

**How aggregates are collected:** `QuerySpecification.visitAggregateFunctionCall` (sql/src/main/java/.../context/QuerySpecification.java:223) collects all aggregate calls from SELECT and HAVING into `aggregators`. These become the `aggExprList` of the `Aggregation` AST node. The `CalciteAggCallVisitor` translates them into Calcite `AggCall` objects during `aggregateWithTrimming` (CalciteRelNodeVisitor.java:1439).

## 2. Verdict on Temp Patch

The temp patch (CalciteRexNodeVisitor.java:349-381) resolves `AggregateFunction` by string-matching its canonical name against the current row type field names:

```java
String canonicalName = funcName + "(" + (fieldExpr instanceof AllFields ? "*" : fieldExpr.toString()) + ")";
for (int i = 0; i < fieldNames.size(); i++) {
    if (name.equalsIgnoreCase(canonicalName) || ...) {
        return context.relBuilder.field(i);
    }
}
```

**Issues:**

| Scenario | Works? | Why |
|----------|--------|-----|
| `COUNT(*)` | ✅ | Matches `COUNT(*)` field name |
| `MAX(age)` | ✅ | Matches `MAX(age)` field name |
| `AVG(DISTINCT x)` | ❌ | `toString()` produces `AVG(x)` (no DISTINCT in toString), but Calcite names the field differently for distinct aggregates |
| `MAX(age) + MIN(age)` | ✅ | Each aggregate resolves independently |
| Same aggregate twice (e.g., `MAX(age), abs(MAX(age))`) | ⚠️ | Works but fragile — relies on Calcite not deduplicating field names with suffixes |
| Case mismatch (`count(*)` vs `COUNT(*)`) | ✅ | Uses `equalsIgnoreCase` |
| Prefix match fallback | ❌ DANGEROUS | Could match wrong aggregate (e.g., `SUM(a)` matching `SUM(ab)`) |
| Aggregate with condition (`COUNT(*) FILTER (WHERE x > 1)`) | ❌ | Condition not reflected in toString |
| Renamed fields (Calcite adds `0` suffix for duplicates) | ❌ | String matching breaks |

**Verdict:** The patch is a reasonable stopgap but fragile. The prefix-match fallback is particularly dangerous. The proper fix should use structural matching, not string matching.

## 3. Recommended Fix

### Approach: Aggregate Index Registry in CalcitePlanContext

The proper fix is to maintain a mapping from `AggregateFunction` AST nodes to their output field indices, populated when the aggregate RelNode is built.

**Key insight:** The `QuerySpecification.aggregators` set (which becomes `Aggregation.aggExprList`) contains `Alias(name, AggregateFunction)` nodes. These are the *same* `AggregateFunction` instances that appear in SELECT/HAVING expressions. Since `AggregateFunction` has `@EqualsAndHashCode`, we can use structural equality for lookup.

### Step 1: Add registry to `CalcitePlanContext`

**File:** `core/src/main/java/org/opensearch/sql/calcite/CalcitePlanContext.java`

```java
import org.opensearch.sql.ast.expression.AggregateFunction;

// Add field:
/**
 * Maps each AggregateFunction AST node to its output field index in the
 * post-aggregate row type. Populated during Aggregation node construction,
 * consumed by CalciteRexNodeVisitor.visitAggregateFunction.
 */
@Getter
private final Map<AggregateFunction, Integer> aggregateOutputIndex = new HashMap<>();

// Add method:
public void registerAggregateOutput(AggregateFunction aggFunc, int fieldIndex) {
    aggregateOutputIndex.put(aggFunc, fieldIndex);
}

public Optional<Integer> resolveAggregateOutput(AggregateFunction aggFunc) {
    return Optional.ofNullable(aggregateOutputIndex.get(aggFunc));
}
```

### Step 2: Populate registry after aggregate is built

**File:** `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`

In `aggregateWithTrimming`, after `context.relBuilder.aggregate(...)` (around line 1556), add:

```java
// Register aggregate output indices for resolution in HAVING/SELECT expressions
context.getAggregateOutputIndex().clear();
List<String> postAggFields = context.relBuilder.peek().getRowType().getFieldNames();
int groupKeyCount = reResolved.getLeft().size();
for (int i = 0; i < aggExprList.size(); i++) {
    UnresolvedExpression aggExpr = aggExprList.get(i);
    // Unwrap Alias to get the AggregateFunction
    AggregateFunction aggFunc = extractAggregateFunction(aggExpr);
    if (aggFunc != null) {
        context.registerAggregateOutput(aggFunc, groupKeyCount + i);
    }
}
```

Add helper method:

```java
private AggregateFunction extractAggregateFunction(UnresolvedExpression expr) {
    if (expr instanceof AggregateFunction af) return af;
    if (expr instanceof Alias alias) return extractAggregateFunction(alias.getDelegated());
    return null;
}
```

### Step 3: Replace string-matching in CalciteRexNodeVisitor

**File:** `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`

Replace the temp patch with:

```java
@Override
public RexNode visitAggregateFunction(AggregateFunction node, CalcitePlanContext context) {
    // After aggregation is built, resolve AggregateFunction to a field reference
    // using the registry populated during aggregate construction.
    Optional<Integer> index = context.resolveAggregateOutput(node);
    if (index.isPresent()) {
        return context.relBuilder.field(index.get());
    }
    // Fallback: match by field name in current row type (for edge cases like
    // renamed aggregates after schema reordering)
    List<String> fieldNames = context.relBuilder.peek().getRowType().getFieldNames();
    String canonicalName = node.toString(); // e.g., "MAX(age)"
    for (int i = 0; i < fieldNames.size(); i++) {
        if (fieldNames.get(i).equalsIgnoreCase(canonicalName)) {
            return context.relBuilder.field(i);
        }
    }
    throw new IllegalStateException(
        "Cannot resolve aggregate function " + node + " in post-aggregate context. "
            + "Available fields: " + fieldNames);
}
```

### Why This Works

1. **Structural equality:** `AggregateFunction` uses `@EqualsAndHashCode`. The same aggregate in HAVING/SELECT will match the one registered during aggregation construction.
2. **Handles DISTINCT:** `AggregateFunction.distinct` is part of `@EqualsAndHashCode`, so `AVG(DISTINCT x)` and `AVG(x)` are different keys.
3. **Handles duplicates:** Same aggregate appearing twice (e.g., `MAX(age)` in both `SELECT MAX(age), abs(MAX(age))`) maps to the same index — correct behavior since Calcite deduplicates identical aggregate calls.
4. **No string fragility:** Primary resolution is by object identity/equality, not string matching.
5. **Schema reordering safe:** The index is computed after `context.relBuilder.aggregate()` and `rename()`, reflecting the final post-aggregate schema.

### Important Caveat: Schema Reordering

In `visitAggregation` (line 1745), after `aggregateWithTrimming`, there's a schema reordering step (`context.relBuilder.project(reordered)`). The registry indices must account for this. Two options:

**Option A (simpler):** Populate the registry *after* the reordering project. This means moving registration to after line 1751 in `visitAggregation`.

**Option B (more robust):** Register using field names instead of indices, and resolve by name at lookup time. But this brings back string matching.

**Recommended: Option A** — register after the final project that reorders the schema. The indices in the registry will then match the row type seen by subsequent visitors (Filter for HAVING, Project for SELECT).

Revised registration location — at the end of `visitAggregation` (after the reordering project):

```java
// After schema reordering, register aggregate outputs
List<String> finalFields = context.relBuilder.peek().getRowType().getFieldNames();
context.getAggregateOutputIndex().clear();
for (int i = 0; i < aggExprList.size(); i++) {
    AggregateFunction aggFunc = extractAggregateFunction(aggExprList.get(i));
    if (aggFunc != null) {
        // Find the field index in the reordered schema
        String aggName = aggFunc.toString(); // e.g., "MAX(age)"
        int idx = finalFields.indexOf(aggName);
        if (idx < 0) {
            // Try case-insensitive
            for (int j = 0; j < finalFields.size(); j++) {
                if (finalFields.get(j).equalsIgnoreCase(aggName)) { idx = j; break; }
            }
        }
        if (idx >= 0) {
            context.registerAggregateOutput(aggFunc, idx);
        }
    }
}
```

**Even better approach:** Since `aggregateWithTrimming` already knows the aggregate list order and the group key count, compute the mapping directly:

```java
// In aggregateWithTrimming, after aggregate + rename:
context.getAggregateOutputIndex().clear();
int numGroupKeys = reResolved.getLeft().size();
for (int i = 0; i < aggExprList.size(); i++) {
    AggregateFunction aggFunc = extractAggregateFunction(aggExprList.get(i));
    if (aggFunc != null) {
        context.registerAggregateOutput(aggFunc, numGroupKeys + i);
    }
}
```

Then in `visitAggregation`, after the reordering project, update the indices:

```java
// Update registry after reordering
if (!context.getAggregateOutputIndex().isEmpty()) {
    Map<AggregateFunction, Integer> updated = new HashMap<>();
    List<String> reorderedFields = context.relBuilder.peek().getRowType().getFieldNames();
    for (var entry : context.getAggregateOutputIndex().entrySet()) {
        String fieldName = entry.getKey().toString();
        for (int i = 0; i < reorderedFields.size(); i++) {
            if (reorderedFields.get(i).equalsIgnoreCase(fieldName)) {
                updated.put(entry.getKey(), i);
                break;
            }
        }
    }
    context.getAggregateOutputIndex().clear();
    context.getAggregateOutputIndex().putAll(updated);
}
```

### Simplest Correct Implementation

Given the complexity of schema reordering, the **simplest correct fix** is:

1. Add the registry to `CalcitePlanContext`
2. In `visitAggregateFunction` of `CalciteRexNodeVisitor`, first try the registry, then fall back to name-based lookup (keeping the `equalsIgnoreCase` match but removing the dangerous prefix fallback)
3. Populate the registry at the end of `visitAggregation` (after reordering) using name-based index lookup

This gives us:
- **Primary path:** Structural equality via registry (handles DISTINCT, conditions, etc.)
- **Fallback:** Case-insensitive exact name match (handles edge cases where AST nodes differ but represent the same aggregate)
- **No prefix matching** (removes the dangerous fallback)

## 4. Test Plan

### Test Class

**Unit tests:** `core/src/test/java/org/opensearch/sql/calcite/CalciteRexNodeVisitorTest.java`  
**Integration tests:** `integ-test/src/test/java/org/opensearch/sql/sql/AggregationIT.java` or create new `integ-test/src/test/java/org/opensearch/sql/calcite/remote/CalciteSQLAggregationIT.java`

### Test Queries

| Query | Tests |
|-------|-------|
| `SELECT abs(MAX(age)) FROM account` | Aggregate wrapped in scalar function |
| `SELECT MAX(age) + MIN(age) FROM account` | Two aggregates in arithmetic expression |
| `SELECT name FROM account GROUP BY name HAVING COUNT(*) > 1` | HAVING with aggregate |
| `SELECT MAX(age), abs(MAX(age)) FROM account` | Same aggregate referenced twice |
| `SELECT COUNT(DISTINCT gender) FROM account` | DISTINCT aggregate |
| `SELECT name, COUNT(*) AS cnt FROM account GROUP BY name HAVING cnt > 1` | HAVING with alias (already handled by AstHavingFilterBuilder alias replacement) |
| `SELECT AVG(age) FROM account HAVING AVG(age) > 30` | HAVING without GROUP BY |
| `SELECT name, SUM(balance) FROM account GROUP BY name HAVING SUM(balance) > 10000 ORDER BY SUM(balance)` | HAVING + ORDER BY with same aggregate |

### Existing Test Infrastructure

- `CalciteRexNodeVisitorTest` (core/src/test) — unit tests with mocked RelBuilder
- `integ-test/src/test/java/org/opensearch/sql/sql/AggregationIT.java` — SQL aggregation integration tests
- `integ-test/src/test/java/org/opensearch/sql/legacy/HavingIT.java` — legacy HAVING tests (reference for expected behavior)
- `integ-test/src/test/java/org/opensearch/sql/calcite/remote/CalciteExplainIT.java` — has HAVING in explain test

### Unit Test Pattern (CalciteRexNodeVisitorTest)

```java
@Test
void visitAggregateFunction_resolvesFromRegistry() {
    // Setup: mock relBuilder with post-aggregate row type containing "MAX(age)"
    // Register AggregateFunction("MAX", QualifiedName("age")) → index 0
    // Assert: visitAggregateFunction returns RexInputRef(0)
}

@Test
void visitAggregateFunction_fallsBackToNameMatch() {
    // Setup: empty registry, row type with "MAX(age)" field
    // Assert: visitAggregateFunction still resolves by name
}

@Test  
void visitAggregateFunction_throwsWhenUnresolvable() {
    // Setup: empty registry, row type without matching field
    // Assert: throws IllegalStateException
}
```
