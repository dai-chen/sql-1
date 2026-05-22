# PR #5450: Fix `CalciteRelNodeVisitor` for Limit, Alias, Subquery, and Values

## 1. PR Metadata

| Field | Value |
|-------|-------|
| **Title** | Fix `CalciteRelNodeVisitor` for Limit, Alias, Subquery, and Values |
| **Author** | dai-chen |
| **Target Branch** | opensearch-project:main |
| **Source Branch** | dai-chen:fix/calcite-visitor-select-path |
| **Status** | Draft (Open) |
| **Commits** | 2 |
| **Latest Commit** | 563a054 |

## 2. PR Description (Verbatim)

> ### Description
>
> Extends `CalciteRelNodeVisitor` to handle several AST node types that were previously unsupported
> in the unified SQL query path:
> * **`visitLimit`** — Adds LIMIT/OFFSET support via `relBuilder.limit()`
> * **Alias in project list** — When an aggregate function output already exists as a field (post-aggregation), reference it directly instead of re-analyzing (which returns null)
> * **`visitRelationSubquery`** — Handles derived tables (`SELECT ... FROM (SELECT ...) AS t`)
> * **`visitValues`** — Treats a single empty row as a dual-table for `SELECT 1` (no FROM clause)
> * **`visitAggregation`** — Makes `bucketNullable` argument lookup null-safe with `getOrDefault`
>
> ### Related Issues
>
> Part of #5248

## 3. Linked Issues

- Part of [#5248](https://github.com/opensearch-project/sql/issues/5248)

## 4. Changed Files Summary

| # | File Path | Summary |
|---|-----------|---------|
| 1 | `api/src/test/java/org/opensearch/sql/api/UnifiedQueryPlannerSqlTest.java` | Added 10 new test methods for window functions, LIMIT/OFFSET, aggregates, aliases, derived tables, and SELECT without FROM |
| 2 | `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java` | Added visitLimit, visitRelationSubquery, fixed expandProjectFields for aggregate aliases, fixed visitValues for dual-table, made bucketNullable null-safe |
| 3 | `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java` | **WINDOW** - Rewrote visitWindowFunction to handle AggregateFunction (DISTINCT, ORDER BY), added pure window function path for RANK/ROW_NUMBER/DENSE_RANK |
| 4 | `core/src/main/java/org/opensearch/sql/calcite/utils/PlanUtils.java` | **WINDOW** - Added translateOrderKeys utility, added RANK/DENSE_RANK cases in makeOver, added `distinct` parameter to makeOver signature |
| 5 | `core/src/main/java/org/opensearch/sql/expression/function/BuiltinFunctionName.java` | **WINDOW** - Registered row_number, rank, dense_rank in WINDOW_FUNC_MAPPING, added isPureWindowFunction helper |
| 6 | `core/src/main/java/org/opensearch/sql/expression/function/PPLFuncImpTable.java` | Registered ISNULL as alias for IS_NULL operator |

## 5. Window Function Diffs (FULL)

### 5.1 CalciteRexNodeVisitor.java — visitWindowFunction rewrite

```diff
diff --git a/core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java b/core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java
index c2ce4a740e..3b85f5c18e 100644
--- a/core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java
+++ b/core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java
@@ -42,6 +42,7 @@ import org.apache.calcite.util.TimestampString;
 import org.apache.logging.log4j.util.Strings;
 import org.opensearch.sql.ast.AbstractNodeVisitor;
+import org.opensearch.sql.ast.expression.AggregateFunction;
 import org.opensearch.sql.ast.expression.Alias;
 import org.opensearch.sql.ast.expression.And;
 import org.opensearch.sql.ast.expression.Between;
@@ -563,15 +564,32 @@ public RexNode visitFunction(Function node, CalcitePlanContext context) {
   @Override
   public RexNode visitWindowFunction(WindowFunction node, CalcitePlanContext context) {
-    Function windowFunction = (Function) node.getFunction();
-    List arguments =
-        windowFunction.getFuncArgs().stream().map(arg -> analyze(arg, context)).toList();
+    String funcName;
+    List arguments;
+    final boolean isDistinct;
+    if (node.getFunction() instanceof AggregateFunction aggFunc) {
+      funcName = aggFunc.getFuncName();
+      isDistinct = Boolean.TRUE.equals(aggFunc.getDistinct());
+      List argExprs = new java.util.ArrayList<>();
+      if (aggFunc.getField() != null) {
+        argExprs.add(aggFunc.getField());
+      }
+      argExprs.addAll(aggFunc.getArgList());
+      arguments = argExprs.stream().map(arg -> analyze(arg, context)).toList();
+    } else {
+      Function windowFunction = (Function) node.getFunction();
+      funcName = windowFunction.getFuncName();
+      isDistinct = false;
+      arguments = windowFunction.getFuncArgs().stream().map(arg -> analyze(arg, context)).toList();
+    }
     List partitions =
         node.getPartitionByList().stream()
             .map(arg -> analyze(arg, context))
             .map(this::extractRexNodeFromAlias)
             .toList();
-    return BuiltinFunctionName.ofWindowFunction(windowFunction.getFuncName())
+    List orderKeys =
+        PlanUtils.translateOrderKeys(node.getSortList(), expr -> analyze(expr, context), context);
+    return BuiltinFunctionName.ofWindowFunction(funcName)
         .map(
             functionName -> {
               RexNode field = arguments.isEmpty() ? null : arguments.getFirst();
@@ -579,6 +597,18 @@ public RexNode visitWindowFunction(WindowFunction node, CalcitePlanContext conte
               (arguments.isEmpty() || arguments.size() == 1)
                   ? Collections.emptyList()
                   : arguments.subList(1, arguments.size());
+              // Pure window functions (ROW_NUMBER, RANK, DENSE_RANK) are not registered
+              // in aggFunctionRegistry, so skip validation for them.
+              if (BuiltinFunctionName.isPureWindowFunction(functionName)) {
+                return PlanUtils.makeOver(
+                    context,
+                    functionName,
+                    field,
+                    args,
+                    partitions,
+                    orderKeys,
+                    node.getWindowFrame());
+              }
               List nodes =
                   PPLFuncImpTable.INSTANCE.validateAggFunctionSignature(
                       functionName, field, args, context.rexBuilder);
@@ -586,24 +616,24 @@ public RexNode visitWindowFunction(WindowFunction node, CalcitePlanContext conte
                   ? PlanUtils.makeOver(
                       context,
                       functionName,
+                      isDistinct,
                       nodes.getFirst(),
                       nodes.size() <= 1
                           ? Collections.emptyList()
                           : nodes.subList(1, nodes.size()),
                       partitions,
-                      List.of(),
+                      orderKeys,
                       node.getWindowFrame())
                   : PlanUtils.makeOver(
                       context,
                       functionName,
+                      isDistinct,
                       field,
                       args,
                       partitions,
-                      List.of(),
+                      orderKeys,
                       node.getWindowFrame());
             })
         .orElseThrow(
-            () ->
-                new UnsupportedOperationException(
-                    "Unexpected window function: " + windowFunction.getFuncName()));
+            () -> new UnsupportedOperationException("Unexpected window function: " + funcName));
   }
```

### 5.2 PlanUtils.java — translateOrderKeys + RANK/DENSE_RANK + distinct parameter

```diff
diff --git a/core/src/main/java/org/opensearch/sql/calcite/utils/PlanUtils.java b/core/src/main/java/org/opensearch/sql/calcite/utils/PlanUtils.java
index 4d2dae4bd6..24c5561df3 100644
--- a/core/src/main/java/org/opensearch/sql/calcite/utils/PlanUtils.java
+++ b/core/src/main/java/org/opensearch/sql/calcite/utils/PlanUtils.java
@@ -69,6 +69,7 @@ import org.opensearch.sql.ast.Node;
 import org.opensearch.sql.ast.expression.IntervalUnit;
 import org.opensearch.sql.ast.expression.SpanUnit;
+import org.opensearch.sql.ast.expression.UnresolvedExpression;
 import org.opensearch.sql.ast.expression.WindowBound;
 import org.opensearch.sql.ast.expression.WindowFrame;
 import org.opensearch.sql.ast.tree.Relation;
@@ -163,9 +164,54 @@ static IntervalUnit spanUnitToIntervalUnit(SpanUnit unit) {
     }
   }
 
+  /**
+   * Translates a list of (SortOption, UnresolvedExpression) pairs into Calcite RexNodes suitable
+   * for use as window function ORDER BY keys, applying DESC and NULL FIRST/LAST directives via
+   * RelBuilder.
+   */
+  static List translateOrderKeys(
+      List<
+              org.apache.commons.lang3.tuple.Pair<
+                  org.opensearch.sql.ast.tree.Sort.SortOption, UnresolvedExpression>>
+          sortList,
+      java.util.function.Function analyzer,
+      CalcitePlanContext context) {
+    return sortList.stream()
+        .map(
+            pair -> {
+              RexNode sortField = analyzer.apply(pair.getRight());
+              if (pair.getLeft().getSortOrder()
+                  == org.opensearch.sql.ast.tree.Sort.SortOrder.DESC) {
+                sortField = context.relBuilder.desc(sortField);
+              }
+              if (pair.getLeft().getNullOrder()
+                  == org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST) {
+                sortField = context.relBuilder.nullsLast(sortField);
+              } else if (pair.getLeft().getNullOrder()
+                  == org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST) {
+                sortField = context.relBuilder.nullsFirst(sortField);
+              }
+              return sortField;
+            })
+        .toList();
+  }
+
+  static RexNode makeOver(
+      CalcitePlanContext context,
+      BuiltinFunctionName functionName,
+      RexNode field,
+      List argList,
+      List partitions,
+      List orderKeys,
+      @Nullable WindowFrame windowFrame) {
+    return makeOver(
+        context, functionName, false, field, argList, partitions, orderKeys, windowFrame);
+  }
+
   static RexNode makeOver(
       CalcitePlanContext context,
       BuiltinFunctionName functionName,
+      boolean distinct,
       RexNode field,
       List argList,
       List partitions,
@@ -216,6 +262,22 @@ static RexNode makeOver(
             true,
             lowerBound,
             upperBound);
+      case RANK:
+        return withOver(
+            context.relBuilder.aggregateCall(SqlStdOperatorTable.RANK),
+            partitions,
+            orderKeys,
+            true,
+            lowerBound,
+            upperBound);
+      case DENSE_RANK:
+        return withOver(
+            context.relBuilder.aggregateCall(SqlStdOperatorTable.DENSE_RANK),
+            partitions,
+            orderKeys,
+            true,
+            lowerBound,
+            upperBound);
       case NTH_VALUE:
         return withOver(
             context.relBuilder.aggregateCall(SqlStdOperatorTable.NTH_VALUE, field, argList.get(0)),
@@ -226,7 +288,7 @@ static RexNode makeOver(
             upperBound);
       default:
         return withOver(
-            makeAggCall(context, functionName, false, field, argList),
+            makeAggCall(context, functionName, distinct, field, argList),
             partitions,
             orderKeys,
             rows,
```

### 5.3 BuiltinFunctionName.java — Window function registration + isPureWindowFunction

```diff
diff --git a/core/src/main/java/org/opensearch/sql/expression/function/BuiltinFunctionName.java b/core/src/main/java/org/opensearch/sql/expression/function/BuiltinFunctionName.java
index 14f058a75d..682b32a45a 100644
--- a/core/src/main/java/org/opensearch/sql/expression/function/BuiltinFunctionName.java
+++ b/core/src/main/java/org/opensearch/sql/expression/function/BuiltinFunctionName.java
@@ -425,6 +425,9 @@ public enum BuiltinFunctionName {
         .put("dc", BuiltinFunctionName.DISTINCT_COUNT_APPROX)
         .put("distinct_count", BuiltinFunctionName.DISTINCT_COUNT_APPROX)
         .put("pattern", BuiltinFunctionName.INTERNAL_PATTERN)
+        .put("row_number", BuiltinFunctionName.ROW_NUMBER)
+        .put("rank", BuiltinFunctionName.RANK)
+        .put("dense_rank", BuiltinFunctionName.DENSE_RANK)
         .build();
 
   public static Optional of(String str) {
@@ -441,6 +444,17 @@ public static Optional ofWindowFunction(String functionName
         WINDOW_FUNC_MAPPING.getOrDefault(functionName.toLowerCase(Locale.ROOT), null));
   }
 
+  /**
+   * Pure window functions (no aggregate semantics, take no field argument). They are not registered
+   * in the aggregate function registry, so callers must skip aggregate validation.
+   */
+  private static final Set PURE_WINDOW_FUNCTIONS =
+      Set.of(ROW_NUMBER, RANK, DENSE_RANK);
+
+  public static boolean isPureWindowFunction(BuiltinFunctionName functionName) {
+    return PURE_WINDOW_FUNCTIONS.contains(functionName);
+  }
+
   public static final Set COMPARATORS =
       Set.of(
           BuiltinFunctionName.EQUAL,
```

### 5.4 Window Function Tests (in UnifiedQueryPlannerSqlTest.java)

```diff
+  @Test
+  public void testSqlWindowFunctionWithOrderBy() {
+    givenQuery(
+        """
+        SELECT name, SUM(age) OVER (PARTITION BY department ORDER BY id) AS running_sum
+        FROM catalog.employees\
+        """)
+    .assertPlan(
+        """
+        LogicalProject(name=[$1], running_sum=[SUM($2) OVER (PARTITION BY $3 ORDER BY $0 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])
+          LogicalTableScan(table=[[catalog, employees]])
+        """);
+  }
+
+  @Test
+  public void testSqlWindowRankFunction() {
+    givenQuery(
+        """
+        SELECT name, RANK() OVER (ORDER BY age DESC) AS rnk
+        FROM catalog.employees\
+        """)
+    .assertPlan(
+        """
+        LogicalProject(name=[$1], rnk=[RANK() OVER (ORDER BY $2 DESC)])
+          LogicalTableScan(table=[[catalog, employees]])
+        """);
+  }
+
+  @Test
+  public void testSqlWindowDistinctAggregate() {
+    givenQuery(
+        """
+        SELECT name, COUNT(DISTINCT department) OVER (PARTITION BY department) AS dist_cnt
+        FROM catalog.employees\
+        """)
+    .assertPlan(
+        """
+        LogicalProject(name=[$1], dist_cnt=[COUNT(DISTINCT $3) OVER (PARTITION BY $3)])
+          LogicalTableScan(table=[[catalog, employees]])
+        """);
+  }
```

## 6. Non-Window-Related File Summaries

| File | Summary |
|------|---------|
| `CalciteRelNodeVisitor.java` | Added `visitLimit` (LIMIT/OFFSET), `visitRelationSubquery` (derived tables), fixed alias projection for aggregates, fixed `visitValues` for dual-table, made `bucketNullable` null-safe |
| `PPLFuncImpTable.java` | Registered `ISNULL` as alias for `SqlStdOperatorTable.IS_NULL` |
| `UnifiedQueryPlannerSqlTest.java` | Added tests for ISNULL, LIMIT OFFSET, aggregate with alias, GROUP BY, SELECT with alias, derived table subquery, SELECT without FROM |

## 7. Reviewer Comments

**No human reviewer comments yet.** The PR is in Draft status and awaiting review from code owners.

**Automated bot (github-actions) comments:**
- PR Reviewer Guide noting: tests present, no security concerns, no TODOs, no multiple themes
- **Possible Issues flagged by bot:**
  1. Alias field name collision: When aggregate alias matches an existing field name, code silently uses wrong field
  2. COUNT(*) OVER: If `aggFunc.getField()` is null (COUNT(*)), the code correctly guards with null check but `aggFunc.getArgList()` could be null causing NPE
- **Code Suggestions (bot):**
  1. Add null-safety for `aggFunc.getArgList()` before `addAll()` (importance: 8/10)
  2. Verify null-safety for `aggFieldName` (importance: 7/10)  
  3. Add type safety for `bucketNullable` cast (importance: 7/10)
  4. Fix alias application order — check `aggFieldName` before `aliasName` (importance: 8/10)

## 8. Test Files

| Test File | What It Tests |
|-----------|---------------|
| `api/src/test/java/org/opensearch/sql/api/UnifiedQueryPlannerSqlTest.java` | 10 new tests: window function with ORDER BY, RANK window function, COUNT DISTINCT OVER window, ISNULL function, LIMIT OFFSET, aggregate with alias, GROUP BY without bucket nullable, SELECT with alias, derived table subquery, SELECT without FROM |

## 9. TODOs / FIXMEs / WIP Markers

- The PR is marked as **Draft** (WIP)
- The automated reviewer noted: "✅ No TODO sections" in the code itself

## 10. Commit Messages

**Commit 1 (eb0e527):**
> fix(calcite): handle Limit, Alias projection, RelationSubquery, and Values dual-table in visitor

**Commit 2 (563a054):**
> feat(calcite): support window functions, RANK/DENSE_RANK, and register ISNULL
