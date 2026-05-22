# Fix 4: NUL Separator Stripping in `AnalyticsExecutionEngine.buildSchema()`

## 1. Root Cause

### Origin of the `\0` Convention

The `\0` (NUL byte) separator in field names was **intentionally introduced** in commit `de26834e2` in `CalciteRexNodeVisitor.visitAlias()`:

```java
// File: core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java
// Commit: de26834e2 (line ~351 in diff)
// For SQL queries, encode expression name and alias in field name so the response
// builder can reconstruct both (V2 compatibility: name=expr, alias=AS-alias).
if (context.queryType == QueryType.SQL && !Strings.isEmpty(node.getAlias())) {
    String exprName = node.getName() != null ? node.getName() : aliasName;
    aliasName = exprName + "\u0000" + node.getAlias();
}
```

**Purpose**: The legacy SQL engine returns schema with `name=expression_text` and `alias=user_alias` as separate fields. The `\0` encoding was a hack to carry both pieces of information in a single Calcite field name string, so the response formatter could split them back out.

### Current State

The `\0` encoding was **removed** from `visitAlias` in commit `d2d6a0218` / `3211f0b79`. The current `visitAlias` (at `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java:343`) correctly uses only the alias:

```java
return context.relBuilder.alias(
    expr, Strings.isEmpty(node.getAlias()) ? node.getName() : node.getAlias());
```

### Why the `\0` Still Appears

The `\0` can still appear in field names through:

1. **Calcite's `SqlToRelConverter` path** — If any query goes through `CalciteNativeStrategy` (Calcite native `SqlParser → SqlValidator → SqlToRelConverter`), the `SqlValidatorImpl.addToSelectList()` and `SqlToRelConverter.convertNonAggregateSelectList()` can produce field names with Calcite's internal `\0` separator convention when disambiguating duplicate names.

2. **Calcite's `RelRoot.project()`** — In the native path, `RelRoot.of(result, validatedRowType, kind)` takes field names from the validated row type. The validated row type field names come from `SqlValidatorImpl` which uses `SqlValidatorUtil.uniquify()` with `EXPR_SUGGESTER`.

3. **Residual from intermediate commits** — On the `feature/analytics-engine-compat-report` branch, the `\0` encoding was present in earlier commits and may have been baked into test expectations or cached plan states.

### How the SQL Parser Creates Alias Nodes

File: `sql/src/main/java/org/opensearch/sql/sql/parser/AstBuilder.java:290`

```java
private UnresolvedExpression visitSelectItem(SelectElementContext ctx) {
    String name = StringUtils.unquoteIdentifier(getTextInQuery(ctx.expression(), query));
    UnresolvedExpression expr = visitAstExpression(ctx.expression());
    if (ctx.alias() == null) {
        return Alias.newAliasAllowMetaMetaField(name, expr, null);
    } else {
        String alias = StringUtils.unquoteIdentifier(ctx.alias().getText());
        return Alias.newAliasAllowMetaMetaField(name, expr, alias);
    }
}
```

For `SELECT ISNULL(lastname) AS name`:
- `Alias.name` = `"ISNULL(lastname)"` (expression text)
- `Alias.alias` = `"name"` (user alias)
- `Alias.delegated` = `Function("ISNULL", Field("lastname"))`

## 2. Verdict on Temp Patch

The temp patch in `buildSchema()`:

```java
String name = field.getName();
int nul = name.indexOf('\0');
if (nul >= 0) {
    name = name.substring(nul + 1);
}
```

### Assessment: Safe but Defensive

**Pros:**
- Correctly extracts the alias (the part after `\0`) which is what the user expects as the column name
- Does not affect field names without `\0` (no-op for clean names)
- No column uniqueness issue: the alias is what the user specified, and SQL requires aliases to be unique within a SELECT

**Cons:**
- Masks the upstream issue rather than fixing it
- If two columns have the same alias (which SQL should reject), stripping could produce duplicates — but this is already a SQL validation error
- Adds runtime overhead (negligible: one `indexOf` per field)

**Verdict**: The temp patch is **safe** and does not cause column name uniqueness issues. However, it's a symptom-level fix. The proper fix is to ensure the `\0` never enters the field name in the first place.

## 3. Recommended Fix

### Option A (Preferred): Ensure `visitAlias` Never Produces `\0`

**Status**: Already done. The current `visitAlias` at `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java:343` does NOT encode `\0`. The fix is to **remove the stripping code** from `buildSchema()` since it's no longer needed.

However, if the `\0` can still leak through from Calcite's internal infrastructure (e.g., `SqlToRelConverter` path, `trimUnusedFields`, or plan optimization), then:

### Option B (Recommended): Keep Defensive Stripping at Boundary

Keep the stripping in `buildSchema()` as a defensive measure against Calcite's internal `\0` convention:

```java
private Schema buildSchema(List<RelDataTypeField> fields) {
    List<Schema.Column> columns = new ArrayList<>();
    for (RelDataTypeField field : fields) {
        ExprType exprType = convertType(field.getType());
        String name = field.getName();
        // Strip Calcite's internal "\0" separator (expression\0alias → alias)
        int nul = name.indexOf('\0');
        if (nul >= 0) {
            name = name.substring(nul + 1);
        }
        columns.add(new Schema.Column(name, null, exprType));
    }
    return new Schema(columns);
}
```

**Rationale**: This is the cleanest minimal fix because:
1. It's a 3-line addition at the output boundary
2. It handles ALL sources of `\0` (whether from the custom visitor, native Calcite path, or plan optimization)
3. It's idempotent — no effect on clean field names
4. The `\0` character is never a valid SQL identifier character, so stripping it is always safe

### Ranking

1. **Option B** (keep defensive stripping) — Recommended. Non-invasive, handles all edge cases.
2. **Option A** (remove stripping, trust upstream) — Only if you can guarantee no Calcite path ever produces `\0` in field names. Risky given Calcite's internal conventions.

## 4. Test Plan

### Unit Test

**File**: `core/src/test/java/org/opensearch/sql/executor/analytics/AnalyticsExecutionEngineTest.java`

Add test method:

```java
@Test
void executeRelNode_nulSeparatorStrippedFromFieldNames() {
    // Simulate Calcite's internal \0 separator in field names
    RelNode relNode = mockRelNodeWithRawNames(
        "ISNULL(lastname)\0name", SqlTypeName.BOOLEAN,
        "COUNT(*)\0cnt", SqlTypeName.BIGINT);
    Iterable<Object[]> rows = Collections.singletonList(new Object[] {false, 42L});
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);

    // Field names should have \0 prefix stripped
    assertEquals("name", response.getSchema().getColumns().get(0).getName());
    assertEquals("cnt", response.getSchema().getColumns().get(1).getName());
}

@Test
void executeRelNode_cleanFieldNamesUnaffected() {
    // Field names without \0 should pass through unchanged
    RelNode relNode = mockRelNode("name", SqlTypeName.VARCHAR, "age", SqlTypeName.INTEGER);
    Iterable<Object[]> rows = Collections.singletonList(new Object[] {"Alice", 30});
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);

    assertEquals("name", response.getSchema().getColumns().get(0).getName());
    assertEquals("age", response.getSchema().getColumns().get(1).getName());
}
```

Helper method to add:
```java
private RelNode mockRelNodeWithRawNames(Object... nameTypePairs) {
    // Same as mockRelNode but allows \0 in field names
    return mockRelNode(nameTypePairs);  // mockRelNode already supports arbitrary strings
}
```

### Integration Test

**File**: `integ-test/src/test/java/org/opensearch/sql/sql/ConditionalIT.java:152`

Existing test `isnullShouldPassJDBC()`:
```java
@Test
public void isnullShouldPassJDBC() throws IOException {
    JSONObject response =
        executeJdbcRequest("SELECT ISNULL(lastname) AS name FROM " + TEST_INDEX_ACCOUNT);
    // With the fix, schema name should be the alias "name", not "ISNULL(lastname)\0name"
    assertEquals("name", response.query("/schema/0/name"));
}
```

### Sample Queries to Test

| Query | Expected Column Name |
|-------|---------------------|
| `SELECT ISNULL(lastname) AS name FROM t` | `name` |
| `SELECT COUNT(*) AS cnt FROM t` | `cnt` |
| `SELECT IFNULL(lastname, 'unknown') AS name FROM t GROUP BY name` | `name` |
| `SELECT NULLIF(lastname, 'unknown') AS name FROM t` | `name` |
| `SELECT age + 1 AS next_age FROM t` | `next_age` |
| `SELECT firstname FROM t` (no alias) | `firstname` |

### Planner-Level Test

**File**: `api/src/test/java/org/opensearch/sql/api/UnifiedQueryPlannerSqlTest.java`

Existing test `testSqlIsNullFunction()` already verifies the plan has clean field names:
```
LogicalProject(is_null=[false])
```

Add a test that verifies the row type field names don't contain `\0`:
```java
@Test
public void testSqlAliasFieldNamesClean() {
    RelNode plan = givenQuery("SELECT ISNULL(department) AS is_null FROM catalog.employees").plan();
    for (RelDataTypeField field : plan.getRowType().getFieldList()) {
        assertFalse("Field name should not contain NUL: " + field.getName(),
            field.getName().contains("\0"));
    }
}
```
