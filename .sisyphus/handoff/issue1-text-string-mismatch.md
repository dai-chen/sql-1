# Issue 1: Text vs String Type Mismatch in Calcite Engine

## 1. Code Location of the Mismatch

**File**: `core/src/main/java/org/opensearch/sql/calcite/utils/OpenSearchTypeFactory.java`

### Forward conversion (OpenSearch → Calcite): Lines 147-210

At line 168 (`ExprCoreType.STRING` case) and line 202 (`text` legacyTypeName case), both `text` and `keyword` fields are mapped to `SqlTypeName.VARCHAR`:

```java
// Line 168: keyword fields (ExprCoreType.STRING) → VARCHAR
case STRING:
  return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);

// Line 202: text fields (OpenSearchTextType, legacyTypeName="text") → VARCHAR
} else if (fieldType.legacyTypeName().equalsIgnoreCase("text")) {
  return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);
```

### Reverse conversion (Calcite → ExprType): Lines 220-230

`convertSqlTypeNameToExprType` maps `VARCHAR` back to `ExprCoreType.STRING`:

```java
// Line 228
case CHAR, VARCHAR, MULTISET -> STRING; // call toString() for MULTISET
```

### Schema type name production: Line 258

`getLegacyTypeName` converts RelDataType → ExprType → type name string:

```java
public static String getLegacyTypeName(RelDataType relDataType, QueryType queryType) {
    ExprType type = convertRelDataTypeToExprType(relDataType);
    return (queryType == PPL ? PPL_SPEC.typeName(type) : type.legacyTypeName())
        .toUpperCase(Locale.ROOT);
}
```

For SQL: `ExprCoreType.STRING.legacyTypeName()` → "KEYWORD"
For PPL: `ExprCoreType.STRING.typeName()` → "STRING"

## 2. Legacy V2 Behavior

**File**: `opensearch/src/main/java/org/opensearch/sql/opensearch/data/type/OpenSearchDataType.java`

The V2 engine preserves the original OpenSearch field type through `OpenSearchDataType` and its subclasses:

### OpenSearchDataType.legacyTypeName() (lines ~240-250):
```java
public String legacyTypeName() {
    if (mappingType == null) {
      return exprCoreType.typeName();
    }
    if (mappingType.toString().equalsIgnoreCase("DATE")) {
      return exprCoreType.typeName();
    }
    return mappingType.toString().toUpperCase(); // "TEXT" for text, "KEYWORD" for keyword
}
```

### OpenSearchTextType (opensearch/.../type/OpenSearchTextType.java):
- Extends `OpenSearchDataType` with `MappingType.Text`
- `legacyTypeName()` → `mappingType.toString().toUpperCase()` → "TEXT"

### MappingType enum (OpenSearchDataType.java lines 24-50):
```java
Text("text", ExprCoreType.UNKNOWN),
Keyword("keyword", ExprCoreType.STRING),
```

In the V2 path, the `ProjectOperator.schema()` returns `OpenSearchTextType` for text fields, preserving the original mapping type. The `JdbcResponseFormatter` then calls `type.legacyTypeName().toLowerCase()` → "text".

## 3. Where Schema Type Names Come Out

### JdbcResponseFormatter (protocol/.../format/JdbcResponseFormatter.java, line 67):
```java
private String convertToLegacyType(ExprType type) {
    return type.legacyTypeName().toLowerCase();
}
```
Used by `RestSQLQueryAction` for `format=jdbc` requests.

### SimpleJsonResponseFormatter (protocol/.../format/SimpleJsonResponseFormatter.java, line 53):
```java
response.columnNameTypes().forEach((name, type) -> json.column(new Column(name, type)));
```
Used by `RestUnifiedQueryAction` (Calcite/analytics path).

### QueryResult.columnNameTypes() (protocol/.../response/QueryResult.java, line 78):
```java
langSpec.typeName(column.getExprType()).toLowerCase(Locale.ROOT)
```
For SQL: `LangSpec.SQL_SPEC.typeName()` → default → `exprType.typeName()` → "STRING" → "string"

### ExprCoreType type name methods (core/.../data/type/ExprCoreType.java):
- `typeName()` (line 97): returns `this.name()` → "STRING"
- `legacyTypeName()` (line 102): returns `LEGACY_TYPE_NAME_MAPPING.getOrDefault(this, this.name())` → "KEYWORD" for STRING

### MatcherUtils.verifySchema (integ-test/.../util/MatcherUtils.java, line 272):
Compares `jsonObject.query("/type")` against the expected type string. The "type" field in the JSON response comes from whichever formatter is used.

## 4. What the Failing Tests Expect

### MatchBoolPrefixIT (integ-test/.../sql/MatchBoolPrefixIT.java):

```java
// Line 28 (query_matches_test):
verifySchema(result, schema("phrase", "text"));

// Line 39 (additional_parameters_test):
verifySchema(result, schema("phrase", "text"));
```

### MatchIT (integ-test/.../sql/MatchIT.java):

```java
// Line 33 (match_in_where):
verifySchema(result, schema("firstname", "text"));

// Line 41 (match_in_having):
verifySchema(result, schema("lastname", "text"));
```

All failing tests expect `"text"` as the type for text-mapped fields. The Calcite engine produces `"string"` instead.

## 5. Root Cause

The root cause is **(a) the Calcite type factory loses the OpenSearch-specific field type info**. Specifically:

1. `OpenSearchTypeFactory.convertExprTypeToRelDataType` maps both `text` and `keyword` fields to the same Calcite type (`SqlTypeName.VARCHAR`) at lines 168 and 202. This is a lossy conversion — the original OpenSearch mapping type (`text` vs `keyword`) is discarded.

2. When building the response schema in `OpenSearchExecutionEngine.buildResultSet` (line 318 in `opensearch/.../executor/OpenSearchExecutionEngine.java`), `convertRelDataTypeToExprType(VARCHAR)` returns `ExprCoreType.STRING` — a generic type that has no knowledge of the original OpenSearch field type.

3. The response formatter then produces either "string" (via `typeName()` in `SimpleJsonResponseFormatter`) or "keyword" (via `legacyTypeName()` in `JdbcResponseFormatter`), but never "text" — because `ExprCoreType.STRING` doesn't know it originated from a `text` field.

The V2 engine avoids this by keeping `OpenSearchTextType` (which preserves `MappingType.Text`) all the way through to the response formatter, where `legacyTypeName()` returns "TEXT".

## 6. Suggested Fix Direction (Informational Only)

### Option A: Create a User-Defined Type (UDT) for text fields
Similar to how `EXPR_DATE`, `EXPR_TIME`, `EXPR_TIMESTAMP`, `EXPR_IP`, and `EXPR_BINARY` are handled as UDTs (via `ExprUDT`), create an `EXPR_TEXT` UDT that wraps `VARCHAR` but preserves the "text" origin. In `convertExprTypeToRelDataType`, map `OpenSearchTextType` → `EXPR_TEXT` UDT. In `convertRelDataTypeToExprType`, map `EXPR_TEXT` back to an ExprType whose `legacyTypeName()` returns "TEXT".

### Option B: Annotate the RelDataType with metadata
Use Calcite's type system to attach metadata (e.g., a custom `RelDataTypeField` property or a wrapper type) that records whether the original field was `text` or `keyword`. Then in `convertRelDataTypeToExprType`, check for this annotation and return the appropriate `OpenSearchDataType` instance instead of `ExprCoreType.STRING`.

### Option C: Preserve OpenSearchDataType in the schema column construction
In `OpenSearchExecutionEngine.buildResultSet` (line 318), instead of calling `convertRelDataTypeToExprType(fieldType)` which loses type info, look up the original `OpenSearchDataType` from the table schema/index mapping. The `CalcitePlanContext` or the `RelNode`'s row type metadata could carry the original field type mapping, allowing the schema builder to use `OpenSearchTextType` directly for text fields.
