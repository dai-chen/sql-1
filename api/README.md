# Unified Query API

This module provides a high-level integration layer for the Calcite-based query engine, enabling external systems such as Apache Spark or command-line tools to parse and analyze queries without exposing low-level internals.

## Overview

This module provides three primary components:

- **`UnifiedQueryPlanner`**: Accepts PPL (Piped Processing Language) queries and returns Calcite `RelNode` logical plans as intermediate representation.
- **`UnifiedQueryTranspiler`**: Converts Calcite logical plans (`RelNode`) into SQL strings for various target databases using different SQL dialects.
- **`UnifiedQueryEvaluator`**: Executes PPL queries against any catalog and returns results in an engine-agnostic format, serving as a reference runtime implementation.

Together, these components enable complete workflows: parse PPL queries into logical plans, transpile those plans into target database SQL, or execute queries directly for testing and conformance validation.

### Experimental API Design

**This API is currently experimental.** The design intentionally exposes Calcite abstractions (`Schema` for catalogs, `RelNode` as IR, `SqlDialect` for dialects) rather than creating custom wrapper interfaces. This is to avoid overdesign by leveraging the flexible Calcite interface in the short term. If a more abstracted API becomes necessary in the future, breaking changes may be introduced with the new abstraction layer.

## Usage

### UnifiedQueryPlanner

Use the declarative, fluent builder API to initialize the `UnifiedQueryPlanner`.

```java
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("opensearch", schema)
    .defaultNamespace("opensearch")
    .cacheMetadata(true)
    .build();

RelNode plan = planner.plan("source = opensearch.test");
```

### UnifiedQueryTranspiler

Use `UnifiedQueryTranspiler` to convert Calcite logical plans into SQL strings for target databases. The transpiler supports various SQL dialects through Calcite's `SqlDialect` interface.

```java
UnifiedQueryTranspiler transpiler = UnifiedQueryTranspiler.builder()
    .dialect(SparkSqlDialect.DEFAULT)
    .build();

String sql = transpiler.toSql(plan);
```

### UnifiedQueryEvaluator

Use `UnifiedQueryEvaluator` to execute PPL queries and get results in an engine-agnostic format. This is useful for CLI tools, embedded engines, and conformance test suites.

```java
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("catalog", schema)
    .defaultNamespace("catalog")
    .build();

UnifiedQueryEvaluator evaluator = UnifiedQueryEvaluator.builder()
    .planner(planner)
    .build();

ExecutionEngine.QueryResponse response = evaluator.evaluate("source = employees | fields name, age");

// Access schema
ExecutionEngine.Schema schema = response.getSchema();
for (ExecutionEngine.Schema.Column column : schema.getColumns()) {
    System.out.println(column.getName() + ": " + column.getExprType());
}

// Access results
for (ExprValue row : response.getResults()) {
    Map<String, ExprValue> tupleValue = row.tupleValue();
    // Process row data
}
```

### Complete Workflow Examples

#### Transpiling PPL to SQL

Combining planner and transpiler to convert PPL queries into target database SQL:

```java
// Step 1: Initialize planner
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("catalog", schema)
    .defaultNamespace("catalog")
    .build();

// Step 2: Parse PPL query into logical plan
RelNode plan = planner.plan("source = employees | where age > 30");

// Step 3: Initialize transpiler with target dialect
UnifiedQueryTranspiler transpiler = UnifiedQueryTranspiler.builder()
    .dialect(SparkSqlDialect.DEFAULT)
    .build();

// Step 4: Transpile to target SQL
String sparkSql = transpiler.toSql(plan);
// Result: SELECT * FROM `catalog`.`employees` WHERE `age` > 30
```

Supported SQL dialects include:
- `SparkSqlDialect.DEFAULT` - Apache Spark SQL
- `PostgresqlSqlDialect.DEFAULT` - PostgreSQL
- `MysqlSqlDialect.DEFAULT` - MySQL
- And other Calcite-supported dialects

#### Executing PPL Queries Directly

Using the evaluator for CLI tools or embedded engines:

```java
// Step 1: Initialize planner with catalog
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("catalog", schema)
    .defaultNamespace("catalog")
    .build();

// Step 2: Initialize evaluator
UnifiedQueryEvaluator evaluator = UnifiedQueryEvaluator.builder()
    .planner(planner)
    .build();

// Step 3: Execute query and get results
ExecutionEngine.QueryResponse response = 
    evaluator.evaluate("source = employees | where age > 30 | fields name, age");

// Step 4: Process results
System.out.println("Schema:");
for (ExecutionEngine.Schema.Column column : response.getSchema().getColumns()) {
    System.out.println("  " + column.getName() + ": " + column.getExprType());
}

System.out.println("\nResults:");
for (ExprValue row : response.getResults()) {
    String name = row.tupleValue().get("name").stringValue();
    Integer age = row.tupleValue().get("age").integerValue();
    System.out.println("  " + name + ", " + age);
}
```

#### Conformance Testing

Using the evaluator as a reference implementation for conformance tests:

```java
// Create test schema with known data
Schema testSchema = createTestSchema();

UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("test", testSchema)
    .defaultNamespace("test")
    .build();

UnifiedQueryEvaluator evaluator = UnifiedQueryEvaluator.builder()
    .planner(planner)
    .build();

// Execute test query
ExecutionEngine.QueryResponse response = 
    evaluator.evaluate("source = test_table | stats count() by category");

// Verify results match expected behavior
assertEquals(expectedRowCount, response.getResults().size());
// Additional assertions...
```

## Development & Testing

A set of unit tests is provided to validate planner behavior.

To run tests:

```
./gradlew :api:test
```

## Integration Guide

This guide walks through how to integrate unified query planner into your application.

### Step 1: Add Dependency

The module is currently published as a snapshot to the AWS Sonatype Snapshots repository. To include it as a dependency in your project, add the following to your `pom.xml` or `build.gradle`:

```xml
<dependency>
  <groupId>org.opensearch.query</groupId>
  <artifactId>unified-query-api</artifactId>
  <version>YOUR_VERSION_HERE</version>
</dependency>
```

### Step 2: Implement a Calcite Schema

You must implement the Calcite `Schema` interface and register them using the fluent `catalog()` method on the builder.

```java
public class MySchema extends AbstractSchema {
  @Override
  protected Map<String, Table> getTableMap() {
    return Map.of(
      "test_table",
      new AbstractTable() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.createStructType(
            List.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
            List.of("id"));
        }
      });
  }
}
```

## Error Handling

The Unified Query API provides structured error handling:

### Syntax Errors

When a query contains syntax errors, `SyntaxCheckException` is thrown with details about the error location:

```java
try {
    RelNode plan = planner.plan("source = employees | invalid_command");
} catch (SyntaxCheckException e) {
    System.err.println("Syntax error: " + e.getMessage());
    // Error includes line, column, and offending token
}
```

### Semantic Errors

When a query references non-existent tables or has type mismatches, `IllegalStateException` is thrown:

```java
try {
    ExecutionEngine.QueryResponse response = 
        evaluator.evaluate("source = nonexistent_table");
} catch (IllegalStateException e) {
    System.err.println("Semantic error: " + e.getMessage());
}
```

## Use Cases

### CLI Tools

The evaluator is ideal for command-line tools that need to execute PPL queries interactively:

```java
Scanner scanner = new Scanner(System.in);
while (true) {
    System.out.print("ppl> ");
    String query = scanner.nextLine();
    
    try {
        ExecutionEngine.QueryResponse response = evaluator.evaluate(query);
        printResults(response);
    } catch (SyntaxCheckException e) {
        System.err.println("Syntax error: " + e.getMessage());
    } catch (IllegalStateException e) {
        System.err.println("Error: " + e.getMessage());
    }
}
```

### Embedded Query Engines

Applications can embed the evaluator to provide PPL query capabilities:

```java
public class EmbeddedQueryEngine {
    private final UnifiedQueryEvaluator evaluator;
    
    public EmbeddedQueryEngine(Schema dataSchema) {
        UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("data", dataSchema)
            .defaultNamespace("data")
            .build();
            
        this.evaluator = UnifiedQueryEvaluator.builder()
            .planner(planner)
            .build();
    }
    
    public List<Map<String, Object>> query(String pplQuery) {
        ExecutionEngine.QueryResponse response = evaluator.evaluate(pplQuery);
        return convertToMaps(response.getResults());
    }
}
```

### Conformance Test Suites

The evaluator serves as a reference implementation for validating PPL semantics:

```java
@Test
public void testPPLConformance() {
    // Test that PPL commands produce correct results
    ExecutionEngine.QueryResponse response = 
        evaluator.evaluate("source = employees | stats avg(age) by department");
    
    // Verify results match PPL specification
    assertNotNull(response.getSchema());
    assertEquals(expectedAggregations, response.getResults());
}
```

## Future Work

- Expand support to SQL language.
- Extend planner to generate optimized physical plans using Calcite's optimization frameworks.
- Add streaming query support to the evaluator.
- Implement result pagination for large datasets.
