# Unified Query API

This module provides a high-level integration layer for the Calcite-based query engine, enabling external systems such as Apache Spark or command-line tools to parse and analyze queries without exposing low-level internals.

## Overview

This module provides components organized into two main areas aligned with the [Unified Query API architecture](https://github.com/opensearch-project/sql/issues/4782):

### Unified Language Specification

- **`UnifiedQueryPlanner`** (`org.opensearch.sql.api`): Accepts PPL (Piped Processing Language) queries and returns Calcite `RelNode` logical plans as intermediate representation.
- **`UnifiedQueryTranspiler`** (`org.opensearch.sql.api.transpiler`): Converts Calcite logical plans (`RelNode`) into SQL strings for various target databases using different SQL dialects.

### Unified Execution Runtime

- **`UnifiedQueryCompiler`** (`org.opensearch.sql.api.runtime`): Compiles Calcite logical plans (`RelNode`) into executable JDBC `PreparedStatement` objects, following the PartiQL compiler pattern for separation of compilation and execution.

Together, these components enable complete workflows: parse PPL queries into logical plans, transpile those plans into target database SQL, or compile and execute queries directly using standard JDBC for testing and conformance validation.

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

### UnifiedQueryCompiler

Use `UnifiedQueryCompiler` to compile Calcite logical plans into executable JDBC statements. This follows the PartiQL compiler pattern, separating compilation from execution and returning standard JDBC types.

```java
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("catalog", schema)
    .defaultNamespace("catalog")
    .build();

RelNode plan = planner.plan("source = employees | fields name, age");

UnifiedQueryCompiler compiler = UnifiedQueryCompiler.builder()
    .context(planner.getContext())
    .build();

// Compile once, execute multiple times with standard JDBC
try (PreparedStatement statement = compiler.compile(plan)) {
    ResultSet resultSet = statement.executeQuery();
    
    // Access schema
    ResultSetMetaData metaData = resultSet.getMetaData();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
        System.out.println(metaData.getColumnName(i) + ": " + metaData.getColumnTypeName(i));
    }
    
    // Access results
    while (resultSet.next()) {
        String name = resultSet.getString("name");
        int age = resultSet.getInt("age");
        // Process row data
    }
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

#### Compiling and Executing PPL Queries

Using the compiler for CLI tools or embedded engines with standard JDBC:

```java
// Step 1: Initialize planner with catalog
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("catalog", schema)
    .defaultNamespace("catalog")
    .build();

// Step 2: Parse query into logical plan
RelNode plan = planner.plan("source = employees | where age > 30 | fields name, age");

// Step 3: Initialize compiler
UnifiedQueryCompiler compiler = UnifiedQueryCompiler.builder()
    .context(planner.getContext())
    .build();

// Step 4: Compile and execute with standard JDBC
try (PreparedStatement statement = compiler.compile(plan)) {
    ResultSet resultSet = statement.executeQuery();
    
    // Process schema
    ResultSetMetaData metaData = resultSet.getMetaData();
    System.out.println("Schema:");
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
        System.out.println("  " + metaData.getColumnName(i) + ": " + 
                         metaData.getColumnTypeName(i));
    }
    
    // Process results
    System.out.println("\nResults:");
    while (resultSet.next()) {
        String name = resultSet.getString("name");
        Integer age = resultSet.getInt("age");
        System.out.println("  " + name + ", " + age);
    }
}
```

#### Conformance Testing

Using the compiler as a reference implementation for conformance tests:

```java
// Create test schema with known data
Schema testSchema = createTestSchema();

UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("test", testSchema)
    .defaultNamespace("test")
    .build();

RelNode plan = planner.plan("source = test_table | stats count() by category");

UnifiedQueryCompiler compiler = UnifiedQueryCompiler.builder()
    .context(planner.getContext())
    .build();

// Compile and execute test query
try (PreparedStatement statement = compiler.compile(plan)) {
    ResultSet resultSet = statement.executeQuery();
    
    // Verify results match expected behavior
    int rowCount = 0;
    while (resultSet.next()) {
        rowCount++;
        // Additional assertions on row data...
    }
    assertEquals(expectedRowCount, rowCount);
}
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

The compiler is ideal for command-line tools that need to execute PPL queries interactively:

```java
Scanner scanner = new Scanner(System.in);
while (true) {
    System.out.print("ppl> ");
    String query = scanner.nextLine();
    
    try {
        RelNode plan = planner.plan(query);
        try (PreparedStatement stmt = compiler.compile(plan)) {
            ResultSet rs = stmt.executeQuery();
            printResults(rs);
        }
    } catch (SyntaxCheckException e) {
        System.err.println("Syntax error: " + e.getMessage());
    } catch (IllegalStateException | SQLException e) {
        System.err.println("Error: " + e.getMessage());
    }
}
```

### Embedded Query Engines

Applications can embed the compiler to provide PPL query capabilities with standard JDBC:

```java
public class EmbeddedQueryEngine {
    private final UnifiedQueryPlanner planner;
    private final UnifiedQueryCompiler compiler;
    
    public EmbeddedQueryEngine(Schema dataSchema) {
        this.planner = UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("data", dataSchema)
            .defaultNamespace("data")
            .build();
            
        this.compiler = UnifiedQueryCompiler.builder()
            .context(planner.getContext())
            .build();
    }
    
    public List<Map<String, Object>> query(String pplQuery) throws SQLException {
        RelNode plan = planner.plan(pplQuery);
        try (PreparedStatement stmt = compiler.compile(plan)) {
            ResultSet rs = stmt.executeQuery();
            return convertToMaps(rs);
        }
    }
}
```

### Conformance Test Suites

The compiler serves as a reference implementation for validating PPL semantics:

```java
@Test
public void testPPLConformance() throws SQLException {
    // Test that PPL commands produce correct results
    RelNode plan = planner.plan("source = employees | stats avg(age) by department");
    
    try (PreparedStatement stmt = compiler.compile(plan)) {
        ResultSet rs = stmt.executeQuery();
        
        // Verify results match PPL specification
        assertNotNull(rs.getMetaData());
        assertEquals(expectedAggregations, countRows(rs));
    }
}
```

## Future Work

- Expand support to SQL language.
- Extend planner to generate optimized physical plans using Calcite's optimization frameworks.
- Add streaming query support to the evaluator.
- Implement result pagination for large datasets.
