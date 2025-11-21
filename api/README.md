# Unified Query API

This module provides a high-level integration layer for the Calcite-based query engine, enabling external systems such as Apache Spark or command-line tools to parse, analyze, and transpile queries without exposing low-level internals.

## Overview

The API provides two main entry points:

1. **`UnifiedQuery`** - A fluent facade API for query planning and transpilation (recommended for most use cases)
2. **`UnifiedQueryPlanner`** - Direct access to the query planner for advanced scenarios

## Quick Start

### Transpile PPL to Spark SQL

The simplest way to convert PPL queries to SQL is using the `UnifiedQuery` fluent API:

```java
String sparkSql = UnifiedQuery
    .lang(QueryType.PPL)
    .catalog("spark_catalog", mySchema)
    .defaultNamespace("spark_catalog.default")
    .transpile("source = employees | where age > 30", SqlDialect.DatabaseProduct.SPARK);
```

### Get Logical Plan

To obtain a Calcite logical plan (RelNode) for further processing:

```java
RelNode plan = UnifiedQuery
    .lang(QueryType.PPL)
    .catalog("opensearch", mySchema)
    .defaultNamespace("opensearch")
    .plan("source = employees | stats avg(age) by department");
```

## API Components

### UnifiedQuery (Recommended)

The `UnifiedQuery` class provides a fluent, chainable API for query processing with three main operations:

- **`plan(query)`** - Parse and analyze a query into a Calcite logical plan
- **`transpile(query, dialect)`** - Convert a query to target SQL dialect
- **`transpile(query, options)`** - Convert with custom transpile options

#### Basic Transpilation

```java
// Transpile to Spark SQL
String sql = UnifiedQuery
    .lang(QueryType.PPL)
    .catalog("catalog", schema)
    .transpile("source = employees | fields name, age", SqlDialect.DatabaseProduct.SPARK);
```

#### Custom Transpile Options

For advanced scenarios, use `TranspileOptions` to configure SQL generation:

```java
TranspileOptions options = TranspileOptions.builder()
    .databaseProduct(SqlDialect.DatabaseProduct.PRESTO)
    .prettyPrint(true)
    .build();

String sql = UnifiedQuery
    .lang(QueryType.PPL)
    .catalog("catalog", schema)
    .transpile("source = employees", options);
```

### UnifiedQueryPlanner (Advanced)

For direct access to the planner without transpilation:

```java
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("opensearch", schema)
    .defaultNamespace("opensearch")
    .cacheMetadata(true)
    .build();

RelNode plan = planner.plan("source = opensearch.test");
```

### Supported SQL Dialects

The API supports transpilation to all major SQL dialects through Calcite's `SqlDialect.DatabaseProduct` enum:

- **`SPARK`** - Apache Spark SQL (recommended for Spark integration)
- **`HIVE`** - Apache Hive SQL
- **`PRESTO`** - Presto SQL
- **`MYSQL`** - MySQL
- **`POSTGRESQL`** - PostgreSQL
- **`ORACLE`** - Oracle Database
- **`MSSQL`** - Microsoft SQL Server
- **`REDSHIFT`** - Amazon Redshift
- **`SNOWFLAKE`** - Snowflake
- **`BIG_QUERY`** - Google BigQuery
- And many more...

## Spark SQL Dialect Specifics

When transpiling to Spark SQL (`SqlDialect.DatabaseProduct.SPARK`), the generated SQL follows these conventions:

### Identifier Quoting
Identifiers are quoted with backticks (`) to handle reserved keywords and special characters:
```sql
SELECT `name`, `age` FROM `catalog`.`employees`
```

### Catalog References
Fully qualified table names include the catalog:
```sql
FROM `spark_catalog`.`default`.`employees`
```

### Function Names
Aggregate and scalar functions use standard Spark SQL syntax:
```sql
SELECT AVG(`age`) `avg(age)`, COUNT(*) `count()`
```

### PPL to Spark SQL Examples

| PPL Query | Generated Spark SQL |
|-----------|---------------------|
| `source = employees` | `SELECT * FROM catalog.employees` |
| `source = employees \| fields name, age` | `SELECT name, age FROM catalog.employees` |
| `source = employees \| where age > 30` | `SELECT * FROM catalog.employees WHERE age > 30` |
| `source = employees \| stats avg(age) by dept` | `SELECT AVG(age) avg(age), dept FROM catalog.employees GROUP BY dept` |

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

## Future Work

- Expand support to SQL language.
- Extend planner to generate optimized physical plans using Calcite's optimization frameworks.
