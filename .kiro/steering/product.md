# Product Overview

OpenSearch SQL is a plugin that enables querying OpenSearch data using familiar SQL and Piped Processing Language (PPL) syntax. It provides a bridge between traditional relational query languages and OpenSearch's document-based data model.

## Core Capabilities

- **SQL Support**: Full ANSI SQL query support for OpenSearch indices
- **PPL (Piped Processing Language)**: Native query language designed for observability and security analytics, similar to Splunk SPL
- **Multiple Query Engines**: 
  - V2 engine (current): Custom-built parser, analyzer, and optimizer
  - V3 engine (in development): Apache Calcite-based for enhanced optimization and SQL compatibility
- **Data Source Support**: OpenSearch, Prometheus, and other external data sources
- **Async Query Execution**: Support for long-running queries with job management
- **Multiple Response Formats**: JSON, JDBC, CSV, and raw formats

## Related Projects

The SQL plugin is part of a larger ecosystem:
- SQL CLI: Command-line interface
- SQL JDBC: JDBC driver for database tools
- SQL ODBC: ODBC driver
- Query Workbench: UI for OpenSearch Dashboards

## Key Design Goals

- Simplify data analysis for observability and security use cases
- Enable migration from Splunk and traditional SQL databases
- Support complex queries including JOINs, subqueries, and aggregations
- Handle structured, semi-structured, and nested data
- Provide high-performance query execution with optimization
