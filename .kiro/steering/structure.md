# Project Structure

## Module Organization

The project follows a multi-module Gradle structure with clear separation of concerns:

### Core Query Engine Modules

- **`core/`**: Core query engine implementation
  - Query execution framework
  - Expression evaluation
  - Type system and data models
  
- **`sql/`**: SQL language processor
  - SQL parser (ANTLR-based)
  - SQL-specific AST nodes
  - SQL to logical plan conversion
  
- **`ppl/`**: PPL (Piped Processing Language) processor
  - PPL parser (ANTLR-based)
  - PPL-specific AST nodes
  - PPL to logical plan conversion

- **`legacy/`**: Legacy SQL engine (V1)
  - Maintained for backward compatibility

### Storage & Data Source Modules

- **`opensearch/`**: OpenSearch storage engine
  - OpenSearch client integration
  - Index mapping and schema
  - Query pushdown to OpenSearch DSL
  
- **`prometheus/`**: Prometheus data source support
  - Prometheus query translation
  - Time-series data handling

- **`datasources/`**: Multi-datasource management
  - Data source registry
  - Connection management
  - Cross-datasource query support

### Query Execution Modules

- **`async-query-core/`**: Async query execution framework
  - Job management
  - Query scheduling
  - Result persistence
  
- **`async-query/`**: Async query plugin implementation
  - REST API for async queries
  - Query state management

- **`direct-query-core/`**: Direct query execution framework
  
- **`direct-query/`**: Direct query plugin implementation

### API & Protocol Modules

- **`api/`**: Public API interfaces
  - Plugin extension points
  - Service interfaces
  
- **`protocol/`**: Request/response protocol
  - Response formatters (JSON, JDBC, CSV)
  - Protocol serialization

- **`common/`**: Shared utilities
  - Common data structures
  - Utility classes
  - Pattern matching

### Plugin & Integration

- **`plugin/`**: OpenSearch plugin packaging
  - Plugin metadata
  - REST handlers
  - Plugin initialization
  - Security integration

- **`integ-test/`**: Integration tests
  - End-to-end query tests
  - Comparison tests with other databases
  - REST API tests

### Testing & Documentation

- **`doctest/`**: Documentation testing
  - Python-based doc tests
  - Test data and mappings
  - Bootstrap scripts

- **`benchmarks/`**: Performance benchmarks
  - JMH benchmarks
  - Query performance tests

- **`language-grammar/`**: ANTLR grammar definitions
  - Shared grammar files
  - Generated parser code

## Directory Layout

```
.
├── api/                    # Public API interfaces
├── async-query/            # Async query plugin
├── async-query-core/       # Async query framework
├── benchmarks/             # Performance benchmarks
├── common/                 # Shared utilities
├── core/                   # Core query engine
├── datasources/            # Multi-datasource support
├── direct-query/           # Direct query plugin
├── direct-query-core/      # Direct query framework
├── docs/                   # Documentation
│   ├── dev/               # Developer documentation
│   └── user/              # User reference manual
├── doctest/               # Documentation tests
├── integ-test/            # Integration tests
├── language-grammar/      # ANTLR grammars
├── legacy/                # Legacy SQL engine
├── opensearch/            # OpenSearch storage engine
├── plugin/                # Plugin packaging
├── ppl/                   # PPL language processor
├── prometheus/            # Prometheus data source
├── protocol/              # Protocol formatters
├── sql/                   # SQL language processor
├── build.gradle           # Root build configuration
└── settings.gradle        # Module definitions
```

## Source Code Layout (Standard Gradle)

Each module follows standard Gradle Java project structure:

```
<module>/
├── src/
│   ├── main/
│   │   ├── java/          # Java source code
│   │   ├── antlr/         # ANTLR grammar files (if applicable)
│   │   └── resources/     # Configuration files, mappings
│   ├── test/
│   │   ├── java/          # Unit tests
│   │   └── resources/     # Test resources
│   └── testFixtures/      # Shared test utilities (some modules)
├── build/                 # Generated build artifacts
└── build.gradle           # Module build configuration
```

## Package Organization

Java packages follow the pattern:
```
org.opensearch.sql.<module>.<component>
```

Example:
- `org.opensearch.sql.ppl.parser` - PPL parser
- `org.opensearch.sql.executor` - Query executor
- `org.opensearch.sql.expression` - Expression evaluation

## Published Artifacts

The following modules are published as Maven artifacts for external use:
- `api`, `sql`, `ppl`, `core`, `opensearch`, `common`, `protocol`, `datasources`, `legacy`

Published as: `org.opensearch.query:unified-query-<module>`

## Key Files

- **`build.gradle`**: Root build configuration with version management
- **`settings.gradle`**: Module inclusion and project structure
- **`gradle.properties`**: Gradle configuration and version
- **`DEVELOPER_GUIDE.rst`**: Comprehensive development guide
- **`CONTRIBUTING.md`**: Contribution guidelines
- **`lombok.config`**: Lombok configuration (root and per-module)
