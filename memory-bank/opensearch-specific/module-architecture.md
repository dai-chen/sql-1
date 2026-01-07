# Module Architecture

## Module Overview

### Core Module (`core/`)
**Purpose**: Foundation module containing shared functionality and AST definitions

**Key Components**:
- **AST Definitions**: `org.opensearch.sql.ast.tree.*` - All AST node classes
- **Expression System**: `org.opensearch.sql.expression.*` - Expression evaluation framework
- **Function Registry**: `BuiltinFunctionName` enum and function tables
- **Calcite Integration**: Core Calcite visitors and plan context
- **Common Utilities**: Shared utilities and helper classes

**Dependencies**: Minimal external dependencies (Calcite, ANTLR runtime)

### PPL Module (`ppl/`)
**Purpose**: PPL (Piped Processing Language) specific implementation

**Key Components**:
- **ANTLR Grammar**: `OpenSearchPPLLexer.g4`, `OpenSearchPPLParser.g4`
- **Parser Implementation**: `PPLSyntaxParser` and generated ANTLR classes
- **AST Builders**: `AstBuilder`, `AstExpressionBuilder`
- **PPL Service**: `PPLService` - Main entry point for PPL query processing
- **Function Tables**: `PPLFuncImpTable` - PPL-specific function implementations

**Dependencies**: Core module, ANTLR runtime

### SQL Module (`sql/`)
**Purpose**: Standard SQL parsing and execution

**Key Components**:
- **SQL Parser**: Standard SQL grammar and parser implementation
- **AST Builders**: SQL-specific AST construction logic
- **SQL Service**: Main entry point for SQL query processing
- **Aggregation Builders**: `AstAggregationBuilder` for SQL aggregations
- **Function Tables**: SQL-specific function implementations

**Dependencies**: Core module, SQL parsing libraries

### OpenSearch Module (`opensearch/`)
**Purpose**: OpenSearch storage engine integration and physical execution

**Key Components**:
- **Execution Engine**: `OpenSearchExecutionEngine` - Main execution coordinator
- **Query Manager**: `OpenSearchQueryManager` - Query lifecycle management
- **Index Scanning**: `CalciteEnumerableIndexScan`, `CalciteLogicalIndexScan`
- **Physical Rules**: `EnumerableIndexScanRule` - Calcite physical planning rules
- **Storage Integration**: Direct OpenSearch client integration

**Dependencies**: Core module, OpenSearch client libraries

### Integration Test Module (`integ-test/`)
**Purpose**: End-to-end integration testing

**Key Components**:
- **Test Suites**: Comprehensive test coverage for SQL and PPL
- **Test Infrastructure**: OpenSearch cluster setup and management
- **Performance Tests**: Query performance and scalability testing
- **Compatibility Tests**: Cross-version compatibility validation

**Dependencies**: All other modules, OpenSearch test framework

## Module Interaction Patterns

### Query Processing Flow
```
User Query → SQL/PPL Module → Core AST → Calcite Planning → OpenSearch Execution
```

### Detailed Flow
1. **Input Processing**: SQL/PPL modules parse user queries
2. **AST Generation**: Parsers create AST using Core module classes
3. **Logical Planning**: Core Calcite visitors convert AST to logical plans
4. **Optimization**: Calcite applies optimization rules
5. **Physical Planning**: OpenSearch module generates physical execution plans
6. **Execution**: OpenSearch module executes against OpenSearch cluster

### Cross-Module Dependencies
```
┌─────────────┐    ┌─────────────┐
│     PPL     │    │     SQL     │
│   Module    │    │   Module    │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
        ┌───────▼───────┐
        │     Core      │
        │    Module     │
        └───────┬───────┘
                │
        ┌───────▼───────┐
        │  OpenSearch   │
        │    Module     │
        └───────────────┘
```

## Package Organization

### Core Module Packages
```
org.opensearch.sql.
├── ast.tree.*              # AST node definitions
├── expression.*            # Expression evaluation
├── calcite.*              # Calcite integration
├── executor.*             # Query execution framework
└── common.*               # Shared utilities
```

### PPL Module Packages
```
org.opensearch.sql.ppl.
├── antlr.*                # Generated ANTLR classes
├── parser.*               # AST builders and parsers
├── utils.*                # PPL-specific utilities
└── calcite.*              # PPL-Calcite integration tests
```

### OpenSearch Module Packages
```
org.opensearch.sql.opensearch.
├── executor.*             # Execution engine
├── storage.*              # Storage layer integration
├── planner.*              # Physical planning
└── client.*               # OpenSearch client management
```

## Build Dependencies

### Gradle Module Configuration
```gradle
// Core module
dependencies {
    implementation "org.apache.calcite:calcite-core:${calcite_version}"
    implementation "org.antlr:antlr4-runtime:${antlr_version}"
}

// PPL module
dependencies {
    implementation project(':core')
    antlr "org.antlr:antlr4:${antlr_version}"
}

// OpenSearch module
dependencies {
    implementation project(':core')
    implementation "org.opensearch.client:opensearch-rest-high-level-client:${opensearch_version}"
}
```

### Build Order
1. **Core**: Built first as foundation
2. **PPL/SQL**: Built in parallel, depend on Core
3. **OpenSearch**: Built after Core, integrates execution
4. **Integration Tests**: Built last, depends on all modules

## Extension Patterns

### Adding New Language Support
1. **Create New Module**: Follow PPL/SQL module pattern
2. **Grammar Definition**: Define ANTLR grammar for new language
3. **AST Integration**: Use Core AST classes or extend as needed
4. **Parser Implementation**: Implement AST builders
5. **Service Layer**: Create service class for query processing

### Adding New Storage Engine
1. **Extend Core**: Add abstract storage interfaces to Core
2. **Implementation Module**: Create new module for storage engine
3. **Physical Planning**: Implement storage-specific physical plans
4. **Execution Engine**: Implement query execution logic
5. **Integration**: Add to main query processing pipeline

## Testing Strategy

### Unit Testing per Module
- **Core**: AST construction, expression evaluation, Calcite integration
- **PPL/SQL**: Parser functionality, AST building, grammar validation
- **OpenSearch**: Execution engine, query translation, client integration

### Integration Testing
- **Cross-Module**: End-to-end query processing
- **Performance**: Query execution benchmarks
- **Compatibility**: OpenSearch version compatibility

### Test Organization
```
src/test/java/
├── unit/                  # Unit tests per module
├── integration/           # Cross-module integration tests
└── performance/           # Performance and load tests
```

## Configuration Management

### Module-Specific Configuration
- **Core**: Calcite planner configuration, type system settings
- **PPL/SQL**: Parser configuration, grammar options
- **OpenSearch**: Client configuration, cluster settings

### Shared Configuration
- **Logging**: Centralized logging configuration
- **Metrics**: Performance monitoring and metrics collection
- **Security**: Authentication and authorization settings

## Deployment Considerations

### Plugin Packaging
- **Single JAR**: All modules packaged into single OpenSearch plugin
- **Dependency Management**: Shade conflicting dependencies
- **Class Loading**: Proper class loader isolation

### Version Compatibility
- **OpenSearch Versions**: Maintain compatibility matrix
- **Module Versioning**: Semantic versioning for each module
- **API Stability**: Maintain backward compatibility for public APIs
