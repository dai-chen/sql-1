# System Patterns

## System Architecture

### High-Level Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SQL/PPL       │    │   Query         │    │   OpenSearch    │
│   Parser        │───▶│   Planner       │───▶│   Executor      │
│                 │    │   (Calcite)     │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   AST Builder   │    │   Logical Plan  │    │   Physical Plan │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Module Dependencies
- **Core**: Foundation module with AST definitions and shared utilities
- **PPL**: Depends on Core, implements PPL-specific parsing and execution
- **SQL**: Depends on Core, implements SQL parsing and execution
- **OpenSearch**: Depends on Core, provides storage engine integration
- **Plugin**: Depends on all modules, packages for OpenSearch deployment

## Key Technical Decisions

### Parser Architecture
- **ANTLR4 Grammar**: Used for both SQL and PPL parsing
- **AST-First Approach**: Parse to AST, then convert to logical plans
- **Visitor Pattern**: Used extensively for AST traversal and transformation

### Query Planning
- **Apache Calcite Integration**: Leverages Calcite's query optimization capabilities
- **Logical Plan Abstraction**: Common logical plan representation for SQL and PPL
- **Physical Plan Specialization**: OpenSearch-specific physical execution plans

### Function System
- **Centralized Function Registry**: `BuiltinFunctionName` enum for all functions
- **Implementation Tables**: Separate tables for SQL and PPL function implementations
- **Type System**: Strong typing with OpenSearch data type mapping

## Design Patterns in Use

### Parser Patterns
- **Grammar Composition**: Separate lexer and parser grammars
- **AST Builder Pattern**: Converts parse trees to typed AST nodes
- **Expression Builder**: Specialized builders for different expression types

### Execution Patterns
- **Strategy Pattern**: Different execution strategies for different query types
- **Visitor Pattern**: AST traversal and transformation
- **Factory Pattern**: Creating appropriate executors and planners

### Integration Patterns
- **Adapter Pattern**: Adapting OpenSearch APIs to SQL/PPL semantics
- **Bridge Pattern**: Bridging Calcite plans to OpenSearch execution
- **Observer Pattern**: Query execution monitoring and metrics

## Component Relationships

### Core Components
```
QueryService ──┐
               ├──▶ PPLService ──▶ PPLSyntaxParser ──▶ AstBuilder
               └──▶ SQLService ──▶ SQLSyntaxParser ──▶ AstBuilder
                                                        │
                                                        ▼
                                              CalciteRelNodeVisitor
                                                        │
                                                        ▼
                                              OpenSearchExecutionEngine
```

### Function Implementation
```
BuiltinFunctionName ──┐
                      ├──▶ PPLFuncImpTable
                      └──▶ SQLFuncImpTable
                                │
                                ▼
                      FunctionImplementation
                                │
                                ▼
                      CalciteRexNodeVisitor
```

## Critical Implementation Paths

### Query Execution Flow
1. **Parse**: SQL/PPL → AST
2. **Plan**: AST → Logical Plan (via Calcite)
3. **Optimize**: Logical Plan → Optimized Logical Plan
4. **Execute**: Logical Plan → Physical Plan → Results

### Error Handling Flow
1. **Parse Errors**: ANTLR error listeners
2. **Semantic Errors**: AST validation
3. **Execution Errors**: Exception handling with context
4. **User Feedback**: Meaningful error messages

### Performance Optimization
- **Query Caching**: Parsed queries and execution plans
- **Pushdown Optimization**: Filter and aggregation pushdown to OpenSearch
- **Memory Management**: Streaming results for large datasets
- **Connection Pooling**: Efficient OpenSearch client management

## Extension Points
- **Custom Functions**: Add new functions via function tables
- **New Commands**: Extend PPL with additional commands
- **Storage Engines**: Abstract storage interface for different backends
- **Optimization Rules**: Custom Calcite optimization rules
