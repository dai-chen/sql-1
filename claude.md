# OpenSearch SQL Development Guide - Claude Code Memory Bank

This file serves as a comprehensive index to the OpenSearch SQL development knowledge base, converted from the Cline memory bank system. All referenced files are available at: https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules

## 🎯 Core Development Rules & Standards

### [Memory Bank System](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/01-memory-bank.md)
- Persistent context management across development sessions
- Structured documentation and knowledge preservation
- Feature investigation methodology

### [Java Development Standards](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/02-java-standards.md)
- Comprehensive Java coding guidelines for OpenSearch SQL
- Code quality requirements and best practices
- Style conventions and naming patterns

### [OpenSearch Project Guidelines](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/03-opensearch-project.md)
- OpenSearch-specific development patterns
- Plugin architecture requirements
- Integration guidelines with OpenSearch core

### [Testing Requirements](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/04-testing.md)
- Testing standards and coverage requirements
- Unit, integration, and system testing patterns
- Test automation and quality gates

### [Development Workflow](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/05-workflow.md)
- Development process patterns
- Code review and collaboration guidelines
- Release and deployment procedures

## 🧠 Memory Bank Core Documentation

### Project Foundation
- **[Project Brief](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/projectbrief.md)**: Core project scope, goals, and requirements
- **[Product Context](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/productContext.md)**: Product vision, user experience goals, and business context

### Technical Context
- **[System Patterns](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/systemPatterns.md)**: Architecture decisions, design patterns, and system organization
- **[Technical Context](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/techContext.md)**: Technology stack, dependencies, and development environment setup

### Project Status
- **[Progress Tracking](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/progress.md)**: Current project status, completed features, and roadmap

## 🔧 OpenSearch-Specific Technical Documentation

### Architecture & Integration
- **[Module Architecture](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/opensearch-specific/module-architecture.md)**: Multi-module project organization and component relationships
- **[Calcite Integration](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/opensearch-specific/calcite-integration.md)**: Apache Calcite query optimization integration patterns
- **[PPL Parser Patterns](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/opensearch-specific/ppl-parser-patterns.md)**: PPL (Piped Processing Language) parser implementation guidelines

## 📋 Feature Development Framework

### Feature Documentation System
- **[Feature Documentation Guide](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/features/README.md)**: Systematic approach to feature investigation and documentation
- **[Feature Template](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules/blob/main/memory-bank/feature-template.md)**: Standardized template for new feature analysis and design

## 🚀 Quick Start Commands

### Build & Test
```bash
# Build the entire project
./gradlew build

# Run specific tests
./gradlew :opensearch:test --tests <TestClassName>

# Run integration tests
./gradlew integTest
```

### Development Environment
```bash
# Set Java version for OpenSearch compatibility
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home

# Build with specific Java version
JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home ./gradlew build
```

## 📚 Key Technologies & Frameworks

### Core Technologies
- **Java**: OpenJDK/Amazon Corretto 21+
- **OpenSearch**: Distributed search and analytics engine
- **Apache Calcite**: SQL parser and query optimization framework
- **ANTLR**: Parser generator for SQL/PPL grammar

### Development Tools
- **Gradle**: Build system and dependency management
- **JUnit**: Unit testing framework
- **Mockito**: Mocking framework for tests
- **Spotless**: Code formatting and style enforcement

## 🎯 Development Principles

### Code Quality Standards
1. **Maintainability**: Write self-documenting, readable code
2. **Testability**: Ensure comprehensive test coverage
3. **Performance**: Consider query performance implications
4. **Security**: Follow OpenSearch security best practices
5. **Compatibility**: Maintain backward compatibility where possible

### Architecture Patterns
- **Layered Architecture**: Clear separation between parsing, planning, and execution
- **Plugin Pattern**: Extensible architecture for new SQL/PPL features
- **Factory Pattern**: Consistent object creation and lifecycle management
- **Strategy Pattern**: Pluggable algorithms for query optimization

## 📖 Additional Resources

- **[Complete Repository](https://github.com/ykmr1224/opensearch-sql-cline-memory-bank-rules)**: Full Cline memory bank with all documentation
- **OpenSearch SQL Plugin**: Main project repository
- **Apache Calcite Documentation**: Query optimization framework docs
- **ANTLR Documentation**: Parser generation tools and patterns

---

**Note**: This claude.md file serves as a centralized index to all development knowledge for the OpenSearch SQL project. Each linked document contains detailed information specific to its domain, providing comprehensive guidance for development tasks.