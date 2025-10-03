# Technical Context

## Technologies Used

### Core Technologies
- **Java 11+**: Primary programming language
- **Gradle**: Build system and dependency management
- **ANTLR 4**: Parser generator for SQL and PPL grammars
- **Apache Calcite**: Query planning and optimization framework
- **OpenSearch**: Target search and analytics engine

### Key Dependencies
- **Apache Calcite Core**: Query planning and optimization
- **ANTLR Runtime**: Parser execution
- **Jackson**: JSON processing
- **JUnit 5**: Unit testing framework
- **Mockito**: Mocking framework for tests
- **Lombok**: Code generation for boilerplate reduction

### Development Tools
- **IntelliJ IDEA / VSCode**: Primary IDEs
- **Git**: Version control
- **GitHub Actions**: CI/CD pipeline
- **Spotless**: Code formatting
- **Checkstyle**: Code style enforcement

## Development Setup

### Build System
```bash
# Full build
./gradlew build

# Run tests
./gradlew test

# Module-specific build
./gradlew :core:build
./gradlew :ppl:build

# Integration tests
./gradlew :integ-test:integTest
```

### Module Structure
```
sql/
├── core/                 # Core functionality and AST
├── ppl/                  # PPL implementation
├── sql/                  # SQL implementation  
├── opensearch/           # OpenSearch integration
├── integ-test/           # Integration tests
├── plugin/               # Plugin packaging
├── common/               # Shared utilities
├── protocol/             # Communication protocols
├── legacy/               # Legacy compatibility
└── build.gradle          # Root build configuration
```

### ANTLR Grammar Generation
- Grammar files: `*.g4` in `src/main/antlr/`
- Generated classes: `build/generated-src/antlr/main/`
- Regeneration: Automatic on build, manual via Gradle tasks

## Technical Constraints

### Performance Requirements
- Query response time: < 1 second for 95% of queries
- Memory usage: Bounded for large result sets
- Concurrent queries: Support multiple simultaneous executions
- Resource isolation: Minimal impact on OpenSearch cluster

### Compatibility Requirements
- **OpenSearch Versions**: 1.0+ compatibility
- **Java Versions**: Java 11+ support
- **API Stability**: Backward compatibility for public APIs
- **Plugin Architecture**: Follow OpenSearch plugin standards

### Security Constraints
- **Input Validation**: All user inputs must be validated
- **SQL Injection Prevention**: Parameterized queries only
- **Access Control**: Respect OpenSearch security model
- **Audit Logging**: Security-relevant operations logged

## Dependencies Management

### Core Dependencies
```gradle
// Apache Calcite for query planning
implementation "org.apache.calcite:calcite-core:1.38.0"

// ANTLR for parsing
implementation "org.antlr:antlr4-runtime:4.13.1"

// OpenSearch client
implementation "org.opensearch.client:opensearch-rest-high-level-client:${opensearch_version}"

// Jackson for JSON
implementation "com.fasterxml.jackson.core:jackson-core:${jackson_version}"
```

### Test Dependencies
```gradle
// JUnit 5
testImplementation "org.junit.jupiter:junit-jupiter:${junit_version}"

// Mockito
testImplementation "org.mockito:mockito-core:${mockito_version}"

// Integration test framework
testImplementation "org.opensearch.test:framework:${opensearch_version}"
```

## Tool Usage Patterns

### Code Generation
- **ANTLR**: Grammar → Parser classes
- **Lombok**: Annotations → Boilerplate code
- **Calcite**: SQL → Relational algebra

### Testing Strategy
- **Unit Tests**: Individual class/method testing
- **Integration Tests**: End-to-end query execution
- **Performance Tests**: Query execution benchmarks
- **Compatibility Tests**: Cross-version validation

### Build Pipeline
1. **Compile**: Java source compilation
2. **Generate**: ANTLR parser generation
3. **Test**: Unit and integration tests
4. **Package**: JAR and plugin packaging
5. **Verify**: Code quality checks

## Development Environment Configuration

### IDE Setup
- **Java SDK**: OpenJDK 11 or higher
- **Gradle**: Use wrapper (`./gradlew`)
- **ANTLR Plugin**: For grammar syntax highlighting
- **Lombok Plugin**: For annotation processing

### Local Testing
- **OpenSearch Instance**: Local cluster for integration tests
- **Test Data**: Sample indices and documents
- **Debug Configuration**: Remote debugging support

### Code Quality Tools
- **Spotless**: Automatic code formatting
- **Checkstyle**: Style guide enforcement  
- **SpotBugs**: Static analysis for bug detection
- **JaCoCo**: Code coverage reporting

## Deployment Considerations
- **Plugin Installation**: Standard OpenSearch plugin mechanism
- **Configuration**: Plugin settings and cluster configuration
- **Monitoring**: Query performance and error metrics
- **Upgrades**: Rolling upgrade compatibility
