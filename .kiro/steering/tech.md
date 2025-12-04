# Technology Stack

## Build System

- **Gradle**: Primary build tool (Gradle 8.x)
- **Java Version**: Java 21 (required for both development and runtime)
- **Build Tool**: OpenSearch Gradle plugins for plugin packaging

## Core Technologies

### Language & Parsing
- **ANTLR4**: Grammar-based parser for SQL and PPL
- **Apache Calcite**: Query optimization framework (V3 engine)
- **Lombok**: Code generation for boilerplate reduction

### Testing
- **JUnit**: Unit testing framework
- **Mockito**: Mocking framework (version 5.7.0)
- **Hamcrest**: Matcher library (version 2.1)
- **JaCoCo**: Code coverage tool (0.8.12)
- **Python doctest**: Documentation testing
- **OpenSearch Test Framework**: Integration testing with in-memory clusters

### Key Dependencies
- **Guava**: 33.3.0-jre
- **AWS Java SDK**: 1.12.651
- **Resilience4j**: 1.5.0 (fault tolerance)
- **Commons IO**: 2.14.0
- **Commons Lang3**: 3.18.0
- **Commons Text**: 1.10.0

## Common Commands

### Building
```bash
# Full build (includes all tests)
./gradlew build

# Fast build (skip integration tests)
./gradlew build -x integTest

# Build specific module
./gradlew :<module_name>:build

# Generate plugin distribution
./gradlew assemble
```

### Testing
```bash
# Run all unit tests
./gradlew test

# Run integration tests
./gradlew :integ-test:integTest

# Run specific test class
./gradlew :integ-test:integTest -Dtests.class="*QueryIT"

# Run REST integration tests
./gradlew :integ-test:yamlRestTest

# Run documentation tests
./gradlew :doctest:doctest

# Run specific doc files
./gradlew :doctest:doctest -Pdocs=search,fields

# Skip Prometheus tests (if unavailable)
./gradlew :integ-test:integTest -DignorePrometheus
```

### Code Quality
```bash
# Check code formatting
./gradlew spotlessCheck

# Apply code formatting
./gradlew spotlessApply

# Run mutation testing
./gradlew pitest
```

### Development
```bash
# Generate ANTLR parser from grammar
./gradlew generateGrammarSource

# Compile Java sources
./gradlew compileJava

# Run plugin locally with OpenSearch
./gradlew :opensearch-sql-plugin:run

# Run with remote debugging enabled
./gradlew opensearch-sql:run -DdebugJVM
```

### Cleaning
```bash
# Clean build artifacts
./gradlew clean
```

## Code Formatting

- **Spotless**: Enforces Google Java Format style
- **Max line length**: 100 characters
- **Indentation**: 2 spaces
- **License header**: Apache 2.0 (automatically added by Spotless)
- **Import order**: Alphabetical, static imports first

## Remote Debugging

Add to `config/jvm.options`:
```
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
```

Or use the `debugJVM` flag when running.
