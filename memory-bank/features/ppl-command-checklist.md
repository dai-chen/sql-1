# PPL Command Implementation Checklist

## Prerequisites
- **REQUIRED**: An approved RFC document containing:
  - Complete syntax specification
  - Parameter definitions and validation rules
  - Usage examples and acceptance criteria
  - Technical implementation approach
  - Testing requirements

## Implementation Process

### 1. RFC Analysis & Pattern Study
- [ ] Review approved RFC specification for implementation details
- [ ] Study existing PPL commands for consistency and reusable patterns
- [ ] Identify integration points with existing infrastructure

### 2. Grammar & Parser Implementation
- [ ] **Lexer Updates**: Add new keywords to `ppl/src/main/antlr4/OpenSearchPPLLexer.g4`
- [ ] **Parser Rules**: Add grammar rules to `ppl/src/main/antlr4/OpenSearchPPLParser.g4`
- [ ] **Rule Updates**: Update `commandName` and `keywordsCanBeId` appropriately
- [ ] **Grammar Testing**: Verify parser accepts new syntax correctly

### 3. AST (Abstract Syntax Tree) Development
- [ ] **Tree Nodes**: Create new tree nodes under `core/src/main/java/org/opensearch/sql/ast/tree/`
- [ ] **Argument Reuse**: Prefer reusing `Argument` class for command arguments
- [ ] **Expression Avoidance**: Avoid creating unnecessary expression nodes under `org.opensearch.sql.ast.expression`
- [ ] **Node Structure**: Ensure proper parent-child relationships in AST

### 4. Visitor Pattern Implementation
- [ ] **Base Visitor**: Add `visit*` methods in `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java`
- [ ] **Required Overrides**: Override `visit*` in all required visitors:
  - [ ] `CalciteRelNodeVisitor` - for query plan generation (`opensearch/src/main/java/org/opensearch/sql/opensearch/planner/CalciteRelNodeVisitor.java`)
  - [ ] `PPLQueryDataAnonymizer` - for data anonymization (if applicable)

### 5. Comprehensive Testing Strategy

#### Unit Tests
- [ ] **Base Class**: Extend `CalcitePPLAbstractTest` (`opensearch/src/test/java/org/opensearch/sql/opensearch/CalcitePPLAbstractTest.java`)
- [ ] **Test Queries**: Keep test queries minimal and focused
- [ ] **Verification**: Include `verifyLogical()` and `verifyPPLToSparkSQL()`
- [ ] **Run**: `./gradlew :ppl:test --tests 'org.opensearch.sql.ppl.antlr.PPLSyntaxParserTest'`

#### Integration Tests (Pushdown)
- [ ] **Base Class**: Extend `PPLIntegTestCase` (`integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java`)
- [ ] **Real-world Queries**: Use complex, realistic query scenarios
- [ ] **Schema & Data**: Include `verifySchema()` and `verifyDataRows()`
- [ ] **Run**: `./gradlew :integ-test:integTest -Dtests.class="YourTestClass"`

#### Integration Tests (Non-pushdown)
- [ ] **Test Location**: Add test class to `integ-test/src/test/java/org/opensearch/sql/calcite/CalciteNoPushdownIT.java`
- [ ] **Fallback Testing**: Verify non-pushdown execution path

#### Explain Tests
- [ ] **Test Location**: Add tests to `integ-test/src/test/java/org/opensearch/sql/calcite/CalciteExplainIT.java`
- [ ] **Query Plans**: Verify logical and physical plan generation

#### V2 Compatibility
- [ ] **Test Location**: Add test in `integ-test/src/test/java/org/opensearch/sql/calcite/NewAddedCommandsIT.java`
- [ ] **Backward Compatibility**: Ensure new commands don't break v2 engine

#### Anonymizer Tests
- [ ] **Test Location**: Add test in `core/src/test/java/org/opensearch/sql/analysis/PPLQueryDataAnonymizerTest.java`
- [ ] **Data Privacy**: Verify sensitive data is properly anonymized

#### Cross-cluster Tests (Optional)
- [ ] **Test Location**: Add test in `integ-test/src/test/java/org/opensearch/sql/ppl/CrossClusterSearchIT.java`
- [ ] **Multi-cluster**: Verify command works across cluster boundaries

### 6. Documentation
- [ ] **User Documentation**: Create `.rst` file under `docs/user/ppl/cmd/`
- [ ] **Index Linking**: Link new documentation to `docs/user/ppl/index.rst`
- [ ] **Content Requirements**:
  - [ ] Syntax definition
  - [ ] Parameter descriptions
  - [ ] Usage examples
  - [ ] Performance considerations
  - [ ] Limitations and constraints

## Quality Assurance Checklist

### Code Standards
- [ ] Follow existing PPL command naming conventions
- [ ] Maintain consistent error handling patterns
- [ ] Ensure proper logging and debugging support
- [ ] Follow OpenSearch SQL coding standards

### Testing Standards
- [ ] Achieve comprehensive test coverage
- [ ] Include edge cases and error scenarios
- [ ] Verify performance implications
- [ ] Test with various data types and sizes

### Documentation Standards
- [ ] Provide clear syntax definitions
- [ ] Include practical usage examples
- [ ] Document limitations and constraints
- [ ] Maintain consistency with existing docs

## Gradle Test Commands Reference

```bash
# Run specific unit test
./gradlew :ppl:test --tests 'org.opensearch.sql.ppl.antlr.PPLSyntaxParserTest'

# Run all PPL tests
./gradlew :ppl:test

# Run specific integration test
./gradlew :integ-test:integTest -Dtests.class="YourTestClass"

# Run all integration tests
./gradlew :integ-test:integTest

# Run tests with specific method
./gradlew :integ-test:integTest -Dtests.class="YourTestClass" -Dtests.method="testMethodName"

# Run tests with debug output
./gradlew :integ-test:integTest -Dtests.class="YourTestClass" --debug
```

## Final Delivery Checklist
- [ ] All source code changes following OpenSearch SQL patterns
- [ ] Complete test coverage at all required levels
- [ ] Proper user documentation with examples
- [ ] Integration verification with existing PPL infrastructure
- [ ] All gradle test commands pass successfully
- [ ] Code follows established PPL command patterns
- [ ] Comprehensive test coverage including edge cases
- [ ] User documentation is complete and accurate
- [ ] Integration with existing commands is verified
