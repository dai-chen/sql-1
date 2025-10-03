---
description: "A comprehensive step-by-step workflow for implementing PPL commands in the OpenSearch SQL plugin, following established patterns and ensuring complete test coverage."
author: "OpenSearch SQL Team"
version: "1.0"
tags: ["ppl", "opensearch", "sql plugin", "command implementation", "calcite", "testing", "antlr", "grammar"]
globs: ["**/*.java", "**/*.g4", "**/*.md", "**/*.rst", "**/build.gradle"]
---

<detailed_sequence_of_steps>

# PPL Command Implementation Workflow

This is a step-by-step workflow for implementing PPL commands. Follow each step sequentially and verify completion before proceeding.

## Prerequisites Verification

### Step 1: RFC & Requirements Analysis
- [ ] **VERIFY**: RFC document exists and is approved
- [ ] **READ**: Complete syntax specification from RFC
- [ ] **DOCUMENT**: Parameter definitions and validation rules
- [ ] **EXTRACT**: Usage examples and acceptance criteria
- [ ] **NOTE**: Technical implementation approach from RFC

### Step 2: Pattern Analysis (CRITICAL - Don't Skip!)
- [ ] **IDENTIFY**: Find 2-3 most similar existing PPL commands
- [ ] **STUDY**: Examine their complete implementation patterns:
  - Grammar structure in lexer/parser files
  - AST node design and relationships
  - All visitor implementations (count them!)
  - Test file locations and patterns
  - Documentation structure
- [ ] **DOCUMENT**: File paths and patterns to follow
- [ ] **COPY**: Test structure templates from similar commands

**Similar Command Examples to Study:**
- For data manipulation: `fillnull`, `eval`, `fields`
- For filtering: `where`, `search`
- For aggregation: `stats`, `rare`, `top`

## Implementation Phase

### Step 3: Grammar & Parser Implementation
- [ ] **BACKUP**: Create git branch for changes
- [ ] **MODIFY**: `ppl/src/main/antlr/OpenSearchPPLLexer.g4`
  - Add new keywords following existing patterns
- [ ] **MODIFY**: `ppl/src/main/antlr/OpenSearchPPLParser.g4`
  - Add grammar rules following command patterns
  - Update `commandName` rule
  - Update `keywordsCanBeId` section
- [ ] **GENERATE**: Run `./gradlew :ppl:generateGrammarSource`
- [ ] **VERIFY**: No compilation errors after grammar generation

### Step 4: AST Node Implementation
- [ ] **CREATE/UPDATE**: AST nodes in `core/src/main/java/org/opensearch/sql/ast/tree/`
  - Follow naming conventions from similar commands
  - Prefer reusing `Argument` class for command arguments
  - Ensure proper parent-child relationships
- [ ] **VERIFY**: AST node compiles and follows patterns

### Step 5: Visitor Pattern Implementation (COMPLETE ALL!)

#### Step 5.1: Base Abstract Visitor
- [ ] **UPDATE**: `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java`
  - Add `visit*` method for new AST node
  - Follow exact naming conventions

#### Step 5.2: AST Builder Visitor
- [ ] **UPDATE**: `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java`
  - Add visitor method for grammar rule
  - Handle parsing and AST construction
  - **VERIFY**: Compilation successful

#### Step 5.3: Calcite Query Planner (CRITICAL)
- [ ] **UPDATE**: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
  - Add visitor method for query plan generation
  - Follow patterns from similar commands
  - Handle logical plan creation

#### Step 5.4: Data Anonymizer (REQUIRED)
- [ ] **UPDATE**: `core/src/main/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizer.java`
  - Add visitor method for data anonymization
  - Follow privacy patterns from similar commands

#### Step 5.5: Additional Visitors (Check Similar Commands)
- [ ] **CHECK**: If similar commands have other visitor implementations
- [ ] **UPDATE**: Any additional required visitors found

### Step 6: Initial Verification
- [ ] **COMPILE**: `./gradlew :ppl:compileJava`
- [ ] **VERIFY**: No compilation errors
- [ ] **BASIC TEST**: Create minimal test to verify parsing works

## Comprehensive Testing Phase

### Step 7: Unit Tests (Base Layer)
- [ ] **CREATE/UPDATE**: Test class extending `CalcitePPLAbstractTest`
  - Location: `ppl/src/test/java/org/opensearch/sql/ppl/calcite/`
  - Follow naming: `CalcitePPL[CommandName]Test.java`
- [ ] **IMPLEMENT**: Minimal focused test queries
- [ ] **INCLUDE**: `verifyLogical()` and `verifyPPLToSparkSQL()` calls
- [ ] **RUN**: `./gradlew :ppl:test --tests "YourTestClass"`
- [ ] **VERIFY**: All unit tests pass

### Step 8: Syntax Parser Tests
- [ ] **UPDATE**: `ppl/src/test/java/org/opensearch/sql/ppl/antlr/PPLSyntaxParserTest.java`
  - Add test methods for new syntax variations
- [ ] **RUN**: `./gradlew :ppl:test --tests "PPLSyntaxParserTest"`
- [ ] **VERIFY**: Parser correctly handles new syntax

### Step 9: Integration Tests - Pushdown
- [ ] **CREATE**: Test class extending `PPLIntegTestCase`
  - Location: `integ-test/src/test/java/org/opensearch/sql/ppl/`
- [ ] **IMPLEMENT**: Complex, realistic query scenarios
- [ ] **INCLUDE**: `verifySchema()` and `verifyDataRows()`
- [ ] **RUN**: `./gradlew :integ-test:integTest -Dtests.class="YourTestClass"`
- [ ] **VERIFY**: Integration tests pass

### Step 10: Integration Tests - Non-pushdown
- [ ] **UPDATE**: `integ-test/src/test/java/org/opensearch/sql/calcite/CalciteNoPushdownIT.java`
- [ ] **ADD**: Test method for non-pushdown execution path
- [ ] **RUN**: Integration test and verify fallback works

### Step 11: Explain Tests
- [ ] **UPDATE**: `integ-test/src/test/java/org/opensearch/sql/calcite/CalciteExplainIT.java`
- [ ] **ADD**: Tests for logical and physical plan generation
- [ ] **VERIFY**: Query plans are generated correctly

### Step 12: V2 Compatibility Tests
- [ ] **UPDATE**: `integ-test/src/test/java/org/opensearch/sql/calcite/NewAddedCommandsIT.java`
- [ ] **ADD**: Test to ensure backward compatibility
- [ ] **VERIFY**: New command doesn't break v2 engine

### Step 13: Anonymizer Tests
- [ ] **UPDATE**: `core/src/test/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizerTest.java`
- [ ] **ADD**: Test for data privacy/anonymization
- [ ] **VERIFY**: Sensitive data is properly handled

### Step 14: Cross-cluster Tests (Optional)
- [ ] **EVALUATE**: If command needs cross-cluster support
- [ ] **UPDATE**: `integ-test/src/test/java/org/opensearch/sql/ppl/CrossClusterSearchIT.java` (if needed)

## Documentation Phase

### Step 15: User Documentation
- [ ] **CREATE**: `.rst` file under `docs/user/ppl/cmd/`
  - Follow naming: `[commandname].rst`
  - Copy structure from similar command docs
- [ ] **INCLUDE**: 
  - Syntax definition
  - Parameter descriptions  
  - Usage examples
  - Performance considerations
  - Limitations and constraints
- [ ] **UPDATE**: `docs/user/ppl/index.rst` to link new documentation

## Final Verification

### Step 16: Complete Test Suite
- [ ] **RUN**: `./gradlew :ppl:test` (all PPL tests)
- [ ] **RUN**: `./gradlew :integ-test:integTest` (all integration tests)
- [ ] **VERIFY**: No regressions in existing functionality
- [ ] **VERIFY**: All new tests pass

### Step 17: Code Quality Check
- [ ] **REVIEW**: Code follows existing PPL command patterns
- [ ] **VERIFY**: Error handling matches established patterns
- [ ] **CHECK**: Logging and debugging support included
- [ ] **CONFIRM**: OpenSearch SQL coding standards followed

### Step 18: Documentation Review
- [ ] **VERIFY**: Documentation is complete and accurate
- [ ] **CHECK**: Examples work as documented
- [ ] **CONFIRM**: Consistent with existing documentation style

</detailed_sequence_of_steps>

## Completion Checklist

- [ ] All grammar and AST changes implemented
- [ ] ALL visitor patterns implemented (no missing visitors!)
- [ ] Complete test coverage at all levels:
  - [ ] Unit tests
  - [ ] Syntax parser tests
  - [ ] Integration tests (pushdown)
  - [ ] Integration tests (non-pushdown)
  - [ ] Explain tests
  - [ ] V2 compatibility tests
  - [ ] Anonymizer tests
- [ ] User documentation created and linked
- [ ] All tests pass without regressions
- [ ] Code follows established patterns

## Common Pitfalls to Avoid

1. **Don't skip visitor implementations** - Every visitor needs updating
2. **Don't skip integration tests** - Unit tests alone are insufficient
3. **Don't forget anonymizer support** - Privacy compliance is required
4. **Don't skip documentation** - User-facing docs are mandatory
5. **Don't implement in isolation** - Always study similar command patterns first

## Emergency Recovery

If something breaks:
- [ ] **REVERT**: Git checkout to backup branch
- [ ] **REGENERATE**: `./gradlew :ppl:generateGrammarSource`
- [ ] **RECOMPILE**: `./gradlew :ppl:compileJava`
- [ ] **RETEST**: Start from step that failed

---

**Remember**: This is a complete production implementation workflow. Don't skip steps even for "quick fixes" - incomplete implementations cause integration issues later.
