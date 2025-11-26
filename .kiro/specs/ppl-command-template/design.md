# Design: [Command Name]

**Phase:** 2 - Solution Design  
**Status:** Draft  
**Owner:** [Your Name]  
**Date:** [Date]  
**Input Requirements:** #[[file:requirements.md]]

## High-Level Decisions

### 1. Subset or Composition?
- [ ] **Subset** of existing command: [which command]
- [ ] **Composition** of existing PPL operators: [which operators]

**Coverage proof:** [1-2 line explanation showing this covers v1 scope and 3 use cases]

### 2. Equivalent SQL

For each use case from requirements:

**Use Case 1:**
```sql
SELECT ... FROM ... WHERE ...
```

**Use Case 2:**
```sql
SELECT ... FROM ... WHERE ...
```

**Use Case 3:**
```sql
SELECT ... FROM ... WHERE ...
```

Or: **N/A** (if subset/composition fully covers)

### 3. Pushdown Feasibility
- [ ] **Full** - entire operation can push to OpenSearch DSL
- [ ] **Partial** - some operations push, boundary: [describe]
- [ ] **None** - must execute in Calcite

### 4. UDF/UDAF Needed?
- [ ] **None**
- [ ] Required functions:
  - `functionName(arg1: type, arg2: type): returnType` - [purpose]

## Similar Commands Reference

Commands studied for patterns:
- `[command1]` - [why similar]
- `[command2]` - [why similar]

## Evidence from Recent PRs

PRs inspected for file-impact patterns:
- #[PR number] - [command name] - [key files changed]
- #[PR number] - [command name] - [key files changed]

## Planned Change List

### Grammar/Parser
- `ppl/src/main/antlr/OpenSearchPPLParser.g4` - [changes]
- `ppl/src/main/antlr/OpenSearchPPLLexer.g4` - [changes]

### AST Nodes
- `core/src/main/java/org/opensearch/sql/ast/tree/[Node].java` - [new/modified]

### Visitors
- `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java` - [changes]
- `ppl/src/main/java/org/opensearch/sql/ppl/planner/CalciteRelNodeVisitor.java` - [changes]
- `core/src/main/java/org/opensearch/sql/ast/tree/DataAnonymizer.java` - [changes]

### Tests
- Unit: `ppl/src/test/java/org/opensearch/sql/ppl/[Command]Test.java`
- Syntax: `ppl/src/test/java/org/opensearch/sql/ppl/parser/[Command]ParserTest.java`
- Integration: `integ-test/src/test/java/org/opensearch/sql/ppl/[Command]IT.java`
  - Pushdown tests
  - Non-pushdown tests
  - Explain tests
  - V2 compatibility tests
  - Anonymizer tests

### Documentation
- `docs/user/ppl/cmd/[commandname].rst` - [new file]
- `docs/category.json` - [add entry]

## Design Notes

### Risk/Complexity
[Brief paragraph on implementation risks or complexity concerns]

### Performance Expectations
[Expected performance characteristics]

### Assumptions
[Critical assumptions that must hold during implementation]

## Task Breakdown (Ordered)

1. Grammar/Parser updates
2. AST node creation/modification
3. AstBuilder visitor implementation
4. CalciteRelNodeVisitor implementation
5. Anonymizer integration
6. Unit tests
7. Syntax parser tests
8. Integration tests (pushdown)
9. Integration tests (non-pushdown)
10. Integration tests (explain)
11. Integration tests (v2 compat)
12. Integration tests (anonymizer)
13. Documentation (.rst + index)
14. Doctest acceptance
15. Final test suite & PR

## Test Mapping

### Doctest Acceptance
All three use cases from requirements.md must be in `docs/user/ppl/cmd/[commandname].rst` as doctest blocks.

**Use Case 1:** [maps to Example 1 in docs]  
**Use Case 2:** [maps to Example 2 in docs]  
**Use Case 3:** [maps to Example 3 in docs]  
**Negative Case:** [error scenario with expected error message]

## Definition of Done

- [ ] Doctest acceptance passes for 3 use cases + negative case
- [ ] Unit tests pass locally
- [ ] Syntax tests pass
- [ ] Integration tests pass (all types)
- [ ] Docs complete; examples match outputs
- [ ] Error messages stable with IDs
- [ ] Pushdown behavior aligns with decision
- [ ] PR includes links to requirements and design

## Mandatory Test Gates

```bash
# Command-specific integration tests
./gradlew :integ-test:integTest -Dtests.class="*[Command]IT"

# Command-specific doctest
./gradlew :doctest:doctest -DignorePrometheus -Pdocs="[commandname].rst"

# Full test suite
./gradlew test
./gradlew :integ-test:integTest
./gradlew :doctest:doctest -DignorePrometheus
```

## Approval

- **Approver:** [Name]
- **Date:** [Date]
- **Status:** ⬜ APPROVE: 2.2 DECISIONS | ⬜ APPROVE: SOLUTION
