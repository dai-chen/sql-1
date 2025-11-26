# Implementation Tasks: [Command Name]

**Phase:** 3 - Implementation  
**Status:** Not Started  
**Owner:** [Your Name]  
**Date:** [Date]  
**Design:** #[[file:design.md]]  
**Requirements:** #[[file:requirements.md]]

## Implementation Contract

This file tracks implementation progress. All work must align with the approved design.

**Source of Truth:** design.md (approved with "APPROVE: SOLUTION")

## Task Checklist

### 1. Grammar/Parser
- [ ] Update `OpenSearchPPLParser.g4`
- [ ] Update `OpenSearchPPLLexer.g4` (if needed)
- [ ] Verify grammar compiles: `./gradlew :ppl:compileJava`

### 2. AST Nodes
- [ ] Create/modify AST node classes
- [ ] Implement visitor methods
- [ ] Verify compilation

### 3. Visitors

#### AstBuilder
- [ ] Implement visit method for new command
- [ ] Handle parameter parsing
- [ ] Build AST node

#### CalciteRelNodeVisitor
- [ ] Implement logical plan generation
- [ ] Handle SQL translation
- [ ] Implement pushdown logic (if applicable)

#### DataAnonymizer
- [ ] Add anonymization support
- [ ] Handle sensitive field masking

#### Other Visitors
- [ ] [List other visitors that need updates]

### 4. Unit Tests
- [ ] Create `[Command]Test.java`
- [ ] Test logical plan generation
- [ ] Test SQL translation
- [ ] Test parameter validation
- [ ] Test edge cases
- [ ] Run: `./gradlew test --tests "*[Command]Test"`

### 5. Syntax Parser Tests
- [ ] Create `[Command]ParserTest.java`
- [ ] Test valid syntax variations
- [ ] Test invalid syntax (error cases)
- [ ] Run: `./gradlew test --tests "*[Command]ParserTest"`

### 6. Integration Tests
- [ ] Create `[Command]IT.java`
- [ ] Pushdown execution tests
- [ ] Non-pushdown execution tests
- [ ] Explain output tests
- [ ] V2 compatibility tests (if applicable)
- [ ] Anonymizer integration tests
- [ ] Run: `./gradlew :integ-test:integTest -Dtests.class="*[Command]IT"`

### 7. Documentation
- [ ] Create `docs/user/ppl/cmd/[commandname].rst`
- [ ] Add 3 use case examples (from requirements)
- [ ] Add 1 negative case example
- [ ] Add to `docs/category.json`
- [ ] Verify doctest syntax

### 8. Doctest Acceptance
- [ ] Run: `./gradlew :doctest:doctest -DignorePrometheus -Pdocs="[commandname].rst"`
- [ ] All 3 use cases pass
- [ ] Negative case passes with expected error
- [ ] Outputs match requirements

### 9. Final Test Suite
- [ ] Run: `./gradlew test`
- [ ] Run: `./gradlew :integ-test:integTest`
- [ ] Run: `./gradlew :doctest:doctest -DignorePrometheus`
- [ ] All gates pass

### 10. PR Preparation
- [ ] Create feature branch: `feature/ppl-[command]`
- [ ] Commit history is clean and reviewable
- [ ] PR description includes:
  - Link to requirements.md
  - Link to design.md
  - Summary of high-level decisions
  - Doctest acceptance evidence
  - Test status summary

## Reconciliation Checklist

Before marking complete, verify:

- [ ] All files in design.md "Planned Change List" are implemented
- [ ] Examples in docs match the 3 use cases from requirements.md
- [ ] Examples produce non-trivial outputs (≥2 rows & ≥2 columns)
- [ ] All Definition of Done items from design.md are satisfied
- [ ] No unplanned scope was added

**Reconciliation Status:** ⬜ RECONCILE: PASS ✅

## Implementation Notes

[Add notes during implementation about decisions, challenges, or deviations that required design updates]

## Completion

- [ ] All tasks complete
- [ ] All tests passing
- [ ] PR opened and linked
- [ ] Code review requested
