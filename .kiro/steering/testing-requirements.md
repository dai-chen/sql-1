---
inclusion: fileMatch
fileMatchPattern: "**/*Test*.java"
---

# Testing Requirements for PPL Commands

## Mandatory Test Gates

All PPL command implementations must pass these gates:

```bash
# Unit tests
./gradlew test

# Integration tests
./gradlew :integ-test:integTest

# Documentation tests (doctest)
./gradlew :doctest:doctest -DignorePrometheus
```

## Test Coverage Requirements

### 1. Unit Tests
- Verify logical plans
- Verify SQL translation
- Test all parameter combinations
- Cover edge cases and error scenarios

### 2. Integration Tests
Required test types:
- Pushdown execution path
- Non-pushdown execution path
- EXPLAIN output verification
- V2 compatibility (if applicable)
- Data anonymizer integration

### 3. Doctest Acceptance
- Place all examples in `docs/user/ppl/cmd/[commandname].rst`
- Must include 3 use case examples
- Must include 1 negative case with expected error
- All doctests must pass
- Examples must produce non-trivial outputs (≥2 rows & ≥2 columns unless command inherently produces one)

## Anti-Skip Rules
- Do NOT use `-x test` or skip test execution
- Do NOT use `@Ignore`, `@Disabled`, or `Assume.*` for new tests
- All tests must be executable and passing

## Test Data
- Use realistic data scenarios
- Test with various data types
- Include null/missing value handling
- Test with different data sizes
