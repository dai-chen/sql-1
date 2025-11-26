---
inclusion: always
---

# Java Coding Standards

Core principles for all Java development in this project, emphasizing clean code and maintainable design.

## Naming & Clarity
- Use intention-revealing names that express purpose, not implementation
- Choose meaningful variable names over explanatory comments
- Functions should do one thing well - aim for 20 lines or fewer

## Design Principles
- Composition over inheritance
- Program to interfaces, not implementations
- Single Responsibility: each class has one reason to change
- DRY: don't repeat yourself in logic, data, or intent
- Deep modules: simple interfaces, complex implementations hidden

## Error Handling
- Fail-fast: detect problems early
- Use exceptions for exceptional circumstances only
- Validate inputs at system boundaries and public methods
- Return empty collections instead of null
- Design by contract: specify preconditions, postconditions, invariants

## Code Quality
- Refactor in small, safe steps with test coverage
- Boy Scout Rule: leave code cleaner than you found it
- Extract methods to improve readability
- Use IDE refactoring tools over manual changes
- Eliminate dead code immediately

## Testing
- Test behavior, not implementation details
- Use descriptive test names explaining the scenario
- Arrange-Act-Assert pattern for clear structure
- Mock external dependencies to isolate units

## Performance
- Prefer immutable objects for simplicity and thread safety
- Use appropriate data structures for access patterns
- Close resources with try-with-resources
- Measure before optimizing - avoid premature optimization
