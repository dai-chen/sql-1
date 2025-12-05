---
inclusion: always
---

# General Coding Standards

Core principles for all development in this project, emphasizing clean, maintainable code and clear design.

## Scope
- Applies to all code in this repository (Java and non-Java).
- Focuses on general engineering practices: readability, design, testing, and refactoring.
- Language- or domain-specific rules are defined in separate steering files.

## Naming & Clarity
- Use intention-revealing names that express purpose, not implementation.
- Choose meaningful variable and function names over explanatory comments.
- Functions should do one thing well – aim for short, focused units (around 20 lines or fewer, not a hard limit).

## Design Principles
- Prefer composition over inheritance or deep type hierarchies.
- Program to abstractions, not concrete implementations.
- Single Responsibility: each module/class has one reason to change.
- DRY: don't repeat yourself in logic, data, or intent.
- Deep modules: simple interfaces, complex implementations hidden.

## Error Handling
- Fail fast: detect problems as early as possible.
- Use the language’s error/exception mechanism for truly exceptional conditions.
- Validate inputs at system boundaries and public APIs.
- Prefer empty collections / safe defaults over returning null when possible.
- Design by contract: be clear about preconditions, postconditions, and invariants.

## Code Quality
- Refactor in small, safe steps with test coverage.
- Boy Scout Rule: leave code cleaner than you found it.
- Extract helpers to improve readability.
- Use editor/IDE refactoring tools over manual large-scale edits.
- Eliminate dead code immediately.

## Testing
- Test behavior, not internal implementation details.
- Use descriptive test names that explain the scenario and expected outcome.
- Follow Arrange–Act–Assert (or similar) for clear structure.
- Isolate units from external dependencies where appropriate (mocks/stubs/fakes).

## Comments & Documentation
- Comment the *why* and non-obvious decisions, not what the code already says.
- Avoid comments that simply restate straightforward code.
- Document public APIs and any complex or surprising behavior.

## Performance
- Choose appropriate data structures for real access patterns.
- Prefer simpler, correct code over premature micro-optimizations.
- Measure before optimizing – let profiling guide performance work.
- Be mindful of resource usage and lifecycles.
