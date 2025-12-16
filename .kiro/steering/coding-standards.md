---
inclusion: always
---

# General Coding Standards

High-level design principles for code generation in this project. For detailed code review standards (naming conventions, JavaDoc, testing requirements, etc.), see `.rules/REVIEW_GUIDELINES.md` used by CodeRabbit.

## Scope
- Applies to all code in this repository (Java and non-Java).
- Focuses on design philosophy and architectural principles.
- Language-specific implementation details are in separate steering files.

## Design Principles
- Prefer composition over inheritance or deep type hierarchies.
- Program to abstractions, not concrete implementations.
- Single Responsibility: each module/class has one reason to change.
- DRY: don't repeat yourself in logic, data, or intent.
- Deep modules: simple interfaces, complex implementations hidden.

## Error Handling
- Fail fast: detect problems as early as possible.
- Design by contract: be clear about preconditions, postconditions, and invariants.

## Code Quality
- Refactor in small, safe steps with test coverage.
- Boy Scout Rule: leave code cleaner than you found it.
- Extract helpers to improve readability.

## Performance
- Prefer simpler, correct code over premature micro-optimizations.
- Be mindful of resource usage and lifecycles.
