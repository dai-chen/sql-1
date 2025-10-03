## Brief overview
Universal Java development principles derived from classic software engineering literature. These guidelines apply to all Java coding tasks, emphasizing clean code, solid architecture, and maintainable design.

## Code quality fundamentals
- Use intention-revealing names that express purpose, not implementation details
- Functions should do one thing and do it well - aim for 20 lines or fewer
- Avoid comments that explain what code does; prefer comments that explain why
- Choose meaningful variable names over explanatory comments
- Eliminate dead code immediately rather than commenting it out

## Object-oriented design principles
- Composition over inheritance: favor object composition over class inheritance
- Program to interfaces, not implementations
- Dependency inversion: depend on abstractions, not concretions
- Single Responsibility Principle: each class should have only one reason to change
- DRY principle: don't repeat yourself in logic, data, or intent

## Complexity management
- Deep modules: design modules with simple interfaces and complex implementations
- Information hiding: conceal implementation details behind clean abstractions
- Interface design: make interfaces easy to use correctly and hard to use incorrectly
- Strategic programming: invest time in good design to reduce future complexity
- Minimize cognitive load: reduce the mental effort required to understand code

## Error handling and defensive programming
- Fail-fast approach: detect problems early and terminate execution immediately
- Use exceptions for exceptional circumstances, not normal program flow
- Validate inputs at system boundaries and public method entry points
- Return empty collections instead of null to avoid null pointer exceptions
- Design by contract: clearly specify preconditions, postconditions, and invariants

## Refactoring and code evolution
- Identify and eliminate code smells: long methods, large classes, duplicate code
- Refactor in small, safe steps with comprehensive test coverage
- Boy Scout Rule: always leave code cleaner than you found it
- Extract methods to improve readability and reduce complexity
- Use IDE refactoring tools rather than manual changes when possible

## Architecture and design patterns
- Maintain clear separation between layers and enforce dependency directions
- Apply design patterns when they solve actual problems, not for pattern's sake
- Factory patterns for object creation when types vary at runtime
- Observer pattern for loose coupling between objects
- Strategy pattern to encapsulate algorithms and make them interchangeable

## Performance and resource management
- Prefer immutable objects to reduce complexity and improve thread safety
- Use appropriate data structures for the access patterns required
- Close resources properly using try-with-resources or explicit finally blocks
- Measure performance before optimizing; avoid premature optimization
- Consider memory implications of object creation in loops and recursive methods

## Testing and quality assurance
- Write tests first to drive design and ensure testability
- Test behavior, not implementation details
- Use descriptive test method names that explain the scenario being tested
- Arrange-Act-Assert pattern for clear test structure
- Mock external dependencies to isolate units under test
