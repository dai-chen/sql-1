---
inclusion: always
---

# PPL Development Guidelines

Architectural principles for all PPL (Piped Processing Language) development in the OpenSearch SQL plugin.

## V3/Calcite-Only Development (CRITICAL)

**All PPL development MUST target V3 engine (Apache Calcite) exclusively.**

### What NOT to Do:
- Do NOT modify `BuiltinFunctionRepository`, `Analyzer` or `ExpressionAnalyzer`
- Do NOT work in the `legacy` module
- Do NOT add functionality to V2 engine components
- Do NOT create bridges between V2 and V3 systems

## Architecture
- Always follow visitor pattern for AST traversal and transformation
- Maintain clear separation: parsing (ANTLR) → AST construction → execution
- Prefer composition over inheritance for new command functionality
- Ensure integration with Calcite query planning framework
- Design for extensibility

## AST Design
- Prefer reusing existing AST nodes (command arguments) over creating new expression nodes
- Ensure proper parent-child relationships in AST structures
- Follow naming conventions: commands use noun forms, visitors use visit* pattern

## Code Consistency
- Study existing PPL commands before implementing new ones
- Maintain consistency with established error handling and logging patterns
- Follow existing package structure and class organization
- Use meaningful names indicating command purpose
- Reuse existing utility functions and common patterns

## Testing Requirements
- Every PPL feature requires comprehensive test coverage at multiple levels
- Unit tests: verify logical plans and SQL translation
- Integration tests: real-world query scenarios with complex data
- Include edge cases, error scenarios, and performance considerations
- Test with various data types and sizes

## Performance
- Evaluate pushdown optimization opportunities for new operations
- Consider memory usage for large datasets
- Leverage OpenSearch's distributed architecture
- Profile performance impact before finalizing
- Document performance limitations

## Integration
- Work correctly with both pushdown and non-pushdown execution paths
- Ensure cross-cluster search compatibility when applicable
- Verify integration with data anonymizer for privacy compliance
- Maintain backward compatibility
- Consider interaction with existing PPL commands in piped operations

## Documentation
- Every feature requires user-facing documentation (syntax and usage)
- Include practical examples demonstrating real-world use cases
- Document limitations, constraints, and performance considerations
- Maintain consistency with existing PPL documentation format
- Update PPL command index when adding new commands

## Quality Gates
- All gradle test commands must pass
- Code review verifies adherence to principles and patterns
- Performance testing validates no regression
- Documentation review ensures clarity and completeness
- Integration testing covers interaction with other PPL commands
