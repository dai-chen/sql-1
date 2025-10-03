## Brief overview
High-level principles and architectural guidelines for all PPL (Piped Processing Language) development in the OpenSearch SQL plugin. These principles apply to new commands, functions, optimizations, and any PPL-related enhancements. All the changes should be focused on latest V3 engine based on Calcite.

## Architectural principles
- Always follow the visitor pattern for AST traversal and transformation
- Maintain clear separation between parsing (ANTLR), AST construction, and execution layers
- Prefer composition over inheritance when implementing new command functionality
- Ensure all new features integrate properly with the Calcite query planning framework
- Design for extensibility - new commands should follow patterns that allow future enhancements

## AST design patterns
- Prefer reusing the existing AST node like command arguments over creating new expression nodes
- Ensure proper parent-child relationships in all AST structures
- Follow established naming conventions: commands use noun forms, visitors use visit* pattern

## Code consistency standards
- Study existing PPL commands for patterns before implementing new ones
- Maintain consistency with established error handling and logging patterns
- Follow the existing package structure and class organization
- Use meaningful names that clearly indicate the command's purpose
- Reuse existing utility functions and common patterns wherever possible

## Testing philosophy
- Every new PPL feature requires comprehensive test coverage at multiple levels
- Unit tests should verify both logical plans and SQL translation
- Integration tests must include real-world query scenarios with complex data
- Always include edge cases, error scenarios, and performance considerations in tests
- Test with various data types and sizes to ensure robustness

## Performance considerations
- Always evaluate pushdown optimization opportunities for new operations
- Consider memory usage implications for operations on large datasets
- Design commands to leverage OpenSearch's distributed architecture
- Profile performance impact before finalizing implementation
- Document any performance limitations or considerations

## Integration requirements
- New features must work correctly with both pushdown and non-pushdown execution paths
- Ensure compatibility with cross-cluster search functionality when applicable
- Verify integration with the data anonymizer for privacy compliance
- Maintain backward compatibility - new commands shouldn't break existing functionality
- Consider how new features interact with existing PPL commands in piped operations

## Documentation standards
- Every PPL feature requires user-facing documentation explaining syntax and usage
- Include practical examples that demonstrate real-world use cases
- Document any limitations, constraints, or performance considerations
- Maintain consistency with existing PPL documentation format and style
- Update the PPL command index when adding new commands

## Quality gates
- All gradle test commands must pass before considering implementation complete
- Code review should verify adherence to these principles and existing patterns
- Performance testing should validate no regression in existing functionality
- Documentation review should ensure clarity and completeness
- Integration testing must cover interaction with other PPL commands
