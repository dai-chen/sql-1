# PPL Parser Patterns

## ANTLR Grammar Structure

### Grammar Files Organization
- **OpenSearchPPLLexer.g4**: Lexical analysis rules for PPL tokens
- **OpenSearchPPLParser.g4**: Syntax rules for PPL command structure
- **Generated Classes**: Auto-generated parser and lexer classes in build directory

### Key Grammar Patterns

#### Command Structure
```antlr
pplStatement
    : searchCommand (PIPE pplCommand)*
    ;

pplCommand
    : whereCommand
    | fieldsCommand
    | statsCommand
    | sortCommand
    | headCommand
    | tailCommand
    | dedupeCommand
    | evalCommand
    ;
```

#### Expression Parsing
```antlr
expression
    : logicalExpression
    | comparisonExpression
    | arithmeticExpression
    | functionCall
    | fieldReference
    | literal
    ;
```

## AST Builder Patterns

### AstBuilder Class Structure
- **Base Class**: `AstBuilder` extends ANTLR's `BaseVisitor`
- **Visit Methods**: One method per grammar rule
- **Return Types**: Typed AST nodes from `org.opensearch.sql.ast.tree`

### Common Builder Patterns
```java
@Override
public UnresolvedPlan visitSearchCommand(SearchCommandContext ctx) {
    return new Search(
        visitTableSource(ctx.tableSource()),
        visitWhereClause(ctx.whereClause())
    );
}

@Override
public UnresolvedExpression visitFieldExpression(FieldExpressionContext ctx) {
    return new Field(ctx.getText());
}
```

## Expression Builder Patterns

### AstExpressionBuilder Specialization
- **Purpose**: Handles complex expression parsing
- **Inheritance**: Extends base AstBuilder for expression-specific logic
- **Key Methods**: Function calls, operators, literals, field references

### Function Call Handling
```java
@Override
public UnresolvedExpression visitFunctionCall(FunctionCallContext ctx) {
    String functionName = ctx.functionName().getText();
    List<UnresolvedExpression> arguments = ctx.functionArgs()
        .stream()
        .map(this::visit)
        .collect(Collectors.toList());
    
    return new Function(functionName, arguments);
}
```

## Command Implementation Patterns

### Standard Command Pattern
1. **Grammar Rule**: Define syntax in `.g4` file
2. **AST Node**: Create corresponding AST class in `core/ast/tree/`
3. **Builder Method**: Implement visitor method in `AstBuilder`
4. **Logical Plan**: Convert AST to logical plan representation
5. **Physical Plan**: Implement execution logic in OpenSearch module

### Example: Stats Command
```java
// AST Node
public class Aggregation extends UnresolvedPlan {
    private final List<UnresolvedExpression> aggregators;
    private final List<UnresolvedExpression> groupByList;
    // ... implementation
}

// Builder Method
@Override
public UnresolvedPlan visitStatsCommand(StatsCommandContext ctx) {
    return new Aggregation(
        visitAggregators(ctx.aggregators()),
        visitGroupByList(ctx.groupByList()),
        child
    );
}
```

## Error Handling Patterns

### Parse Error Recovery
- **Error Listeners**: Custom ANTLR error listeners for better error messages
- **Context Preservation**: Maintain parsing context for error reporting
- **Graceful Degradation**: Continue parsing after recoverable errors

### Semantic Validation
- **AST Validation**: Post-parse validation of AST structure
- **Type Checking**: Expression type validation during planning
- **Reference Resolution**: Field and function reference validation

## Testing Patterns

### Grammar Testing
```java
@Test
public void testStatsCommand() {
    String query = "search source=logs | stats count() by status";
    UnresolvedPlan plan = parser.parse(query);
    
    assertThat(plan, instanceOf(Aggregation.class));
    Aggregation agg = (Aggregation) plan;
    assertThat(agg.getAggregators(), hasSize(1));
    assertThat(agg.getGroupByList(), hasSize(1));
}
```

### AST Builder Testing
```java
@Test
public void testExpressionBuilder() {
    String expression = "count(status) + sum(bytes)";
    UnresolvedExpression expr = expressionBuilder.parse(expression);
    
    assertThat(expr, instanceOf(Add.class));
    Add addExpr = (Add) expr;
    assertThat(addExpr.getLeft(), instanceOf(Function.class));
    assertThat(addExpr.getRight(), instanceOf(Function.class));
}
```

## Performance Considerations

### Parser Optimization
- **Grammar Efficiency**: Left-recursive rules for better performance
- **Token Caching**: Reuse lexer tokens where possible
- **AST Node Pooling**: Object pooling for frequently created nodes

### Memory Management
- **Visitor Pattern**: Stateless visitors to avoid memory leaks
- **Immutable AST**: Immutable AST nodes for thread safety
- **Lazy Evaluation**: Defer expensive operations until needed

## Extension Patterns

### Adding New Commands
1. **Grammar Extension**: Add new rule to parser grammar
2. **AST Node Creation**: Create corresponding AST class
3. **Builder Implementation**: Add visitor method to AstBuilder
4. **Test Coverage**: Add comprehensive test cases

### Function Extension
1. **Function Registration**: Add to `BuiltinFunctionName` enum
2. **Implementation**: Add to `PPLFuncImpTable`
3. **Grammar Support**: Ensure function call syntax is supported
4. **Documentation**: Update function documentation

## New PPL Command Development Checklist

### Prerequisite
- ✅ **Open RFC Issue**: Describe purpose, syntax definition, usage and examples
- ✅ **PM Review Approval**: Obtain approval from PM or repository maintainers

### Coding & Tests
- ✅ **Lexer/Parser Updates**:
  - Add new keywords to `OpenSearchPPLLexer.g4`
  - Add grammar rules to `OpenSearchPPLParser.g4`
  - Update `commandName` and `keywordsCanBeId`
- ✅ **AST Implementation**:
  - Add new tree nodes under `org.opensearch.sql.ast.tree`
  - Prefer reusing `Argument` for command arguments over creating new expression nodes
- ✅ **Visitor Pattern**:
  - Add `visit*` in `AbstractNodeVisitor`
  - Override `visit*` in `Analyzer`, `CalciteRelNodeVisitor` and `PPLQueryDataAnonymizer`
- ✅ **Unit Tests**:
  - Extend `CalcitePPLAbstractTest`
  - Keep test queries minimal
  - Include `verifyLogical()` and `verifyPPLToSparkSQL()`
- ✅ **Integration Tests (pushdown)**:
  - Extend `PPLIntegTestCase`
  - Use complex real-world queries
  - Include `verifySchema()` and `verifyDataRows()`
- ✅ **Integration Tests (Non-pushdown)**:
  - Add test class to `CalciteNoPushdownIT`
- ✅ **Explain Tests**:
  - Add tests to `ExplainIT` or `CalciteExplainIT`
- ✅ **Unsupported in v2 Test**:
  - Add a test in `NewAddedCommandsIT`
- ✅ **Anonymizer Tests**:
  - Add a test in `PPLQueryDataAnonymizerTest`
- ✅ **Cross-cluster Tests** (optional):
  - Add a test in `CrossClusterSearchIT`
- ✅ **User Documentation**:
  - Add `.rst` file under `docs/user/ppl/cmd`
  - Link new doc to `docs/user/ppl/index.rst`
