# Calcite Integration

## Integration Architecture

### Calcite Role in OpenSearch SQL
- **Query Planning**: Converts AST to optimized logical plans
- **Optimization**: Applies rule-based optimizations
- **SQL Generation**: Generates SQL for cross-engine compatibility
- **Type System**: Provides robust type checking and inference

### Key Integration Points
```
AST → CalciteRelNodeVisitor → RelNode → Optimization → Physical Plan
```

## CalciteRelNodeVisitor Patterns

### Visitor Implementation
- **Purpose**: Converts OpenSearch AST nodes to Calcite RelNode tree
- **Pattern**: Visitor pattern for AST traversal
- **Output**: Calcite RelNode representing logical query plan

### Core Visitor Methods
```java
@Override
public RelNode visitRelation(Relation node, CalcitePlanContext context) {
    return LogicalTableScan.create(
        context.getCluster(),
        context.getTable(node.getTableName())
    );
}

@Override
public RelNode visitFilter(Filter node, CalcitePlanContext context) {
    RelNode input = visit(node.getChild(), context);
    RexNode condition = visitExpression(node.getCondition(), context);
    
    return LogicalFilter.create(input, condition);
}
```

## CalciteRexNodeVisitor Patterns

### Expression Conversion
- **Purpose**: Converts OpenSearch expressions to Calcite RexNode
- **Scope**: Handles all expression types (functions, operators, literals)
- **Integration**: Works with CalciteRelNodeVisitor for complete plan conversion

### Expression Handling Examples
```java
@Override
public RexNode visitFunction(Function node, CalcitePlanContext context) {
    SqlOperator operator = context.getOperatorTable()
        .lookupOperatorOverloads(
            SqlIdentifier.of(node.getFunctionName()),
            null,
            SqlSyntax.FUNCTION
        ).get(0);
    
    List<RexNode> operands = node.getArguments().stream()
        .map(arg -> visit(arg, context))
        .collect(Collectors.toList());
    
    return context.getRexBuilder().makeCall(operator, operands);
}
```

## Plan Context Management

### CalcitePlanContext
- **Cluster Management**: Maintains Calcite cluster configuration
- **Schema Information**: Provides table and column metadata
- **Type Factory**: Creates and manages Calcite data types
- **Rex Builder**: Constructs RexNode expressions

### Context Usage Pattern
```java
public class CalcitePlanContext {
    private final RelOptCluster cluster;
    private final RelDataTypeFactory typeFactory;
    private final RexBuilder rexBuilder;
    private final Map<String, RelOptTable> tables;
    
    public RelNode optimize(RelNode logicalPlan) {
        RelOptPlanner planner = cluster.getPlanner();
        planner.setRoot(logicalPlan);
        return planner.findBestExp();
    }
}
```

## Optimization Rules

### Built-in Calcite Rules
- **FilterPushdownRule**: Pushes filters closer to data sources
- **ProjectMergeRule**: Merges adjacent projection operations
- **AggregateReduceRule**: Optimizes aggregation operations
- **JoinReorderRule**: Reorders joins for better performance

### Custom OpenSearch Rules
```java
public class OpenSearchFilterPushdownRule extends RelOptRule {
    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        OpenSearchTableScan scan = call.rel(1);
        
        // Convert filter to OpenSearch query DSL
        QueryBuilder queryBuilder = convertToOpenSearchQuery(filter.getCondition());
        
        // Create new scan with pushed-down filter
        OpenSearchTableScan newScan = scan.withQuery(queryBuilder);
        call.transformTo(newScan);
    }
}
```

## Type System Integration

### OpenSearch to Calcite Type Mapping
```java
public class OpenSearchTypeFactory extends JavaTypeFactoryImpl {
    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        switch (typeName) {
            case VARCHAR:
                return createSqlType(SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED);
            case BIGINT:
                return createSqlType(SqlTypeName.BIGINT);
            case DOUBLE:
                return createSqlType(SqlTypeName.DOUBLE);
            // ... other type mappings
        }
    }
}
```

### Field Type Resolution
- **Dynamic Mapping**: Infers types from OpenSearch index mappings
- **Type Coercion**: Handles implicit type conversions
- **Null Handling**: Manages nullable vs non-nullable types

## Query Execution Flow

### Logical to Physical Conversion
1. **AST → Logical Plan**: CalciteRelNodeVisitor converts AST
2. **Optimization**: Calcite applies optimization rules
3. **Physical Planning**: Convert optimized logical plan to physical operations
4. **Execution**: Execute physical plan against OpenSearch

### Physical Plan Generation
```java
public class OpenSearchPhysicalPlanGenerator {
    public PhysicalPlan generate(RelNode logicalPlan) {
        return logicalPlan.accept(new RelShuttle() {
            @Override
            public RelNode visit(LogicalTableScan scan) {
                return new OpenSearchIndexScan(scan.getTable());
            }
            
            @Override
            public RelNode visit(LogicalFilter filter) {
                return new OpenSearchFilter(
                    visit(filter.getInput()),
                    filter.getCondition()
                );
            }
        });
    }
}
```

## Performance Optimization

### Query Pushdown Strategies
- **Filter Pushdown**: Push WHERE conditions to OpenSearch
- **Projection Pushdown**: Limit fields retrieved from OpenSearch
- **Aggregation Pushdown**: Use OpenSearch aggregations when possible
- **Limit Pushdown**: Apply LIMIT at OpenSearch level

### Calcite Configuration
```java
public class OpenSearchCalciteConfig {
    public static RelOptPlanner createPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();
        
        // Add OpenSearch-specific rules
        planner.addRule(OpenSearchFilterPushdownRule.INSTANCE);
        planner.addRule(OpenSearchProjectPushdownRule.INSTANCE);
        planner.addRule(OpenSearchAggregationPushdownRule.INSTANCE);
        
        // Add standard Calcite rules
        planner.addRule(FilterProjectTransposeRule.INSTANCE);
        planner.addRule(ProjectMergeRule.INSTANCE);
        
        return planner;
    }
}
```

## Error Handling

### Planning Errors
- **Type Mismatch**: Handle incompatible type operations
- **Unsupported Operations**: Graceful fallback for unsupported features
- **Schema Validation**: Validate table and column references

### Runtime Errors
- **Execution Failures**: Handle OpenSearch query execution errors
- **Resource Limits**: Manage memory and timeout constraints
- **Connection Issues**: Handle OpenSearch connectivity problems

## Testing Patterns

### Calcite Integration Tests
```java
@Test
public void testFilterPushdown() {
    String sql = "SELECT * FROM logs WHERE status = 'ERROR'";
    RelNode logicalPlan = parseAndPlan(sql);
    
    // Verify filter is pushed down to table scan
    OpenSearchTableScan scan = findTableScan(logicalPlan);
    assertThat(scan.getQuery(), containsString("status"));
}
```

### Plan Validation Tests
```java
@Test
public void testOptimizationRules() {
    RelNode originalPlan = createTestPlan();
    RelNode optimizedPlan = optimize(originalPlan);
    
    // Verify optimization was applied
    assertThat(optimizedPlan.getRowType(), equalTo(originalPlan.getRowType()));
    assertThat(countNodes(optimizedPlan, LogicalFilter.class), lessThan(
        countNodes(originalPlan, LogicalFilter.class)
    ));
}
```

## Extension Points

### Custom Functions
- **Function Registration**: Register OpenSearch-specific functions with Calcite
- **Implementation**: Provide RexNode generation for custom functions
- **Optimization**: Add function-specific optimization rules

### Custom Operators
- **Operator Definition**: Define new SQL operators for OpenSearch features
- **Code Generation**: Generate efficient execution code
- **Rule Integration**: Add optimization rules for new operators
