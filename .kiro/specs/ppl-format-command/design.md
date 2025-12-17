# PPL Format Command Design

## 1. Approach

### Implementation Strategy

**Type:** Composition of existing PPL operators with custom formatting logic

The `format` command will be implemented as a new PPL command that:
1. Collects all input rows into memory
2. Extracts field names and values from each row
3. Applies formatting rules to construct the search string
4. Outputs a single row with a `search` field containing the formatted result

**Coverage:** This approach covers all v1 requirements:
- Default formatting with standard delimiters
- Custom row/column formatting with user-specified delimiters
- Multivalue field handling with configurable separator
- Empty result handling (deferred to v2 based on out-of-scope)

**Pushdown Feasibility:** None - formatting is a presentation-layer operation that requires all results to be collected and processed in the query engine. The formatting logic cannot be pushed down to OpenSearch.

### SQL Equivalents

**Use Case 1: Basic Field Formatting**
```sql
SELECT 
  CONCAT(
    '( ',
    STRING_AGG(
      CONCAT(
        '( ',
        CONCAT_WS(' AND ',
          CONCAT('host="', host, '"'),
          CONCAT('sourcetype="', sourcetype, '"')
        ),
        ' )'
      ),
      ' OR '
    ),
    ' )'
  ) AS search
FROM (
  SELECT host, sourcetype 
  FROM logs 
  LIMIT 2
) subquery;
```

**Use Case 2: Custom Row and Column Formatting**
```sql
SELECT 
  CONCAT(
    '[ ',
    STRING_AGG(
      CONCAT(
        '[ ',
        CONCAT_WS(' && ',
          CONCAT('host="', host, '"'),
          CONCAT('sourcetype="', sourcetype, '"')
        ),
        ' ]'
      ),
      ' || '
    ),
    ' ]'
  ) AS search
FROM (
  SELECT host, sourcetype 
  FROM logs 
  LIMIT 2
) subquery;
```

**Use Case 3: Multivalue Field Separator**
```sql
SELECT 
  CONCAT(
    '( ',
    CONCAT(
      '( ',
      STRING_AGG(
        CONCAT('tags="', tag_value, '"'),
        ' OR '
      ),
      ' )'
    ),
    ' )'
  ) AS search
FROM (
  SELECT UNNEST(tags) AS tag_value
  FROM logs
) subquery;
```

## 2. Alternative

### Alternative 1: Native Logical Plan Operator

Implement `format` as a dedicated logical plan operator with optimized execution.

**Pros:**
- Better performance through specialized execution
- More control over memory management
- Easier to optimize for large result sets

**Cons:**
- More complex implementation requiring changes across multiple layers
- Duplicates string manipulation logic that exists elsewhere
- Harder to maintain and test

### Alternative 2: Post-Processing Function

Implement as a post-processing function that operates on complete result sets.

**Pros:**
- Simpler integration with existing pipeline
- Clear separation of concerns
- Easier to test in isolation

**Cons:**
- Less flexible for optimization
- May require buffering entire result set
- Harder to integrate with subsearch patterns

### Why Composition Approach (Chosen)

The composition approach balances implementation complexity with functionality:
- Reuses existing PPL command infrastructure
- Provides clear extension point for future enhancements
- Sufficient performance for typical use cases (subsearches are usually small)
- Aligns with how other formatting commands are implemented

## 3. Implementation Discussion

### Behavioral Notes

**Null vs Missing Handling:**
- NULL values: Formatted as empty string `field=""`
- Missing fields: Omitted from output entirely
- Empty strings: Formatted as `field=""`

**Type Coercion:**
- All field values converted to strings before formatting
- Boolean values: `true` → "true", `false` → "false"
- Numeric values: Converted using standard string representation
- Arrays/Objects: Each element formatted separately (multivalue handling)

**Ordering:**
- Field order within a row: Preserved from input schema
- Row order: Preserved from input order
- Multivalue order: Preserved from array order (stable)

**Quoting and Escaping:**
- Field values always wrapped in double quotes
- Double quotes within values escaped as `\"`
- Backslashes escaped as `\\`
- No escaping for delimiters (used literally)

**Memory Considerations:**
- All input rows buffered in memory before formatting
- Suitable for subsearch results (typically < 10,000 rows)
- May need optimization for very large result sets in future

### Open Questions

1. **Empty result handling:** Should we implement `emptystr` parameter in v1 or defer to v2?
   - **Decision:** Defer to v2 as it's primarily a failsafe feature for subsearches
   
2. **maxresults parameter:** Should we implement result limiting in v1?
   - **Decision:** Defer to v2 - users can use `head` command before `format`

3. **Field name quoting:** Should field names be quoted or unquoted in output?
   - **Decision:** Follow Splunk behavior - field names unquoted, values quoted

4. **Internal fields:** Should internal fields (starting with `_`) be included?
   - **Decision:** Exclude internal fields by default, matching Splunk behavior

### Trade-offs

**Memory vs Streaming:**
- **Chosen:** Buffer all results in memory
- **Alternative:** Stream results and format incrementally
- **Rationale:** Simpler implementation, acceptable for typical subsearch sizes

**String Building:**
- **Chosen:** Use StringBuilder for efficient concatenation
- **Alternative:** Use string templates or formatting libraries
- **Rationale:** Direct control over output format, better performance

**Parameter Validation:**
- **Chosen:** Validate all-or-nothing for row/column parameters at parse time
- **Alternative:** Allow partial specification with defaults
- **Rationale:** Matches Splunk behavior, prevents user confusion

## 4. Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     PPL Parser Layer                         │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  OpenSearchPPLParser.g4                                │ │
│  │  - formatCommand rule                                  │ │
│  │  - Parameter parsing (mvsep, row/column delimiters)   │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                      AST Layer                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Format.java (AST Node)                                │ │
│  │  - mvsep: String                                       │ │
│  │  - rowPrefix, colPrefix, colSep, colEnd, rowSep,      │ │
│  │    rowEnd: Optional<String>                            │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  Logical Plan Layer                          │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  LogicalFormat (Logical Plan Node)                     │ │
│  │  - Extends UnaryPlan                                   │ │
│  │  - Contains formatting parameters                      │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   Physical Plan Layer                        │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  FormatOperator (Physical Plan Node)                   │ │
│  │  - Collects input rows                                 │ │
│  │  - Applies formatting logic                            │ │
│  │  - Outputs single row with 'search' field             │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Input:** Stream of rows with multiple fields
2. **Collection:** Buffer all rows in memory
3. **Formatting:** For each row:
   - Extract field-value pairs (excluding internal fields)
   - Format each pair as `field="value"`
   - Join pairs with column separator
   - Wrap with column prefix/suffix
4. **Aggregation:** Join all formatted rows with row separator
5. **Wrapping:** Add row prefix/suffix
6. **Output:** Single row with `search` field

### Data Models

**Format AST Node:**
```java
public class Format extends UnresolvedPlan {
  private final String mvsep;
  private final Optional<String> rowPrefix;
  private final Optional<String> columnPrefix;
  private final Optional<String> columnSeparator;
  private final Optional<String> columnEnd;
  private final Optional<String> rowSeparator;
  private final Optional<String> rowEnd;
  
  // All row/column parameters must be present or all absent
  // Validated at construction time
}
```

**LogicalFormat Node:**
```java
public class LogicalFormat extends UnaryPlan {
  private final String mvsep;
  private final String rowPrefix;
  private final String columnPrefix;
  private final String columnSeparator;
  private final String columnEnd;
  private final String rowSeparator;
  private final String rowEnd;
  
  // Defaults applied during logical plan construction
}
```

**FormatOperator:**
```java
public class FormatOperator extends PhysicalPlan {
  private final List<ExprValue> bufferedRows;
  private final FormattingConfig config;
  
  @Override
  public ExprValue next() {
    // Collect all input rows
    // Apply formatting
    // Return single result
  }
}
```


## 5. Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Search field creation
*For any* input data with one or more rows, applying the format command should produce exactly one output row containing a field named "search"
**Validates: Requirements 1.1**

### Property 2: Default formatting structure
*For any* input data, when format is applied with no parameters, the output should match the structure: "( " + row groups joined by " OR " + " )" where each row group is "( " + field-value pairs joined by " AND " + " )"
**Validates: Requirements 1.2, 1.3, 1.5, 1.6**

### Property 3: Field-value pair format
*For any* field name and value, the formatted output should contain the pattern `fieldname="value"` where the value is properly escaped
**Validates: Requirements 1.4**

### Property 4: Custom delimiter application
*For any* set of six custom delimiters (row prefix, column prefix, column separator, column end, row separator, row end) and any input data, the output should use exactly those delimiters in the correct positions
**Validates: Requirements 2.1, 2.2**

### Property 5: Multivalue field expansion
*For any* field containing multiple values, each value should appear as a separate field-value pair in the output, separated by the mvsep delimiter
**Validates: Requirements 3.1, 3.2**

### Property 6: Default multivalue separator
*For any* input containing multivalue fields, when mvsep is not specified, the multivalue field-value pairs should be separated by " OR "
**Validates: Requirements 3.3**

### Property 7: Format round-trip structure preservation
*For any* input data, the number of row groups in the formatted output should equal the number of input rows, and the number of field-value pairs in each row group should equal the number of non-internal fields in that input row
**Validates: Requirements 1.2, 1.3, 2.2**

## 6. Error Handling

### Invalid Parameter Combinations

**Partial row/column parameters:**
- **Error:** User specifies some but not all six row/column parameters
- **Handling:** Parse-time error with message: "format command requires all six row/column parameters or none"
- **Recovery:** None - user must fix query

**Invalid parameter types:**
- **Error:** User provides non-string value for delimiter parameters
- **Handling:** Parse-time type error
- **Recovery:** None - user must fix query

### Runtime Errors

**Memory exhaustion:**
- **Error:** Input result set too large to buffer in memory
- **Handling:** Throw runtime exception with message indicating memory limit exceeded
- **Recovery:** User should reduce result set size with `head` command

**Empty input:**
- **Error:** No input rows to format
- **Handling:** Return single row with `search` field containing empty string ""
- **Recovery:** Not an error - valid output for empty input

**Internal fields only:**
- **Error:** All fields in input are internal fields (start with `_`)
- **Handling:** Return single row with `search` field containing empty string ""
- **Recovery:** Not an error - valid output when no user fields present

### Edge Cases

**Null and missing values:**
- Null values formatted as `field=""`
- Missing fields omitted from output
- No error thrown

**Special characters in values:**
- Double quotes escaped as `\"`
- Backslashes escaped as `\\`
- Other special characters preserved literally

**Field name conflicts:**
- If input already has a `search` field, it is overwritten
- No warning or error

## 7. Testing Strategy

### Unit Testing

**Parser tests:**
- Valid syntax variations (with/without parameters)
- Invalid syntax (partial parameters, wrong types)
- Edge cases (empty strings, special characters in delimiters)

**Formatting logic tests:**
- Single row, single field
- Multiple rows, multiple fields
- Multivalue fields with various separators
- Null and missing value handling
- Special character escaping

**AST construction tests:**
- Correct AST node creation from parse tree
- Parameter validation
- Default value application

### Property-Based Testing

Property-based tests will use a PBT library appropriate for Java (likely jqwik or QuickTheories) and will run a minimum of 100 iterations per property.

**Property 1: Search field creation**
- Generate: Random input data (1-10 rows, 1-5 fields per row)
- Apply: format command with no parameters
- Verify: Output has exactly one row with a "search" field

**Property 2: Default formatting structure**
- Generate: Random input data
- Apply: format command with no parameters
- Verify: Output matches regex pattern for default structure

**Property 3: Field-value pair format**
- Generate: Random field names and values (including special characters)
- Apply: format command
- Verify: Each field-value pair matches pattern with proper escaping

**Property 4: Custom delimiter application**
- Generate: Random six delimiters and random input data
- Apply: format command with custom delimiters
- Verify: Output contains exactly those delimiters in correct positions

**Property 5: Multivalue field expansion**
- Generate: Random input with multivalue fields
- Apply: format command with custom mvsep
- Verify: Each multivalue element appears as separate pair with mvsep between

**Property 6: Default multivalue separator**
- Generate: Random input with multivalue fields
- Apply: format command without mvsep parameter
- Verify: Multivalue pairs separated by " OR "

**Property 7: Format round-trip structure preservation**
- Generate: Random input data
- Apply: format command
- Verify: Parse output to count row groups and field-value pairs, compare to input

### Integration Testing

**End-to-end tests:**
- Format command in isolation
- Format command in subsearch context
- Format command with various upstream commands (fields, head, eval)
- Format output used as input to subsequent search

**Compatibility tests:**
- Compare output format with Splunk for identical inputs
- Verify subsearch behavior matches Splunk patterns

**Performance tests:**
- Small result sets (< 100 rows)
- Medium result sets (100-1000 rows)
- Large result sets (1000-10000 rows)
- Memory usage profiling
