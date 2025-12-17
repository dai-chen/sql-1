# PPL Format Command Requirements

## 1. Problem Statement

Splunk users migrating to OpenSearch need the `format` command to transform search results into formatted search strings. The `format` command is essential for subsearch operations and exporting results to external systems. Without this command, users cannot easily construct dynamic search filters from result sets or format output for integration with other tools.

## 2. Current State

Currently, OpenSearch PPL lacks the `format` command. Users who need to:
- Create dynamic search filters from subsearch results must manually construct search strings
- Export formatted results to external systems must use custom post-processing
- Migrate Splunk queries that rely on `format` must rewrite their queries entirely

This creates a significant barrier for Splunk-to-OpenSearch migration and limits PPL's utility for advanced search patterns.

## 3. Long-Term Goals

### Primary Objectives
- Enable Splunk users to migrate queries using `format` without syntax changes
- Support subsearch result formatting for dynamic search construction
- Provide flexible output formatting for external system integration
- Maintain performance parity with native OpenSearch operations

### Out-of-Scope for v1
- Custom format templates beyond row/column parameters - deferred to v2
- Binary or non-text output formats - requires separate serialization framework
- Streaming format updates - needs incremental processing support
- Format validation against target system schemas - requires external schema registry

## 4. Proposal

### Use Case 1: Basic Field Formatting with Default Parameters

**User Story:** As a security analyst, I want to format search results into a search string using default delimiters, so that I can use the output as a filter in subsequent searches.

**Acceptance Criteria (EARS only):**
1. WHEN a user applies format with no parameters, THEN the system SHALL create a search field containing formatted results
2. WHEN formatting multiple rows, THEN the system SHALL use "OR" as the default row separator
3. WHEN formatting multiple columns, THEN the system SHALL use "AND" as the default column separator
4. WHEN formatting field-value pairs, THEN the system SHALL use the pattern `field="value"`
5. WHEN wrapping rows, THEN the system SHALL use "(" as row prefix and ")" as row suffix
6. WHEN wrapping columns, THEN the system SHALL use "(" as column prefix and ")" as column suffix

**PPL Query:**
```ppl
source=logs | fields host, sourcetype | head 2 | format
```

**Input Data:**
```
| host          | sourcetype |
|---------------|------------|
| my_laptop     | syslog     |
| bobs_laptop   | syslog     |
```

**Expected Output:**
```
| search                                                                                    |
|-------------------------------------------------------------------------------------------|
| ( ( host="my_laptop" AND sourcetype="syslog" ) OR ( host="bobs_laptop" AND sourcetype="syslog" ) ) |
```

### Use Case 2: Custom Row and Column Formatting

**User Story:** As a data engineer, I want to specify custom row and column delimiters, so that I can format output for external systems with different syntax requirements.

**Acceptance Criteria (EARS only):**
1. WHEN a user specifies row prefix, column prefix, column separator, column end, row separator, and row end, THEN the system SHALL use those values for formatting
2. WHEN all six formatting parameters are provided, THEN the system SHALL apply them in the correct order
3. WHEN custom delimiters contain special characters, THEN the system SHALL preserve them literally
4. WHEN custom delimiters are empty strings, THEN the system SHALL omit those delimiters from output

**PPL Query:**
```ppl
source=logs | fields host, sourcetype | head 2 | format "[" "[" "&&" "]" "||" "]"
```

**Input Data:**
```
| host          | sourcetype |
|---------------|------------|
| my_laptop     | syslog     |
| bobs_laptop   | syslog     |
```

**Expected Output:**
```
| search                                                                                    |
|-------------------------------------------------------------------------------------------|
| [ [ host="my_laptop" && sourcetype="syslog" ] || [ host="bobs_laptop" && sourcetype="syslog" ] ] |
```

### Use Case 3: Multivalue Field Separator

**User Story:** As a log analyst, I want to specify how multivalue fields are formatted, so that I can control how array values are represented in the output.

**Acceptance Criteria (EARS only):**
1. WHEN a user specifies mvsep parameter, THEN the system SHALL use that separator for multivalue field values
2. WHEN a field contains multiple values, THEN the system SHALL format each value as a separate field-value pair
3. WHEN mvsep is not specified, THEN the system SHALL use "OR" as the default multivalue separator
4. WHEN a multivalue field is empty, THEN the system SHALL omit that field from the output

**PPL Query:**
```ppl
source=logs | eval tags=["error", "critical"] | fields tags | format mvsep="OR"
```

**Input Data:**
```
| tags                |
|---------------------|
| ["error", "critical"] |
```

**Expected Output:**
```
| search                                      |
|---------------------------------------------|
| ( ( tags="error" OR tags="critical" ) )     |
```

## Appendix: Baseline Study

### Splunk Documentation Summary

**Source:** https://docs.splunk.com/Documentation/SplunkCloud/latest/SearchReference/Format

**Command Purpose:**
The `format` command is used implicitly by subsearches to transform result sets into formatted search strings. It takes multiple rows and columns and creates a single formatted string in a new field called `search`.

**Syntax:**
```
format [mvsep="<mv separator>"] [maxresults=<int>] ["<row prefix>" "<column prefix>" "<column separator>" "<column end>" "<row separator>" "<row end>"] [emptystr="<string>"]
```

**Parameters:**
- `mvsep`: Separator for multivalue fields (default: "OR")
- `maxresults`: Maximum results to return (default: 0 = unlimited)
- Row/column formatting (must specify all 6 if any):
  - `<row prefix>`: Default "("
  - `<column prefix>`: Default "("
  - `<column separator>`: Default "AND"
  - `<column end>`: Default ")"
  - `<row separator>`: Default "OR"
  - `<row end>`: Default ")"
- `emptystr`: String to output when results are empty (default: "NOT( )")

**Behavioral Semantics:**
1. **Implicit usage**: Automatically applied at end of subsearches
2. **Field-value formatting**: Creates `field="value"` pairs for each column
3. **Row grouping**: Each input row becomes a group of field-value pairs
4. **Multivalue handling**: Each value in multivalue field becomes separate field-value pair
5. **Empty results**: Returns `emptystr` value when no results (default "NOT( )")
6. **All-or-nothing formatting**: Must specify all 6 row/column parameters or none

**Canonical Examples from Splunk:**

**Example 1 - Default formatting:**
```
Input:
| source          | sourcetype | host          |
|-----------------|------------|---------------|
| syslog.log      | syslog     | my_laptop     |
| bob-syslog.log  | syslog     | bobs_laptop   |

Query: ... | head 2 | fields source, sourcetype, host | format

Output:
| search |
|--------|
| ( ( host="my_laptop" AND source="syslog.log" AND sourcetype="syslog" ) OR ( host="bobs_laptop" AND source="bob-syslog.log" AND sourcetype="syslog" ) ) |
```

**Example 2 - Custom formatting:**
```
Query: ... | format "[" "[" "&&" "]" "||" "]"

Output:
| search |
|--------|
| [ [ host="my_laptop" && source="syslog.log" && sourcetype="syslog" ] || [ host="bobs_laptop" && source="bob-syslog.log" && sourcetype="syslog" ] ] |
```

**Notable Constraints:**
- Row/column parameters are positional and must all be specified together
- Empty results behavior can be customized with `emptystr` for failsafe patterns
- Commonly used in subsearches to create dynamic search filters
- Output always goes to a field named `search`
