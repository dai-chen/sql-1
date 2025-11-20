# PPL Command Requirements: mvcombine

**Status:** APPROVED  
**Created:** 2025-01-19  
**Phase:** 1 - Requirements  
**Last Updated:** 2025-01-19

---

## Baseline Summary (Splunk Reference)

### What Splunk Supports

The `mvcombine` command in Splunk is a transforming command that:

1. **Core Functionality:**
   - Takes a group of events that are identical except for one specified field
   - Combines those events into a single event
   - Converts the specified field into a multivalue field containing all individual values
   - Does not apply to internal fields

2. **Syntax:**
   ```
   mvcombine [delim=<string>] <field>
   ```

3. **Parameters:**
   - `field` (required): Name of the field to merge on
   - `delim` (optional): Delimiter string for combined values (default: single space " ")

4. **Behavioral Characteristics:**
   - Creates both multivalue representation (default) and delimited single-value representation
   - Single-value version visible when using `nomv` command or CSV export
   - Multivalue version shown in UI and JSON exports
   - Groups results where ALL other fields are identical
   - Most effective after field reduction with `fields` command
   - Commonly used with `stats` and `timechart` output

5. **Canonical Examples:**
   ```
   # Basic combination
   ... | mvcombine host
   
   # With custom delimiter (requires nomv to see delimiter)
   ... | mvcombine delim="," host | nomv host
   ```

### Potential Drift from OpenSearch PPL

1. **NULL vs MISSING Semantics:**
   - Splunk: Single space as default delimiter
   - OpenSearch: Need to define behavior for NULL fields in grouping logic

2. **Type Coercion:**
   - Splunk: Implicit string coercion for delimiter application
   - OpenSearch: Must define type handling rules (numeric, string, boolean fields)

3. **Field Handling:**
   - Splunk: Excludes internal fields automatically
   - OpenSearch: Need to define which fields are excluded (e.g., _id, _index, etc.)

4. **Display Representation:**
   - Splunk: Dual representation (MV + delimited single-value)
   - OpenSearch: JSON-based, likely single representation approach

### Initial v1 Scope Proposal

**IN SCOPE for v1:**
- Combine events with identical field values (excluding specified field) into single result
- Convert specified field to multivalue array OR delimited string (based on `delim` parameter)
- Support `delim` parameter with string delimiter
- Support for string, numeric, and boolean field types
- **Automatic internal/system field filtering** (only regular fields considered for grouping)

**OUT OF SCOPE for v1:**
- `nomv` command implementation (separate command)
- Complex type coercion beyond basic string conversion
- Nested field support
- Performance optimizations for large datasets

---

## Overview

The `mvcombine` command consolidates multiple events that share identical field values (except for one designated field) into a single event. The designated field becomes a multivalue field containing all the individual values from the combined events.

**Primary Use Cases:**
1. Consolidating results from aggregation commands like `stats`
2. Reducing result set size by merging similar events
3. Creating multivalue fields for downstream processing

---

## Supported Syntax (v1 Scope)

### Grammar Sketch (ANTLR-style)

```
mvcombineCommand
    : MVCOMBINE (DELIM EQUALS delimString)? fieldExpression
    ;

delimString
    : stringLiteral
    ;

fieldExpression
    : fieldName
    ;

MVCOMBINE : 'mvcombine' ;
DELIM : 'delim' ;
EQUALS : '=' ;
```

### Parameter Table

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `field` | field name | Yes | - | Name of the field to combine into multivalue field |
| `delim` | string | No | `" "` (single space) | Delimiter string to join values |

### Constraints

- Field name must be a valid identifier (non-internal field)
- Delimiter can be any string (including empty string)
- Command groups on ALL other fields being equal

---

## Use Cases

### Use Case 1: Consolidating Host Values After Stats

**Scenario:** After running statistics that produce duplicate rows differing only by host, combine the host values.

**Input Data:**
```
| max | min | host  |
|-----|-----|-------|
| 500 | 100 | web1  |
| 500 | 100 | web2  |
| 500 | 100 | web3  |
```

**PPL Query:**
```sql
source=logs | stats max(bytes) AS max, min(bytes) AS min BY host 
| mvcombine host
```

**Expected Output:**
```
| max | min | host             |
|-----|-----|------------------|
| 500 | 100 | ["web1", "web2", "web3"] |
```

**Note:** Output shows multivalue field as JSON array representation.

---

### Use Case 2: Combining with Custom Delimiter

**Scenario:** Combine server names with a comma delimiter for readable display.

**Input Data:**
```
| status | server   |
|--------|----------|
| active | server1  |
| active | server2  |
| active | server3  |
```

**PPL Query:**
```sql
source=servers status="active" | fields status, server 
| mvcombine delim="," server
```

**Expected Output:**
```
| status | server                |
|--------|----------------------|
| active | "server1,server2,server3" |
```

**Behavioral Note:** When `delim` parameter is specified, the output is a single delimited string, not a multivalue array.

---

### Use Case 3: Combining Numeric Values

**Scenario:** Combine error codes that occur for the same user, preserving numeric types.

**Input Data:**
```
| user  | error_code |
|-------|------------|
| alice | 404        |
| alice | 500        |
| alice | 503        |
```

**PPL Query:**
```sql
source=errors | fields user, error_code 
| mvcombine error_code
```

**Expected Output:**
```
| user  | error_code        |
|-------|-------------------|
| alice | [404, 500, 503]   |
```

**Note:** Without `delim` parameter, numeric values are preserved as numbers in the multivalue array, not converted to strings.

---

## Behavioral Notes / Gotchas

### Grouping Logic
- Events are grouped when **ALL** fields except the specified field have identical values
- Comparison is exact match (case-sensitive for strings)
- Fields not present in all events may cause unexpected grouping behavior

### NULL and Missing Value Handling
- NULL values in the combining field are included in the multivalue result as NULL
- Missing fields are treated as distinct from NULL
- If grouping fields contain NULL, those are treated as a distinct value for grouping

### Type Coercion
- **Without `delim` parameter:** Values preserved in their original types as multivalue array
  - String fields: preserved as strings
  - Numeric fields: preserved as numbers
  - Boolean fields: preserved as boolean values
  - Mixed types in same field: converted to string representation
- **With `delim` parameter:** All values converted to strings and joined with delimiter into single string value

### Ordering
- Order of values in the resulting multivalue field is **unspecified**
- Do not rely on any particular ordering of values
- If order matters, sort before using `mvcombine`

### Delimiter Parameter
- When **NOT specified:** Output is multivalue array with original value types preserved
- When **specified:** Output is single delimited string with all values converted to strings and joined
- Empty string delimiter (`delim=""`) is valid and results in concatenated string with no separator

### Limits
- No inherent limit on number of values in multivalue field (subject to system memory constraints)
- Large multivalue fields may impact performance

---

## Out-of-Scope (v1)

The following features are explicitly **not** included in v1:

1. **`nomv` Command:**
   - Separate command for converting multivalue to single-value
   - Will be addressed in separate RFC

2. **Nested Field Support:**
   - Cannot combine values from nested JSON objects
   - Only top-level fields supported

3. **Advanced Type Handling:**
   - No automatic type promotion for mixed-type fields
   - Complex objects (arrays, nested objects) not supported as combining field

4. **Performance Optimizations:**
   - No special optimizations for very large result sets
   - No distributed processing optimizations

5. **Field Wildcards:**
   - Cannot specify patterns like `host*` to combine multiple fields
   - Must specify exact field name

---

## Resolved Decisions

1. **Output Format:**
   - **Without `delim` parameter:** Result is array of original value types
   - **With `delim` parameter:** Result is single delimited string
   - **Decision:** ✅ Approved

2. **Single Value Case:**
   - If only one event matches grouping criteria, field is still converted to multivalue array with single element
   - **Decision:** ✅ Approved

3. **Empty Result Handling:**
   - When no events match grouping criteria, preserve original values as-is (no combination occurs)
   - **Decision:** ✅ Approved

4. **Field Name Conflicts:**
   - Follow existing PPL command conventions for handling reserved words
   - **Decision:** ✅ Approved

---

## Approval Record

**Date:** 2025-01-19  
**Approver:** Maintainer (OpenSearch SQL Team)  
**Approval Summary:**
- v1 scope confirmed with automatic internal/system field filtering
- Output format clarified: array without `delim`, string with `delim`
- Use Case 3 updated to demonstrate type preservation (numeric array)
- All open questions resolved and documented in "Resolved Decisions" section
- Requirements complete and approved for Phase 2 (Solution Design)

**Approval Token:** Requirements: LOCKED ✅
