---
inclusion: manual
---

# PPL Command Workflow

When creating or updating PPL command specs (requirements.md, design.md, tasks.md), follow the RFC template structure with PPL-specific adaptations.

## requirements.md Structure (MUST MATCH THIS EXACTLY)

````markdown
## 1. Problem Statement
[TBD]

## 2. Current State
[TBD]

## 3. Long-Term Goals
### Primary Objectives
[TBD]
### Out-of-Scope for v1
[TBD]

## 4. Proposal
### Use Case 1: [Title]
**User Story:** [TBD]

**Acceptance Criteria (EARS only):**
- WHEN ..., THE SYSTEM SHALL ...
- WHEN ..., THE SYSTEM SHALL ...

**PPL Query:**
```ppl
[TBD]

**Input Data:**
[TBD]

**Expected Output:**
[TBD]

### Use Case 2: [Title]
[Same structure as Use Case 1]

### Use Case 3: [Title]
[Same structure as Use Case 1]

## Appendix: Baseline Study
[TBD]
````

**IMPORTANT:** Generate at most 3 use cases in the Proposal section. Focus on the most important and representative scenarios.

**STEP 0: Baseline Study (MANDATORY - DO THIS FIRST)**

Before writing requirements.md, study Splunk's implementation for feature parity:

1. **Read Splunk Documentation:**
   - Fetch public document from https://docs.splunk.com/Documentation/SplunkCloud/latest/SearchReference/[command]
   - Alternative URL: https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/[command]
   - **CRITICAL: Actually fetch and read the Splunk documentation**
   - **DO NOT** proceed based on assumptions from the command name alone
   - Read the entire page front-to-back carefully – not skim

2. **Capture from Splunk:**
   - Syntax forms, parameters, defaults, notable constraints
   - Behavioral semantics: null/missing handling, type coercion, ordering, determinism
   - 1-2 canonical examples from Splunk docs with their explanations

After completing the baseline study, follow the RFC template from `.github/ISSUE_TEMPLATE/request_of_comments.md` with these PPL-specific adaptations:

### 1. Problem Statement
What issue/challenge needs to be addressed by this PPL command.

### 2. Current State
- Existing setup and its shortcomings
- What users currently have to do without this command
- Pain points in current workflows

### 3. Long-Term Goals
- Ideal outcome and primary objectives
- Sustainability/scalability considerations
- **Out-of-Scope for v1**: Explicitly list what is NOT included in v1 but may be considered in future versions

**Example:**
```markdown
## Long-Term Goals

### Primary Objectives
- Enable Splunk users to migrate queries without syntax changes
- Support all common use cases for field combination
- Maintain performance parity with native OpenSearch operations

### Out-of-Scope for v1
- `sdelim` parameter (source delimiter) - deferred to v2
- Nested field combination - requires broader nested field support
- Custom aggregation functions - needs UDF framework
```

### 4. Proposal
Suggested solution with **Concrete PPL Query Examples** (MANDATORY).

**IMPORTANT:** Generate at most 3 use cases. Focus on the most important and representative scenarios.

Each use case MUST have:
- **User Story**: As a [role], I want to [action] so that [benefit]
- **Acceptance Criteria**: WHEN/THEN format
- **PPL Query**: Actual executable PPL query
- **Input Data**: Sample data (≥2 rows, ≥2 columns)
- **Expected Output**: Exact table/JSON showing results

**Example:**
````markdown
## Proposal

### Use Case 1: Basic Field Combination

**User Story:** As a data analyst, I want to combine multiple rows with the same grouping fields into a single row with a delimited string field, so that I can see all values together.

**Acceptance Criteria:**
1. WHEN a user specifies a target field with mvcombine, THEN the system SHALL group rows by all other fields
2. WHEN rows are grouped, THEN the system SHALL combine target field values into a single delimited string
3. WHEN no delimiter is specified, THEN the system SHALL use comma "," as the default delimiter
4. WHEN a delimiter is specified, THEN the system SHALL use that delimiter to separate values

**PPL Query:**
```ppl
source=employees | mvcombine field=department
```

**Input Data:**
```
| id | name  | department |
|----|-------|------------|
| 1  | Alice | Engineering|
| 1  | Alice | Sales      |
| 1  | Alice | Marketing  |
| 2  | Bob   | Engineering|
```

**Expected Output:**
```
| id | name  | department                    |
|----|-------|-------------------------------|
| 1  | Alice | Engineering,Sales,Marketing   |
| 2  | Bob   | Engineering                   |
```

### Use Case 2: [Next use case]
[Repeat format]
````

### 5. Appendix: Baseline Study
Include the baseline study from STEP 0 as an appendix.

## design.md Structure (MUST MATCH THIS EXACTLY)

````markdown
## 1. Approach
### Implementation Strategy
[TBD]

### SQL Equivalents
**Use Case 1:**
```sql
[TBD]
```

**Use Case 2:**
```sql
[TBD]
```

**Use Case 3:**
```sql
[TBD]
```

## 2. Alternative
### Alternative 1: [Title]
[TBD]

### Alternative 2: [Title]
[TBD]

### Why [Chosen Approach]
[TBD]

## 3. Implementation Discussion
### Behavioral Notes
[TBD]

### Open Questions
[TBD]

### Trade-offs
[TBD]

## 4. Optional: Advanced Details
[TBD - Only if needed for complex features]
````

Create design.md to document technical implementation decisions.

### 1. Approach (MANDATORY)
High-level implementation strategy:

**Include:**
- **Subset or Composition?**
  - Is this a subset of existing command, composition of operators, or new command?
  - Coverage proof showing this covers v1 scope
  
- **Equivalent SQL**: Show SQL equivalent for each use case from requirements
  
- **Pushdown Feasibility**: Full/Partial/None with reasoning

**Example:**
````markdown
## Approach

### Implementation Strategy
- **Type**: Composition of existing PPL operators (stats + eval)
- **Coverage**: Covers all v1 requirements through GROUP BY + string aggregation
- **Pushdown**: Partial - grouping pushes to OpenSearch, string aggregation in Calcite

### SQL Equivalents
**Use Case 1:**
```sql
SELECT id, name, ARRAY_JOIN(department, ',') as department
FROM employees
GROUP BY id, name
```
````

### 2. Alternative (MANDATORY)
Alternative implementation approaches or workarounds:

**Include:**
- Alternative technical solutions considered
- Workarounds that can partially/temporarily solve the problem
- Why the chosen approach (from section 1) is preferred

**Example:**
```markdown
## Alternative

### Alternative 1: Native Command Implementation
Implement mvcombine as a standalone native command with dedicated logic.
- **Pros**: Better performance, more control over optimization
- **Cons**: More code to maintain, duplicates existing functionality

### Alternative 2: User-Level Workaround
Users can manually use stats with list() aggregation.
- **Pros**: No new code needed
- **Cons**: Verbose syntax, not Splunk-compatible, requires user knowledge

### Why Composition Approach (Chosen)
- Reuses battle-tested operators
- Simpler implementation and maintenance
- Sufficient performance for v1 use cases
```

### 3. Implementation Discussion
Technical discussion points:

**Include:**
- **Behavioral Notes** (MANDATORY):
  - Null vs missing semantics
  - Type coercion rules
  - Ordering/stability guarantees
  - Time/locale assumptions (if applicable)
- Open questions or concerns about the approach
- Trade-offs between different implementation options
- Compatibility considerations with existing features

**Example:**
```markdown
## Implementation Discussion

### Behavioral Notes
- **Null handling**: NULL values are skipped in combination
- **Missing fields**: Treated as empty array, result is empty string
- **Type coercion**: Non-string values converted to string before combination
- **Ordering**: Preserves array element order (stable)

### Open Questions
- Should we match Splunk's default delimiter (space) or use comma for consistency with other commands?
- How to handle extremely large arrays (memory concerns)?

### Trade-offs
- **Option A (Composition)**: Reuse existing operators, simpler but less optimized
- **Option B (Native command)**: Better performance but more code to maintain
```

### 4. Optional: Advanced Details
For complex features, include:
- Architecture diagrams
- Detailed algorithm pseudocode
- Performance analysis with benchmarks
- Security threat modeling

## tasks.md Structure

**NOTE:** tasks.md focuses on actionable implementation breakdown based on the design decisions in requirements.md.

### 1. Evidence from Recent PRs (MANDATORY)
Study 2-4 recent merged PRs that added similar PPL commands to understand the implementation pattern.

**Example:**
```markdown
## Evidence from Recent PRs

- **PR #4754**: Added `fillnull` command
  - Files: OpenSearchPPLParser.g4, FillNull.java, CalciteRelNodeVisitor.java
  - Pattern: New AST node + visitor implementation
  
- **PR #4123**: Added `mvcombine` command  
  - Files: Similar pattern with additional string handling
  - Lessons: Need careful null handling in string operations
```

### 2. Planned Change List (MANDATORY - Grouped by Category)
Based on studying recent PRs, list all files that need to be created or modified, grouped by category:

**Example:**
```markdown
## Planned Change List

### Grammar/Parser
- `ppl/src/main/antlr/OpenSearchPPLParser.g4` - Add mvcombine syntax rule

### AST Nodes
- `core/src/main/java/org/opensearch/sql/ast/tree/Mvcombine.java` - New AST node

### Visitors
- `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java` - Build AST from parse tree
- `opensearch/src/main/java/org/opensearch/sql/opensearch/visitor/CalciteRelNodeVisitor.java` - Convert to Calcite RelNode
- `core/src/main/java/org/opensearch/sql/ast/tree/Anonymizer.java` - Add anonymization support

### Tests
- Unit: `MvcombineTest.java`
- Syntax: `MvcombineSyntaxTest.java`  
- Integration: `MvcombineIT.java` (pushdown/non-pushdown/explain/v2-compat/anonymizer)

### Documentation
- `docs/user/ppl/cmd/mvcombine.rst`
- Update `docs/user/ppl/category.json`
```

### 3. Task Breakdown
Break down the Planned Change List into concrete, testable tasks with clear acceptance criteria.

**Example:**
```markdown
## Task Breakdown

### Task 1: Grammar Definition
**Files:** `OpenSearchPPLParser.g4`
**Acceptance Criteria:**
- [ ] Add mvcombine rule to parser grammar
- [ ] Support syntax: `mvcombine [delim=<string>] <field>`
- [ ] Grammar compiles without errors

### Task 2: AST Node Implementation
**Files:** `Mvcombine.java`
**Acceptance Criteria:**
- [ ] Create Mvcombine AST node extending UnresolvedPlan
- [ ] Include field name and delimiter parameters
- [ ] Implement equals/hashCode/toString

### Task 3: AST Builder
**Files:** `AstBuilder.java`
**Acceptance Criteria:**
- [ ] Add visitMvcombine method
- [ ] Parse field and delimiter from parse tree
- [ ] Return Mvcombine AST node

[Continue for all tasks...]
```

### 4. Optional: Technical Details
For complex implementations, include:
- Algorithm pseudocode
- Data structure choices
- Performance considerations
- Edge case handling
