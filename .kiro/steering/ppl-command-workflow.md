---
inclusion: manual
---

# PPL Command Workflow

When creating or updating PPL command specs (requirements.md, design.md, tasks.md), follow the RFC template structure with PPL-specific adaptations.

## requirements.md Structure

Follow the RFC template from `.github/ISSUE_TEMPLATE/request_of_comments.md` with these PPL-specific adaptations:

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

Each use case MUST have:
- **User Story**: As a [role], I want to [action] so that [benefit]
- **Acceptance Criteria**: WHEN/THEN format
- **PPL Query**: Actual executable PPL query
- **Input Data**: Sample data (≥2 rows, ≥2 columns)
- **Expected Output**: Exact table/JSON showing results

**Example:**
````markdown
## Proposal

### Use Case 1: Basic Multi-Value Combination

**User Story:** As a data analyst, I want to combine multi-valued fields into a single delimited string.

**Acceptance Criteria:**
1. WHEN a user specifies a multi-valued field, THEN combine all values into a single string
2. WHEN a delimiter is specified, THEN use that delimiter to separate values
3. WHEN no delimiter is specified, THEN use comma as default delimiter

**Query Example:**
```ppl
source=employees | mvcombine field=skills delim=", "
```

**Input Data:**
```
| id | name  | skills              |
|----|-------|---------------------|
| 1  | Alice | ["Java", "Python"]  |
| 2  | Bob   | ["SQL", "NoSQL"]    |
```

**Expected Output:**
```
| id | name  | combined_skills    |
|----|-------|--------------------|
| 1  | Alice | Java, Python       |
| 2  | Bob   | SQL, NoSQL         |
```

### Use Case 2: [Next use case]
[Repeat format]
````

### 5. Approach
High-level implementation strategy - this maps to design.md content:

**Include:**
- **Subset or Composition?**
  - Is this a subset of existing command, composition of operators, or new command?
  - Coverage proof showing this covers v1 scope
  
- **Equivalent SQL**: Show SQL equivalent for each requirement
  
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
SELECT id, name, ARRAY_JOIN(skills, ', ') as combined_skills
FROM employees
```
````

### 6. Alternative
Alternative solutions or workarounds that can partially/temporarily solve the problem.

### 7. Implementation Discussion
Discussion points regarding the proposed implementation:

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

## Appendix: Baseline Study (MANDATORY)

**CRITICAL: This section is MANDATORY and must be completed BEFORE writing requirements.**

Before writing any requirements, you MUST study Splunk's implementation for feature parity:

1. **Read Splunk Documentation (USE THE TOOL):**
   - URL: https://docs.splunk.com/Documentation/SplunkCloud/latest/SearchReference/[command]
   - Alternative: https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/[command]
   - **CRITICAL: Actually fetch and read the Splunk documentation**
   - **DO NOT** proceed based on assumptions from the command name alone
   - Read the entire page front-to-back carefully – not skim

2. **Capture from Splunk:**
   - Syntax forms, parameters, defaults, notable constraints
   - Behavioral semantics: null/missing handling, type coercion, ordering, determinism
   - 1-2 canonical examples from Splunk docs with their explanations

**Example Baseline Study:**
```markdown
## Appendix: Baseline Study

### Splunk Reference
- **URL:** https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Mvcombine

### Core Semantic (CRITICAL)
**Exact quote from Splunk Documentation:**
"Takes a group of events that are identical except for the specified field, which contains a single value, and combines those events into a single event. The specified field becomes a multivalue field."

**What this means:**
- Multiple rows with identical values in ALL fields EXCEPT the target field
- Target field has different single values across these rows
- Rows are collapsed into ONE row
- Target field becomes multi-valued/delimited string

### Splunk Syntax
`mvcombine [delim=<string>] [sdelim=<string>] <field>`

### Key Behaviors
- Groups events by all fields except target field
- Default delimiter: space (not comma)
- Supports sdelim for source delimiter
- Reduces number of output events

### Potential Drift from Splunk
- Our default delimiter: comma (different from Splunk's space)
- We don't support sdelim in v1
- NULL vs MISSING semantics may differ
```

## design.md (Usually Not Needed)

**IMPORTANT:** Design decisions are already covered in requirements.md:
- **Approach section**: Subset/Composition, SQL equivalents, Pushdown feasibility
- **Implementation Discussion section**: Behavioral notes, trade-offs

**Only create design.md if the feature requires:**
- Complex architecture diagrams
- Detailed algorithm pseudocode
- Performance analysis with benchmarks
- Security threat modeling

Otherwise, skip design.md and proceed directly to tasks.md.

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
