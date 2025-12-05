---
inclusion: manual
---

# PPL Spec Format Requirements

When creating or updating PPL command specs (requirements.md, design.md, tasks.md), follow these CRITICAL format requirements.

## BEFORE Creating requirements.md: Splunk Baseline Study (MANDATORY)

Before writing any requirements, you MUST study Splunk's implementation for feature parity:

1. **Read Splunk Documentation:**
   - URL: https://docs.splunk.com/Documentation/SplunkCloud/latest/SearchReference/[command]
   - Alternative: https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/[command]
   - **Read the entire page front-to-back carefully** – not skim:
   - Pay particular attention to:
     - Behavior / semantics descriptions.
     - All examples and their explanations.
     - Notes / warnings / edge cases and limitations.

2. **Capture from Splunk:**
   - Syntax forms, parameters, defaults, notable constraints
   - Behavioral semantics: null/missing handling, type coercion, ordering, determinism
   - 1-2 canonical examples from Splunk docs

3. **Document Baseline:**
   - Create a "Baseline Study" section in requirements.md
   - Note what Splunk supports
   - Identify potential drift vs our PPL (NULL vs MISSING, coercion, timezone/locale)
   - Propose initial v1 scope: what we support NOW vs OUT-OF-SCOPE

**Example Baseline Study Section:**
```markdown
## Baseline Study

### Splunk Reference
- **URL:** https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/mvcombine
- **Splunk Syntax:** `mvcombine [delim=<string>] [sdelim=<string>] <field>`
- **Key Behaviors:**
  - Default delimiter: space (not comma)
  - Supports sdelim for source delimiter
  - Handles multi-valued fields from previous commands
- **Potential Drift:**
  - Our default delimiter: comma (different from Splunk's space)
  - We don't support sdelim in v1
  - NULL vs MISSING semantics may differ
```

## requirements.md MUST Include

### 1. Baseline Study Section (MANDATORY - comes first)
See "BEFORE Creating requirements.md" section above. This MUST be the first section after the header.

### 2. Concrete PPL Query Examples (MANDATORY)
Each use case MUST have:
- **PPL Query**: Actual executable PPL query using the command
- **Input Data**: Sample data showing what's being queried
- **Expected Output**: Exact table/JSON showing expected results (≥2 rows, ≥2 columns)

**Example Format:**
````markdown
### Requirement 1: Basic Multi-Value Combination

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
````

### 3. Behavioral Notes Section
MUST explicitly cover:
- Null vs missing semantics
- Type coercion rules
- Ordering/stability guarantees
- Time/locale assumptions (if applicable)

### 4. Out-of-Scope Section
Explicitly list what is NOT included in v1.

## design.md MUST Include

### 1. High-Level Decisions Section (MANDATORY)

This section MUST answer these 4 questions explicitly:

````markdown
## High-Level Decisions

### 1. Subset or Composition?
- [ ] **Subset** of existing command: [which command]
- [ ] **Composition** of existing PPL operators: [which operators]
- [ ] **New command** (neither subset nor composition)

**Coverage proof:** [1-2 line explanation showing this covers v1 scope and all requirements]

### 2. Equivalent SQL for Each Requirement

**Requirement 1:**
```sql
SELECT id, name, ARRAY_JOIN(skills, ', ') as combined_skills
FROM employees
```

**Requirement 2:**
```sql
[SQL for requirement 2]
```

**Requirement 3:**
```sql
[SQL for requirement 3]
```

Or: **N/A** (if subset/composition fully covers)

### 3. Pushdown Feasibility
- [ ] **Full** - entire operation can push to OpenSearch DSL
- [ ] **Partial** - some operations push, boundary: [describe]
- [ ] **None** - must execute in Calcite

**Reasoning:** [Why this pushdown decision]

### 4. UDF/UDAF Required?
- [ ] **None**
- [ ] Required functions:
  - `functionName(arg1: type, arg2: type): returnType` - [purpose]
````

### 2. Evidence from Recent PRs
MUST list 2-4 recent merged PRs that added similar PPL commands, with PR numbers and files changed.

### 3. Planned Change List (Grouped)
You should be able to figure this out by studying recent similar PRs. MUST group files by category:
- Grammar/Parser (*.g4 files)
- AST nodes (core/.../ast/tree/*)
- Visitors (AstBuilder, CalciteRelNodeVisitor, Anonymizer, others)
- Tests (unit, syntax, integration: pushdown/non-pushdown/explain/v2-compat/anonymizer)
- Docs (*.rst files, category.json)

## tasks.md MUST Include

### 1. Task Checklist
Ordered tasks matching the design.md plan.

### 2. Final Reconciliation (MUST PASS)

This is a CRITICAL gate before claiming completion. You MUST verify:

**Documentation Examples Check:**
- [ ] Examples in `docs/user/ppl/cmd/<command>.rst` can use different test dataset
- [ ] Examples MUST cover all the same scenarios as the requirements in requirements.md
- [ ] Examples MUST meet the requirements from each requirement in requirements.md
- [ ] Examples MUST match the requirements (not simplified versions)
- [ ] Examples produce non-trivial outputs (≥2 rows & ≥2 columns unless command inherently produces one)

**Implementation Completeness Check:**
- [ ] All code changes listed in design.md "Planned Change List" are completed
- [ ] All files mentioned in the plan have been touched and implemented
- [ ] No planned changes are missing or incomplete

**Reconciliation Status:**
- If ANY check fails: Post `RECONCILE: FAIL — <short reason>` → Go back to implementation, fix gaps, then return here
- If ALL checks pass: Post `RECONCILE: PASS ✅` → Proceed to PR

**Loop Rule:** Do NOT claim completion or open a PR until `RECONCILE: PASS ✅` has been posted
