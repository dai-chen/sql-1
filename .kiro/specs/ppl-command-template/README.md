# PPL Command Development Template

This template provides a structured 3-phase approach for developing new PPL commands in the OpenSearch SQL plugin.

## How to Use This Template

1. **Copy this template** to `.kiro/specs/[your-command-name]/`
2. **Follow the phases in order:**
   - Phase 1: Complete `requirements.md`
   - Phase 2: Complete `design.md`
   - Phase 3: Complete `tasks.md`
3. **Get approval** at each phase gate before proceeding

## Three-Phase Workflow

### Phase 1: Requirements (RFC)
**File:** `requirements.md`  
**Role:** Product Manager  
**Goal:** Define what to build (syntax, use cases, behavior)  
**Exit Gate:** "Requirements: LOCKED ✅"

Focus on:
- Baseline study (Splunk reference)
- Syntax scope (v1)
- 3 use cases with expected outputs
- Behavioral notes
- Out-of-scope items

**Do NOT include:** SQL translations, UDF lists, design details, implementation plans

### Phase 2: Solution Design
**File:** `design.md`  
**Role:** Architect/Designer  
**Goal:** Make high-level design decisions and create implementation plan  
**Exit Gates:** 
1. "APPROVE: 2.2 DECISIONS" (after high-level decisions)
2. "APPROVE: SOLUTION" (after complete plan)

Focus on:
- Subset/composition decision
- SQL equivalents for use cases
- Pushdown feasibility
- UDF/UDAF requirements
- File-impact plan from recent PRs
- Task breakdown

**Do NOT include:** Production code, low-level implementation details

### Phase 3: Implementation
**File:** `tasks.md`  
**Role:** Developer/Implementer  
**Goal:** Execute the plan exactly as specified  
**Exit Gate:** "RECONCILE: PASS ✅"

Focus on:
- Following task breakdown in order
- Implementing planned changes
- Passing all test gates
- Doctest acceptance (3 use cases + negative)
- PR preparation

**Do NOT include:** Unplanned scope additions

## File References

Use Kiro's file reference syntax to link between documents:
```markdown
See the requirements: #[[file:requirements.md]]
```

## Test Gates

All implementations must pass:
```bash
./gradlew test
./gradlew :integ-test:integTest
./gradlew :doctest:doctest -DignorePrometheus
```

## Related Steering Files

- `.kiro/steering/ppl-development.md` - PPL development principles
- `.kiro/steering/testing-requirements.md` - Test requirements
- `.kiro/steering/coding-standards.md` - General coding standards
