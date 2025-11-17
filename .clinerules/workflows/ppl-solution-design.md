---
description: "Phase 2 – Solution design and implementation plan for a new PPL command in the OpenSearch SQL plugin."
author: "OpenSearch SQL Team"
version: "1.0"
tags: ["ppl", "opensearch", "sql plugin", "design", "plan", "calcite", "antlr", "planner"]
globs: ["**/*.md", "**/*.java", "**/*.g4"]
---

> ROLE: Architect/Designer
> GOAL: Make high-level design decisions and emit a single **Implementation Plan** for coding.
> SCOPE: Decide subset/composition, equivalent SQL for the 3 use cases, pushdown feasibility, and whether any UDF/UDAF is needed. Synthesize file-impact from recent similar PRs.
> OUT OF SCOPE: Writing production code; low-level plan internals beyond what’s required by the Implementation Plan.
> DELIVERABLE: `plans/ppl/<command>-implementation-plan.md`
> GATES: After §2.2 decisions, STOP and require “APPROVE: 2.2 DECISIONS”. After plan creation, require “APPROVE: SOLUTION”.

<detailed_sequence_of_steps>

# PPL Command Solution Design Workflow (Phase 2)

This phase produces a **concise design** and one **Implementation Plan** file to drive Phase 3.
No production code in this phase.

> **Precondition:** Phase 1 finished and the maintainer confirmed **“Requirements: LOCKED ✅”**.  
> **Input (requirements):** `rfcs/ppl/<command>-requirements.md`

---

## 2.0 Read the Phase-1 Requirements (input)

- Open and read: `rfcs/ppl/<command>-requirements.md`
- Extract for this design:
  - **v1 syntax & parameters**
  - **Use cases (3) with expected outputs**
  - **Behavioral notes** (null vs missing, coercion, time/locale, ordering)
  - **Out-of-scope** and any **open questions**

> If requirements are unclear, **STOP AND ASK**. Do not guess.

---

## 2.1 Identify nearest reference commands (context only)

- Locate 1–2 **similar PPL commands** in the repo (by syntax/semantics).  
  Note their patterns to reuse (grammar shape, AST/visitor touchpoints, test layout).  
  *No file edits yet.*

---

## 2.2 High-level design decisions (non-technical summary)

Answer **exactly** these:

1) **Subset or composition?**  
   Is this command a **subset** of an existing command’s features, or a **composition** of existing PPL commands/operators?

2) **Equivalent SQL**  
   What **SQL statement(s)** (ANSI/Spark-ish) would represent each of the **3 Phase-1 use cases**?

3) **Pushdown feasibility**  
   Can this be **pushed down** to OpenSearch **DSL**?  
   - Choose: **full**, **partial**, or **none**  
   - If partial/none, state the boundary briefly.

4) **UDF/UDAF needed?**  
   List names + signatures only (no impl details). If **none**, say **none**.

> Deeper technical details (logical/physical plans, node types, typing rules, codegen) are left to Phase 3.

### 2.2 Gate — Maintainer confirmation (hard stop)

- Present a one-screen summary of the four decisions above.  
- **Do not proceed** until the maintainer replies with:

> **“APPROVE: 2.2 DECISIONS”**

On denial/changes, revise §2.2 and re-request approval.

---

## 2.3 Design notes (tight, optional)

- One short paragraph on **risk/complexity** or **perf expectations**, if any.  
- Any **assumptions** that must hold when we enter implementation.

---

## 2.4 Change-list synthesis from recent similar PRs (evidence-based)

> Derive a tentative file-impact plan by inspecting **recent merged PRs** that added/extended **similar PPL commands** in `opensearch-project/sql` (avoid keeping a static checklist).

How to gather evidence (choose one or both):

**A) GitHub CLI (recommended)**

```bash
# List recent merged PRs with likely relevant keywords
gh pr list -R opensearch-project/sql --state merged --limit 30 \
  --search 'PPL in:title add OR new OR command'

# For each candidate PR number (e.g., 1234), show changed files
gh pr view 1234 -R opensearch-project/sql --json files \
  --jq '.files[].path' | sort -u
```

**B) GitHub Web (fallback)**

- Open https://github.com/opensearch-project/sql/pulls
- Search titles like: PPL: add, PPL command, new command (or specific command names)
- Open 2–4 closest PRs → Files changed → copy paths → group by subsystem.

Output: a grouped list (Grammar/Parser, AST nodes, Visitors, Tests, Docs) with PR references.

---

## 2.5 Implementation Plan (artifact for Phase 3)

Create a single plan file that Phase 3 will consume as its contract.
Path: `plans/ppl/<command>-implementation-plan.md`

Required sections (must be present verbatim):

- Header
    - Command: <command>
    - Date:
    - Owner:
    - Input Requirements: `rfcs/ppl/<command>-requirements.md`
    - Approver(s):
- High-level Decisions (copied from §2.2)
    - Subset/Composition:
    - SQL equivalents for 3 use cases:
    - Pushdown feasibility (full/partial/none) + boundary note:
    - UDF/UDAF (names + signatures or “none”):
- Evidence & References
    - Similar Commands Considered:
    - PRs inspected (IDs/links) for change-list synthesis:
- Planned Change List (grouped) (synthesized in §2.4; may evolve)
    - Grammar/Parser (`*.g4`):
    - AST nodes (`core/.../ast/tree/*`):
    - Visitors (AstBuilder, CalciteRelNodeVisitor, Anonymizer, others):
    - Tests (unit, syntax, integration pushdown & non-pushdown, explain, v2 compat, anonymizer):
    - Docs (`docs/user/ppl/cmd/*.rst`, index links):
- Task Breakdown (ordered)
    - Grammar/Parser
    - AST node(s)
    - Visitors (AstBuilder → Calcite planner → Anonymizer → others)
    - Unit tests
    - Syntax parser tests
    - Integration tests (pushdown / non-pushdown / explain / v2 compat / anonymizer)
    - Docs (.rst + index)
    - Final suite & PR
- Test Mapping (Phase-1 use cases → doctest acceptance)
    - Put all three Phase-1 use cases under the “Examples” section of
`docs/user/ppl/cmd/[commandname].rst` as doctest code blocks.
    - Include one negative case with the exact expected error text as a doctest.
    - These doctests must pass and serve as the acceptance test.
- Definition of Done
    - Doctest acceptance passes for all 3 use cases + negative case
    - Unit + syntax tests pass locally
    - Docs complete; examples match outputs
    - Error messages stable with IDs
    - Pushdown behavior aligns with §2.2 decision
    - PR template sections filled (links to requirements, plan, and doctest report)
- Mandatory Test Gates to run in Phase 3

```
./gradlew test
./gradlew :integ-test:integTest
./gradlew :doctest:doctest -DignorePrometheus
```

*(Phase 3 will stop on the first failure and report logs.)*

---

## 2.6 Deliverables

- `plans/ppl/<command>-implementation-plan.md` (the single file Phase 3 will consume)
    - Contains §2.2 decisions
    - Contains §2.4 evidence-based change list
    - Contains ordered tasks, test mapping, DoD, and test gates

---

## 2.7 Exit gate

Present the Implementation Plan and request explicit approval:

> “APPROVE: SOLUTION”

Upon approval, proceed to Phase 3 (Implementation).
If Phase-1 requirements change later, return to Phase 1 and re-lock before revising this plan.

</detailed_sequence_of_steps>
