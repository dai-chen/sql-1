---
description: "Phase 3 – Implement exactly what's in the Phase-2 Implementation Plan for a new PPL command in the OpenSearch SQL plugin."
author: "OpenSearch SQL Team"
version: "1.0"
tags: ["ppl", "opensearch", "sql plugin", "implementation", "calcite", "antlr", "testing", "doctest"]
globs: ["**/*.java", "**/*.g4", "**/*.md", "**/*.rst", "**/build.gradle"]
---

> ROLE: Developer/Implementer (with Tester mindset)
> GOAL: Implement EXACTLY what the Phase-2 plan specifies.
> SOURCE OF TRUTH: `plans/ppl/<command>-implementation-plan.md`
> SCOPE: Execute the plan’s **Task Breakdown** in order; no unplanned work.
> ACCEPTANCE: Doctest in user docs (Examples) MUST pass (3 use cases + 1 negative).
> OUT OF SCOPE: Adding scope not in the plan. If new work is needed, PAUSE and request plan update + re-approval.
> EXIT: Open PR only when plan’s Definition of Done is fully satisfied.

<detailed_sequence_of_steps>

# PPL Command Implementation Workflow (Phase 3)

**Goal:** Implement the command **exactly as specified** in the approved plan from Phase 2.  
**Source of truth for acceptance:** **Doctest** (Examples in user docs) **must pass**.

> **Preconditions**
> - Phase 1: **“Requirements: LOCKED ✅”**
> - Phase 2: Approved plan token **“APPROVE: SOLUTION”**
> - Plan file present: `plans/ppl/<command>-implementation-plan.md`

---

## 3.0 Load & validate the Implementation Plan (hard stop if invalid)

1. **Open** `plans/ppl/<command>-implementation-plan.md`.
2. **Verify the required sections exist (verbatim):**
    - Header (Command/Date/Owner/Input Requirements/Approver(s))
    - **High-level Decisions** (subset/composition, SQL equivalents, pushdown feasibility, UDF/UDAF)
    - **Evidence & References** (similar commands, PRs inspected)
    - **Planned Change List (grouped)**
    - **Task Breakdown (ordered)**
    - **Doctest Acceptance (source of truth)**
    - **Definition of Done**
    - **Mandatory Test Gates**
3. **Confirm approval present:** the plan references **“APPROVE: SOLUTION”**.
4. **If anything is missing/unclear/conflicts with repo reality → STOP AND ASK** the maintainer to amend Phase 2.  
   Do **not** proceed with guesses or add items that are not in the plan.

---

## 3.1 Branch & safety

- Create a feature branch named from the plan header (e.g., `feature/ppl-<command>`).
- Commit in small, reviewable slices (one commit per Task Breakdown item).

---

## 3.2 Execute the Task Breakdown **in the exact order from the plan**

> **Rule:** Do not introduce tasks that aren’t listed.  
> If you discover new required work (e.g., additional UDF), **pause**, propose an update to the Phase-2 plan, and wait for a refreshed **“APPROVE: SOLUTION”**.

For each task listed in **Task Breakdown** of the plan:

1. **Implement only the files implied by the plan’s “Planned Change List” groups** (Grammar/Parser, AST, Visitors, Tests, Docs).
2. After each **major** task completes, run a fast compile to catch regressions:
   ```bash
   ./gradlew :ppl:compileJava
   ```
3. Commit with a message referencing the exact task name from the plan.

*(Examples of tasks you might see in the plan: Grammar/Parser → AST node(s) → Visitors → Unit tests → Syntax tests → Doctest acceptance → Docs → Final suite & PR. Follow the plan literally.)*

---

## 3.3 Doctest acceptance

- Ensure all **three Phase-1 use cases** and **one negative case** are placed in **Examples** as doctest blocks in:
  ```
  docs/user/ppl/cmd/[commandname].rst
  ```
- Run doctests (stop on first failure):
  ```bash
  ./gradlew :doctest:doctest -DignorePrometheus
  ```
- Fix issues until doctests **pass**.
- Commit: `Doctest acceptance green for all examples (+ negative case)`

> If the expected outputs in doctest don’t match reality, **do not silently change behavior**.  
> Reconcile with Phase-1 requirements or return to Phase-2 for a plan adjustment and renewed **“APPROVE: SOLUTION”**.

---

## 3.4 Unit & Syntax tests (per plan)

- Execute tests listed in the plan (do not invent new suites unless the plan lists them):
  ```bash
  ./gradlew test
  ```
- If the plan includes specific class targets, run those too (e.g., `--tests` filters).
- Keep commits aligned with the plan’s test items.

---

## 3.5 Integration tests

- Ensure all required IT (pushdown / non-pushdown / explain / v2 compat / anonymizer) **MUST** be added and then:
  ```bash
  ./gradlew :integ-test:integTest
  ```

---

## 3.6 Final Mandatory Test Gates (must match the plan)

Run exactly the plan’s gates (at minimum, doctest + unit):

```bash
./gradlew test
./gradlew :doctest:doctest -DignorePrometheus
# Run the next only if it appears in the plan:
# ./gradlew :integ-test:integTest
```

- If any gate fails → **STOP**, fix within the scope of the plan.
- If fixes require scope changes, go back to Phase-2 and update the plan first.

---

## 3.7 Final Reconciliation (MUST PASS)

- Verify **Examples** in `docs/user/ppl/cmd/<command>.rst`:
    - Tests in Examples can use different test dataset. But it **MUST** cover all the same scenarios in **Test Mappings** in `rfcs/ppl/<command>-requirements.md`.
    - Tests in Examples **MUST** meet the requirement in **Use Case** in `rfcs/ppl/<command>-requirements.md`.
    - Confirm Examples **MUST** match the three Phase-1 use cases (not simplified), and outputs are non-trivial. (≥2 rows & ≥2 columns unless the command inherently produces one).
    - Otherwise, **STOP** → revert Examples → **fix code**.
- Verify all code changes listed in the **Planned Change List** are completed (files touched and implemented) in `plans/ppl/<command>-implementation-plan.md`.
- If anything is missing or deviates from the plan:  
  **Post:** `RECONCILE: FAIL — <short reason>` → **GO TO 3.2 Execute the Task Breakdown**, address gaps, then return here.
- If all checks pass:  
  **Post:** `RECONCILE: PASS ✅` → proceed to the next step.

> Loop rule: **Do not claim completion** or open a PR until `RECONCILE: PASS ✅` has been posted.

---

## 3.8 PR packaging (reference the plan & requirements)

Prepare a PR that includes:

- Links to:
    - `rfcs/ppl/<command>-requirements.md` (Phase-1)
    - `plans/ppl/<command>-implementation-plan.md` (Phase-2)
- A short summary of **High-level Decisions** (copied from the plan).
- **Doctest acceptance evidence**:
    - Attach the last 30 lines of doctest output or link to the report.
- Test status summary matching the plan’s **Definition of Done**.

*(Do not include unplanned scope. If something changed, pause and get the plan updated/approved.)*

---

## 3.9 Exit criteria

Only mark implementation complete when **all** items in the plan’s **Definition of Done** are satisfied:

- Doctest acceptance **PASSED** for 3 use cases + negative case
- Unit (+ any other tests the plan required) **PASSED**
- Docs updated and examples match doctest outputs
- Error messages stable (IDs if applicable)
- Pushdown behavior aligns with the plan’s decision
- PR includes plan/requirements links and test evidence

</detailed_sequence_of_steps>
