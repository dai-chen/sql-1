---
description: "Phase 1 – RFC and requirements analysis for implementing new PPL commands in the OpenSearch SQL plugin."
author: "OpenSearch SQL Team"
version: "1.0"
tags: ["ppl", "opensearch", "sql plugin", "command implementation", "requirements", "spec", "splunk"]
globs: ["**/*.md"]
---

> ROLE: Product Manager (PM)
> GOAL: Produce a minimal, shippable **requirements spec** for a new PPL command.
> SCOPE: Requirements only — syntax scope, parameters, 3 use cases + expected outputs, behavioral notes.
> OUT OF SCOPE: SQL translations, UDF lists, design, implementation, tests.
> DELIVERABLE: `rfcs/ppl/<command>-requirements.md`
> HARD RULES: Do NOT write code or design details in this phase.
> EXIT: Post “Requirements: LOCKED ✅” only after maintainer approval.

<detailed_sequence_of_steps>

# PPL Command RFC & Requirements Workflow (Phase 1)

This workflow is for **requirements gathering and specification only**.

> **Rule:** Do **not** write production code, design docs, or tests in this phase.  
> **Outcome:** a single **Requirements Summary (handoff)** file for Phase 2.

## 1.0 External Baseline Study (Cline action)

> **External baseline:** Read Splunk's Search Reference for the **same command**:  
> https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference

1. **Open** the Splunk page for this command and read end-to-end.  
2. **Capture**:
   - Syntax forms, parameters, defaults, notable constraints
   - Behavioral semantics: null/missing handling, type coercion, ordering, determinism
   - 1–2 canonical examples
3. Produce a concise **Baseline Summary**:
   - **What Splunk supports**
   - **Potential drift** vs our PPL (NULL vs MISSING, coercion, timezone/locale)
   - **Initial v1 scope proposal**: what we will support **now** vs **out-of-scope**

## 1.1 Requirements Confirmation with Maintainer (interactive)

Negotiate a **minimal, shippable v1** spec. **Keep this phase non-technical** (no SQL translations, no UDF lists).

1) **Syntax (scope-limited)**
   - Provide a concise grammar sketch (EBNF/ANTLR-style) for the **narrowed v1**.
   - List parameters with types, allowed ranges, defaults, and whether optional/required.
   - Call out explicitly what we are **not** supporting in v1 (out-of-scope list).

2) **Use cases (at most 3)**
   - Provide **three** high-frequency PPL examples (copy-pastable).
   - For each, include a PPL query using the new command with a tiny input sketch **and** the **expected result** (table/JSON).
   - Each example must map directly to the v1 syntax above.

> If any ambiguity or conflict with existing PPL commands arises, **stop and ask**. Do not guess.

## 1.2 Requirements Summary (handoff artifact for Phase 2)

Create a **single** file that Phase 2 will consume.  
**Path:** `rfcs/ppl/<command>-requirements.md`

**Contents (requirements only; no design/implementation details):**
- **Overview**: what the command does and when to use it
- **Supported Syntax (v1 scope)**: grammar sketch + parameter table (type, default, required?)
- **Use Cases (3)**: the three examples with expected outputs (tables/JSON)
- **Behavioral Notes / Gotchas**:
  - Null vs missing semantics
  - Type coercion rules
  - Time/locale assumptions
  - Ordering/stability (guaranteed order or “unspecified”)
  - Limits/constraints applicable to v1
- **Out-of-Scope (v1)**: explicitly list what is deferred
- **Open Questions (if any)**: items needing maintainer decisions
- **Approval Record**: date, approver, approval token used

> **Do NOT include** technical discussion such as SQL translations, UDF/UDAF plans, pushdown notes, file impact, or test wiring here—those belong to **Phase 2 (Solution Design)**.

Provide the file and read it back to the maintainer for final confirmation.

## 1.3 Exit Gate (hard stop)

- Present the Baseline Summary and the finalized **Requirements Summary** file.
- Ask the maintainer to confirm correctness and completeness of requirements.

Only after explicit confirmation, post in chat:

> **“Requirements: LOCKED ✅”**

This token ends Phase 1. Any later change to syntax, use cases, or behavior requires **returning to this phase** and re-locking before design/implementation continues.

</detailed_sequence_of_steps>
