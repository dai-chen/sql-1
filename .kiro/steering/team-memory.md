---
inclusion: always
---

# Team Memory

Follow these steps for each **design, review, or code-generation interaction**.

---

## 1. Author Identification

- Assume all principles in the Memory MCP come from the same author: **default_user** (me).
- When interpreting memory, treat it as **my design taste and review standards** distilled from my past PR comments.
- If there is any conflict between generic best practices and stored memory, **prefer the stored memory** unless I explicitly override it.

---

## 2. Memory Retrieval

- Before proposing designs, refactors, or generating code:
  - Use the **Memory MCP** to retrieve relevant items from my “memory”.
  - Focus on entities with these `entity_type` values:
    - `core_principle` (team tenets, overarching rules)
    - `design_pattern` (architectural or structural patterns)
    - `code_maintenance` (clean code, error handling, readability, naming, decomposition)
    - `test_approach` (how to test, what to test, test structure)
    - `performance` (efficiency, scalability, resource usage)
    - `documentation` (comments, docs, API clarity)
    - `refactoring` (how and when to improve existing code)
  - Use those retrieved principles as **primary guidance** when:
    - Making design choices
    - Suggesting refactors
    - Generating or modifying code
    - Proposing test strategies

---

## 3. Memory (What to Extract During Work)

While designing or coding, watch for **new or clarified principles** that fit these categories:

- **Core Principles (`core_principle`)**  
  High-level tenets that should guide many future decisions.

- **Design Patterns (`design_pattern`)**  
  Preferred architectural layouts, module boundaries, or specific patterns I tend to favor or reject.

- **Code Maintenance (`code_maintenance`)**  
  Clean code and maintainability: readability, naming, decomposition, error handling, logging, and similar concerns.  
  **Exclude** low-level formatting or style issues that are already enforced or auto-fixed by tools.

- **Test Approach (`test_approach`)**  
  My expectations about tests: when they are required, how they should be structured, what edge cases matter.

- **Performance (`performance`)**  
  Opinions about efficient algorithms, data structures, and patterns to avoid performance regressions.

- **Documentation (`documentation`)**  
  Expectations for comments, public API docs, and explanations in complex areas.

- **Refactoring (`refactoring`)**  
  My typical guidance on when and how to refactor code safely, including small, incremental improvements.

---

## 4. Memory Update

- When you discover or infer a **stable, reusable principle** from my current instructions or new PR feedback:
  - **Create or update a memory entity** in the Memory MCP with:
    - A concise `name` describing the principle.
    - An appropriate `entity_type` **only** from:
      - `core_principle`, `design_pattern`, `code_maintenance`,
        `test_approach`, `performance`, `documentation`, `refactoring`.
    - One or more **observations** that:
      - Describe the principle in my wording where possible.
      - Include any nuance or conditions (when to apply or not).
  - Connect related entities via relations when helpful:
    - e.g., a `design_pattern` that supports a specific `core_principle`,
      or a `refactoring` rule that enforces a `code_maintenance` guideline.
- Prefer **fewer, high-quality, stable entries** over noisy or one-off notes.
- Only store principles that are likely to be useful for **future design, review, or code-generation tasks** in this project.
