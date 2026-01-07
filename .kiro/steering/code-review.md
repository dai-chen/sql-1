---
inclusion: manual
---

# Code Review Manual (Critical-Only)

## Goal
Given a pull request (diff + description), produce **3–5** of the **most critical** review comments focused on:
1) **API / UX design** (ergonomics, clarity, compatibility)
2) **Correctness** (logic, edge cases, safety, concurrency)
3) **Performance** (complexity, resource use, scalability)

If the PR is very small, still return up to 3 comments (or fewer if truly nothing matters).

## Non-goals
- Do **NOT** comment on formatting, import order, minor naming bikeshedding, or “style nits” unless it impacts API clarity or correctness.
- Do **NOT** list every possible improvement. Only the highest-impact issues.
- Do **NOT** restate the code or describe what changed unless needed to explain a risk.

## Severity rubric
- **Blocker:** likely wrong results / data loss / crash / leak / security issue / incompatible public API
- **High:** plausible production issue, hard-to-debug behavior, or significant perf risk
- **Medium:** important improvement but unlikely to break prod immediately

## Must-check (quick pass)
- **Behavioral change:** Does this PR change outputs, defaults, error behavior, or ordering?
- **Resource lifecycle:** Any new streams/iterators/clients/executors? Ensure close/shutdown happens on all paths.
- **Failure modes:** Retries/timeouts/backoff, partial failures, and error mapping (don’t swallow exceptions).
- **State growth:** Caches, buffers, queues, maps — any risk of unbounded growth?
- **Concurrency:** Shared mutable state, lazy init, thread-safety assumptions.
- **Compatibility:** Config/mapping/schema changes; safe rollout and downgrade path.

## Review Approach (do in this order)
1) **Understand intent**
   - Read PR title/description first. Infer the user-facing contract and invariants.
2) **API / UX scan**
   - Public interfaces, configs, defaults, naming, docs, error messages.
   - Backward compatibility and migration risk.
3) **Correctness scan**
   - Null/empty handling, boundary cases, exception paths, cleanup (close/try-finally), idempotency.
   - Concurrency/thread-safety if relevant (shared state, caching, lazy init).
4) **Performance scan**
   - Algorithmic complexity (O(n), O(n²)), hot paths, unnecessary allocations, repeated parsing/serialization.
   - I/O patterns, batching, retries/backoff, timeouts, memory growth.

## What counts as “critical”
Pick issues that are likely to cause one or more of:
- Production bug / data loss / incorrect results
- API confusion or hard-to-use UX that will become hard to change later
- Significant performance regression or scalability ceiling
- Security/safety issues (resource leaks, unbounded growth, unsafe defaults)

## Output Rules
- Output **exactly 3–5 comments**, ordered by **severity/impact**.
- Each comment must include:
  - **Severity**: `Blocker` | `High` | `Medium`
  - **Category**: `API/UX` | `Correctness` | `Performance`
  - **Where**: file/module + function/class (line numbers if provided)
  - **Why it matters**: one short paragraph
  - **Actionable fix**: concrete recommendation (not vague)
  - **Test / proof**: what test to add or what scenario to verify

If something is uncertain due to missing context, phrase it as a **targeted question** plus a suggested validation.

## Response Format (must follow)
### 1) [Severity] [Category] — <short title>
**Where:** <file :: symbol>
**Issue:** <what’s wrong>
**Impact:** <why it matters>
**Suggestion:** <how to fix>
**Validate:** <test case / scenario>

(repeat for 3–5 items)

## Examples of good comments (style)
- Prefer: “This public method returns `null` on failure; callers can’t distinguish ‘not found’ vs ‘error’. Consider `Optional`/typed error.”
- Prefer: “This loop re-parses JSON per row; move parsing outside the loop or cache compiled schema.”
- Avoid: “Rename x to y” unless it materially improves API clarity or prevents misuse.
