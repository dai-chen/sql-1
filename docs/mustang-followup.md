# Mustang SQL Compatibility — Deferred Buckets

Entries for error buckets skipped or deferred from the fix passes in
`feature/mustang-sql-it-local-changes`, with the required fix category
per `docs/mustang-reduce-sql-api-failures.md`.

Category legend (from the prompt):
- A: Tweak `UnifiedSqlSpec` parser/validator/conformance config.
- B: Register Calcite library functions via the operator table.
- C: Add post-parse rewriters.
- SKIP: requires custom Calcite grammar, custom `SqlOperator`, new UDFs
  with Java bodies, or changes outside `api/` (e.g. core/, legacy/, AE).

## Remaining SQL/api buckets after Fixes 1–5

- **CONVERT_TIMEZONE(<CHARACTER>, <CHARACTER>, <TIMESTAMP>) — 14 failures.**
  `ConvertTzRewriter` (Fix #3 + #4) renames `convert_tz` → `CONVERT_TIMEZONE`
  and wraps the datetime operand in `CAST(... AS TIMESTAMP)`. Calcite 1.41's
  `CONVERT_TIMEZONE` is registered with operand checker
  `OperandTypes.CHARACTER_CHARACTER_DATETIME`, so the rewritten call should
  match, but it's still rejected as "No match found for function signature
  CONVERT_TIMEZONE(<CHARACTER>, <CHARACTER>, <TIMESTAMP>)". Suspected root
  cause: Calcite's signature matcher requires `TIMESTAMP(3)`/nanosecond
  precision or a specific family. Category **C refinement** — inspect
  Calcite's `OperandTypes.CHARACTER_CHARACTER_DATETIME` implementation and
  adjust the CAST to produce the exact expected type. Alternatively,
  register a custom `SqlOperator` with looser operand checking (SKIP per
  the prompt's skip list).

- **AVG(<column>) where column type is temporal — ~8 failures.**
  `AvgTemporalCastRewriter` (Fix #5) handles `AVG(CAST(x AS temporal))`
  and `AVG(timestamp(x))` forms but cannot handle `AVG(<bare_column>)`
  because the column type is unknown at post-parse stage. Remaining
  failures are in `AggregationIT#testAvgTimeInMemory`,
  `AggregationIT#testAvgDateTimePushedDown`. Category **C advanced** —
  needs type-aware rewriter (runs after partial validation / before
  aggregation) or AE-side numeric coercion.

- **percentile(<NUMERIC>, <NUMERIC>) — 3 failures.**
  Calcite has `PERCENTILE_CONT2` and `PERCENTILE_DISC2` in
  `SqlLibraryOperators` but they expect the `WITHIN GROUP (ORDER BY …)`
  syntax, not a function-call with two numeric args. Rewriting to the
  correct syntax would require substantial AST restructuring
  (aggregate → window / ordered aggregate). Category **C advanced or SKIP**
  per the prompt (new UDF / custom operator).

- **YEAR_MONTH is not a valid time frame — 2 failures.**
  Queries use intervals like `INTERVAL '1-2' YEAR_MONTH`, a MySQL-specific
  compound interval. Calcite's Babel parser rejects `YEAR_MONTH` as a time
  frame. Category **A** — investigate whether a `SqlConformanceEnum` or
  custom `TimeFrameSet` setting can enable `YEAR_MONTH`. Likely **SKIP**
  otherwise.

## Long-tail SQL/api singletons (≤1 failure each, 70+ buckets)

Scattered single-occurrence failures (`modulus`/`divide`/`subtract` etc.
singletons were fixed in Fix #3). Remaining singletons include obscure
function-name mismatches or exotic query shapes. Each would need its own
Category B/C treatment with <1 failure payoff. **Deferred** — long-tail
clean-up after AE reaches full compatibility on high-frequency buckets.

## SQL/preflight (25 failures, parser-level)

- **`.01` literal parser error — 3 failures.** Numeric literal starting
  with a dot is not recognized by the Babel parser with BIG_QUERY lex. The
  OpenSearch SQL parser accepts this. Category **A** — investigate
  `SqlParser.Config` options. **Deferred.**
- **`{t imestamp '...'}` JDBC escape — 2 failures.** JDBC escape syntax
  for timestamp literals. Category **A** — parser config; or SKIP if not
  supported by the Babel parser.
- **`\'b\'` quoted identifier — 1 failure.** Deferred.
- Misc `ExpressionEvaluationException` / `SqlParseException` from the
  legacy SQL parser (7 buckets, mostly 1 failure each). These originate
  in `SQL/preflight` — our analyzer bailed out before reaching the
  Calcite path, so they don't count as api/ failures. Deferred.

## Test-origin failures (20 failures)

These are **test-code assertion mismatches**, not server errors. They
include tests that expected a particular HTTP status (e.g. "Expected: 400
but was: 404"), tests asserting on deprecated V1-engine response shapes,
and tests asserting the absence of a `ResponseException` for queries
that now succeed through the analytics-engine path (semantic
expectations that no longer hold). **Out of scope** — fixing these
requires test-code changes which the prompt forbids.

## SQL/other (7 failures)

- `unsupported method: ABSa` (1)
- `err find condition class …` (1)
- Misc internal plugin wiring (5).
These originate in code paths under `SQL/plugin/` (non-api), beyond the
scope of api/-only work.
