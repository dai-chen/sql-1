# Issue Summary — `\0` in Aliased Column Names (AE / Unified Query Path)

## Symptom

In the JSON response from queries that go through the Analytics Engine (AE) /
unified query path, aliased columns can show up with names of the form
`"<expression>\u0000<alias>"` (a NUL byte separating the expression text and
the user-facing alias) instead of just `"<alias>"`.

## Example query

```sql
SELECT ISNULL(lastname) AS name FROM <index>
```

Expected JSON response:

```json
{
  "schema":  [{"name":"name", "type":"boolean"}],
  "datarows": [...],
  ...
}
```

Observed (without the fix):

```json
{
  "schema":  [{"name":"ISNULL(lastname)\u0000name", "type":"boolean"}],
  "datarows": [...],
  ...
}
```

The `\u0000` is a literal NUL byte; tools that print it as a string typically
show `\0` or skip it entirely, making the visible name look like
`"ISNULL(lastname)name"`.

## Where the symptom is reportedly observed

- Path: SQL or PPL queries routed to `AnalyticsExecutionEngine` via
  `RestUnifiedQueryAction` (i.e., the AE / unified query path, not the
  legacy V1 SQL path).
- Surface: the response schema (`schema[*].name`) — i.e., what client tools
  like JDBC, the SQL workbench, and dashboards see as the column name.

## What helps you find a failing IT

Look for an integration test that satisfies all of:
1. Routes through the **AE / unified query path** (likely uses
   `RestUnifiedQueryAction` or asserts a plan that goes through
   `AnalyticsExecutionEngine`). Tests that hard-code the legacy V1 path
   won't show this.
2. The query has an **alias on a non-trivial expression** in `SELECT` —
   e.g., `<udf>(...) AS alias` or `<expr> AS alias`. The most reproducible
   shape is `<scalar_or_aggregate_fn>(<col>) AS <alias>`.
3. The test asserts on the **response schema column name** (or compares the
   full JSON), not just on data rows.

Common candidates to check (in `integ-test/src/test/java/org/opensearch/sql/`):
- `ConditionalIT` (uses `ISNULL`, `IFNULL`, `NULLIF` with aliases)
- `AggregationIT` / `CalciteSQLAggregationIT` (uses `COUNT(*) AS cnt` etc.)
- `MathematicalFunctionIT` (uses scalar functions with aliases)
- `JdbcFormatIT` / `ExplainIT` / any IT that asserts on `/schema/0/name`
  in the JSON response

If you find one that fails when our `AnalyticsExecutionEngine.buildSchema`
fix is reverted, please share:
- IT class name + test method (file:line)
- The exact SQL/PPL query
- The actual `schema[0].name` it produces
- Whether the test is asserting against `\0`-containing string or just
  expects the alias

## Where the fix currently lives

**File:** `core/src/main/java/org/opensearch/sql/executor/analytics/AnalyticsExecutionEngine.java`
**Method:** `buildSchema(List<RelDataTypeField>)`

```java
for (RelDataTypeField field : fields) {
  ExprType exprType = convertType(field.getType());
  String name = field.getName();
  // Strip Calcite's internal NUL separator (expression\0alias → alias).
  int nul = name.indexOf('\0');
  if (nul >= 0) {
    name = name.substring(nul + 1);
  }
  columns.add(new Schema.Column(name, null, exprType));
}
```

It's a defensive boundary strip — the rule "if a NUL appears in a column
name, take only what comes after the last NUL".

## Origin of the `\0` convention (per prior deep-dive)

A previous version of `CalciteRexNodeVisitor.visitAlias` (commit
`de26834e2`, since reverted) intentionally encoded `expression\0alias`
into a single Calcite field name string for V1-engine schema-formatting
compatibility. That encoding was removed in commits `d2d6a0218` /
`3211f0b79`. The current `visitAlias` only sets the alias, so it
shouldn't be a producer.

The handoff hypothesized that Calcite's *own* internal naming may produce
`\0` (e.g., `SqlValidatorUtil.uniquify` or `addToSelectList`) under
disambiguation, but this was never empirically demonstrated.

## What we want to find out

1. Which **current** code path (in this repo or in Calcite itself) puts a
   `\0` into a field name that reaches `buildSchema`?
2. If we can identify and fix that producer, can we drop the boundary
   strip in `buildSchema`?

If you have a concrete failing IT, that pins down the producer
deterministically — running it through `AnalyticsExecutionEngine` with the
fix removed will print the exact field name and the path that produced it.
