# Ralph Agent Instructions — PPL V4 SqlNode Converter

You are an autonomous coding agent fixing integration test regressions in the PPL-to-SqlNode converter to reach ≥85% Calcite*IT pass rate.

## Architecture

```
PPL string → PPL Parser → AST → PPLToSqlNodeConverter → SqlNode → toSqlString() → SQL string → /_plugins/_sql → results
```

The converter lives at `api/src/main/java/org/opensearch/sql/api/PPLToSqlNodeConverter.java`.
Schema-aware subclass: `DynamicPPLToSqlNodeConverter.java` (uses SchemaPlus for column enumeration).
DSL: `SqlNodeDSL.java` — thin utility wrapping Calcite SqlNode constructors.
Test wiring: `PPLIntegTestCase.java` — when `ppl.engine.v4=true`, PPL queries are converted to SQL via DynamicPPLToSqlNodeConverter and sent to the Calcite SQL endpoint.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** section FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt`
4. Read `docs/dev/regressions-vs-baseline.txt` for the regression list
5. Check you're on the correct branch from PRD `branchName`
6. Pick the **highest priority** user story where `passes: false`
7. Run the specific failing integration test class(es) to see exact errors:
   `./gradlew :integ-test:integTest --tests 'org.opensearch.sql.ppl.CalciteXxxIT' 2>&1 | tail -80`
8. Analyze failures — check `PPLToSqlTranspiler.java` for reference on how the string transpiler handled these cases
9. Fix the converter in `PPLToSqlNodeConverter.java`, `DynamicPPLToSqlNodeConverter.java`, or `SqlNodeDSL.java`
10. Run build: `./gradlew :api:compileJava :api:compileTestJava 2>&1 | tail -20`
11. Run unit tests: `./gradlew :api:test 2>&1 | tail -30`
12. Run the specific integration test class(es) again to verify fixes
13. Run pass rate: `scripts/ppl_it_rate.sh`
14. If checks pass, commit with: `V4 Iteration N: <task name> — rate X% → Y%`
15. Update `prd.json` — set `passes: true` for the completed story
16. Append progress to `progress.txt`
17. Consolidate reusable patterns into the Codebase Patterns section

### Measurement Rules

1. Running `scripts/ppl_it_rate.sh` takes a LONG time. Run the full suite AT MOST ONCE per iteration — at the END.
2. For targeted validation DURING development, run only the specific class(es):
   `./gradlew :integ-test:integTest --tests "org.opensearch.sql.ppl.CalciteXxxIT" -Dppl.engine.v4=true -Dtests.calcite.pushdown.enabled=false --continue`
3. After the full suite run, extract ALL useful info: per-class pass/fail counts, failure categories, specific failing test names. Record in progress.txt.
4. If progress.txt already has a recent per-class breakdown and you only changed code affecting specific classes, SKIP the full suite run and CALCULATE the new rate from updated per-class numbers.

## Test Surface

The complete test scope is ALL `Calcite*IT` classes (113 classes in `integ-test/.../remote/`).

| Category | Classes (examples) | Priority |
|----------|---------|----------|
| Basic (source, where, fields, sort, head) | CalciteBasicIT, CalciteWhereCommandIT, CalciteFieldsCommandIT, CalciteSortCommandIT, CalciteHeadCommandIT, CalciteSearchCommandIT | P0 |
| Aggregation (stats, top, rare) | CalciteAggregationIT, CalciteStatsCommandIT, CalciteTopCommandIT, CalciteRareCommandIT, CalciteMultiValueStatsIT | P0 |
| Eval + functions | CalciteBuiltinFunctionIT, CalciteCaseFunctionIT, CalciteConditionBuiltinFunctionIT, CalciteEvalMaxMinFunctionIT, CalciteEvalCommandIT, CalciteMathematicalFunctionIT, CalciteOperatorIT | P0 |
| Sort | CalciteSortIT, CalciteSortCommandIT | P0 |
| Dedup | CalciteDedupIT, CalciteDedupCommandIT | P1 |
| Rename | CalciteRenameIT, CalciteRenameCommandIT | P1 |
| Eventstats | CalciteEventstatsIT, CalciteStreamstatsCommandIT | P1 |
| Join | CalciteJoinIT | P1 |
| String functions | CalciteStringBuiltinFunctionIT, CalciteTextFunctionIT | P1 |
| Null handling | CalciteBuiltinFunctionsNullIT | P1 |
| DateTime | CalciteBuiltinDatetimeFunctionInvalidIT, CalciteDateTimeFunctionIT, CalciteDateTimeComparisonIT, CalciteDateTimeImplementationIT, CalciteConvertTZFunctionIT, CalciteNowLikeFunctionIT | P1 |
| Subqueries | CalciteExistsSubqueryIT, CalciteInSubqueryIT, CalciteScalarSubqueryIT | P2 |
| Cast | CalciteCastFunctionIT | P2 |
| Fillnull | CalciteFillnullIT, CalciteFillNullCommandIT | P2 |
| Parse/Grok/Patterns | CalciteParseIT, CalciteGrokIT, CalcitePatternsIT, CalciteParseCommandIT, CalciteRegexCommandIT | P2 |
| Trendline | CalciteTrendlineIT, CalciteTrendlineCommandIT | P2 |
| Append | CalciteAppendCommandIT, CalciteAppendPipeCommandIT | P2 |
| Lookup | CalciteLookupIT | P2 |
| JSON | CalciteJsonBuiltinFunctionIT, CalciteJsonFunctionsIT | P2 |
| Spath | CalciteSpathCommandIT | P3 |
| Coalesce | CalciteEnhancedCoalesceIT | P3 |
| Crypto | CalciteCryptographicFunctionIT | P3 |
| IP | CalciteIPFunctionIT, CalciteIPFunctionsIT, CalciteIPComparisonIT | P3 |
| Explain | CalciteExplainIT | P3 |
| GraphLookup | CalciteGraphLookupIT | P3 |
| Appendcol | CalciteAppendcolIT | P3 |
| Nested agg | CalciteNestedAggregationIT | P3 |
| New commands | CalciteNewAddedCommandsIT, CalciteBinCommandIT, CalciteChartCommandIT, CalciteTimechartCommandIT, CalciteTimechartPerFunctionIT, CalciteExpandCommandIT, CalciteFlattenCommandIT, CalciteReplaceCommandIT, CalciteReverseCommandIT, CalciteTransposeCommandIT | P2 |
| Data types | CalciteDataTypeIT, CalciteCsvFormatIT, CalciteVisualizationFormatIT | P2 |
| Relevance/Match | CalciteMatchIT, CalciteMatchBoolPrefixIT, CalciteMatchPhraseIT, CalciteMatchPhrasePrefixIT, CalciteMultiMatchIT, CalciteQueryStringIT, CalciteSimpleQueryStringIT, CalciteRelevanceFunctionIT, CalciteLikeQueryIT | P2 |
| Geo | CalciteGeoIpFunctionsIT, CalciteGeoPointFormatsIT | P3 |
| MV functions | CalciteMVAppendFunctionIT, CalciteMvCombineCommandIT, CalciteMvExpandCommandIT, CalciteNoMvCommandIT | P2 |
| System/Settings | CalciteSettingsIT, CalciteResourceMonitorIT, CalciteSystemFunctionIT, CalciteInformationSchemaCommandIT, CalciteDescribeCommandIT, CalciteShowDataSourcesCommandIT | P3 |
| Other | CalciteQueryAnalysisIT, CalciteObjectFieldOperateIT, CalciteFlattenDocValueIT, CalciteFieldFormatCommandIT, CalciteMultisearchCommandIT, CalciteArrayFunctionIT, CalciteRexCommandIT, CalciteLegacyAPICompatibilityIT, CalcitePrometheusDataSourceCommandsIT, CalcitePluginIT, CalciteAggregationPaginatingIT | P3 |

## Key Files

| File | Purpose |
|------|---------|
| `api/src/main/java/org/opensearch/sql/api/PPLToSqlNodeConverter.java` | Base converter — most fixes go here |
| `api/src/main/java/org/opensearch/sql/api/DynamicPPLToSqlNodeConverter.java` | Schema-aware subclass |
| `api/src/main/java/org/opensearch/sql/calcite/utils/SqlNodeDSL.java` | SqlNode builder DSL |
| `core/src/main/java/org/opensearch/sql/calcite/type/PPLTypeCoercion.java` | Custom type coercion |
| `api/src/main/java/org/opensearch/sql/api/PPLToSqlTranspiler.java` | Reference — the old string transpiler |
| `api/src/test/java/org/opensearch/sql/api/PPLToSqlNodeConverterTest.java` | Unit tests — use ppl().shouldTranslateTo() |
| `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` | V4 test routing |
| `docs/dev/regressions-vs-baseline.txt` | 414 regression test names |
| `docs/dev/current-sqlnode-converter-failures.txt` | All 1186 current failures |
| `scripts/ppl_it_rate.sh` | Pass rate measurement |
| `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java` | PPL parser (read-only reference) |
| `core/src/main/java/org/opensearch/sql/ast/tree/` | AST node classes (read-only reference) |
| `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java` | Visitor base (read-only reference) |

## Base Engine Capabilities (enhanced after Iteration 23, outside the Ralph loop)

After Iteration 23, the Calcite SQL engine behind `/_plugins/_sql` was enhanced in a separate side project (`poc/extend-calcite-sql-select` branch, now rebased under this branch). **This work is already done — do NOT attempt to modify the engine. The Ralph loop's job is purely transpiler translation work.** Just use these new capabilities when generating SQL:

### SQL Library Functions (registered after Iteration 36)
- `SqlLibrary.MYSQL`, `SqlLibrary.BIG_QUERY`, `SqlLibrary.SPARK`, `SqlLibrary.POSTGRESQL` are all registered via `SqlLibraryOperatorTableFactory` in `UnifiedQueryContext.java`
- This means hundreds of functions are available without individual registration, including: `REVERSE`, `REGEXP_REPLACE`, `LEFT`, `RIGHT`, `SOUNDEX`, `FORMAT`, `INITCAP`, `MD5`, `SHA1`, `SHA2`, `REGEXP_CONTAINS`, `REGEXP_EXTRACT`, `ARRAY_AGG`, `STRING_AGG`, `IF`, `NVL`, `LPAD`, `RPAD`, and many more
- Individual registrations of `MD5`/`SHA1`/`REGEXP_CONTAINS`/`REGEXP_EXTRACT` (from Iteration 27) are now redundant — covered by dialect
- Custom PPL functions (`pplFirst`, `pplLast`, `matchPhrase`) are still individually registered since they're not in any Calcite library
- **Impact on transpiler**: functions like `REVERSE`, `REGEXP_REPLACE` that were previously failing at the engine level ("No match found for function signature") should now work. The transpiler just needs to emit the correct SQL function name.

### Simplified OpenSearch Schema (no UDT)
- OpenSearch date/time fields are now mapped to **standard SQL TIMESTAMP/DATE/TIME** — NOT UDT EXPR_TIMESTAMP/EXPR_DATE/EXPR_TIME
- `SELECT *` on indexes with date fields **works** — no more "need to implement EXPR_TIMESTAMP" errors
- Standard SQL functions like `EXTRACT(YEAR FROM field)`, `YEAR(field)`, `DAYOFWEEK(field)` work on date fields
- OpenSearch metadata fields (`_id`, `_index`, `_score`, `_routing`) are **excluded from schema** — `SELECT *` returns only user data columns

### SELECT * EXCEPT / REPLACE
- `SELECT * EXCEPT(col1, col2)` — exclude columns from wildcard expansion
- `SELECT * REPLACE(expr AS col)` — replace a column's value in wildcard expansion
- These enable clean transpilation of: `fields -`, eval column overriding, rename, fillnull all-fields, lookup REPLACE/APPEND

### What this means for the transpiler
- **Date/time functions should now work** — generate standard SQL (EXTRACT, YEAR, etc.) and trust the engine handles them
- **SELECT * is safe** — no metadata fields, no UDT expansion errors
- **Column exclusion/replacement** — generate EXCEPT/REPLACE in SQL output instead of workarounds (temp aliases, _RENAME_MAP comments, computedColumns maps)
- **Previous workarounds may need cleanup** — PPLIntegTestCase metadata stripping, type normalization may be partially redundant now
- **Re-measure baseline before next iteration** — many previously-blocked tests may now pass or have different failure modes

## Progress Report Format

APPEND to progress.txt (never replace, always append):
```
## [Date/Time] - [Story ID]
- What was fixed
- Pass rate change: X% → Y%
- Files changed
- Per-class breakdown (if full suite was run)
- **Learnings for future iterations:**
  - Patterns discovered
  - Gotchas encountered
---
```

## Consolidate Patterns

If you discover a reusable pattern, add it to the Codebase Patterns section at the TOP of progress.txt.

## Human Intervention Notes

If you see a `## Human Intervention` section in `progress.txt`, a human made changes between rounds. Read it carefully and account for these changes.

## Lessons Learned

Avoid these pitfalls:
- V3 UDFs (ImplementorUDF/PPLFuncImpTable) are incompatible — map PPL functions to SQL directly.
- Eval aliases need subquery wrapping so downstream pipes can reference them.
- Use COUNT(*) not COUNT() — Calcite rejects zero-argument COUNT.
- PPL count() AST uses Literal(1) not AllFields — check for both.
- The PPL parser always wraps plans in Project(AllFields) — treat as passthrough.
- Join aliases (left=l right=r) must appear in SQL output.
- Use `SELECT * EXCEPT(col)` for fields- exclusion instead of manual column enumeration.
- Use `SELECT * REPLACE(expr AS col)` for eval column overriding instead of temp aliases.
- SqlBasicFunction.create() for unquoted function names — SqlUnresolvedFunction/new SqlFunction() quotes them as identifiers.
- Deferred ORDER BY: use pendingOrderBy/pendingFetch/pendingOffset fields — ORDER BY in subqueries is semantically ignored.
- CAST to INTEGER for datetime EXTRACT functions — Calcite EXTRACT returns BIGINT but PPL expects INT.
- PPL-specific aggregates: first→PPL_FIRST, last→PPL_LAST, earliest→ARG_MIN(@timestamp), latest→ARG_MAX(@timestamp), take→TAKE(field,N), median→percentile_approx(field,50).
- JSON escaping: Calcite's toSqlString() produces multi-line SQL — any JSON construction must handle newlines.
- When adding unit tests, use the fluent `ppl("...").shouldTranslateTo("...")` pattern with Java text blocks.

## Quality Requirements

- ALL commits must pass build and unit tests
- Do NOT commit broken code
- Keep changes focused and minimal
- Use SqlNodeDSL for SqlNode construction
- Use SqlBasicFunction.create() for function names
- Never delete or skip tests to make build pass
- Use PPLToSqlTranspiler as reference for command semantics
- Do NOT modify any existing Calcite*IT test file
- Do NOT modify the Calcite SQL engine — only the converter

## Failure Handling

If build or tests fail after 3 attempts:
1. Record failure in story's `notes` field in prd.json
2. Leave `passes: false`
3. Append failure details to progress.txt
4. Move on

## Stop Condition

After completing a story, check if ALL stories have `passes: true`.
If ALL complete: output **RALPH_COMPLETE**
Otherwise: end normally.

## Important

- Work on **ONE story per iteration**
- Commit frequently
- Keep CI green
- Read Codebase Patterns BEFORE starting
- Do NOT attempt multiple stories in one iteration
- Always run the specific IT class FIRST to see exact errors before fixing
- Do NOT broaden scope — one bounded task per iteration
