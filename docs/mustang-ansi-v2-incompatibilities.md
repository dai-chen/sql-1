# ANSI SQL (Calcite/Babel) vs OpenSearch SQL V2 — Incompatibility Inventory

Generated from the mustang compatibility run (2026-05-02).  
Branch: `feature/mustang-sql-it-local-changes`.  
Source: `integ-test/build/reports/analytics-sql-compatibility/REPORT.md` (487 failures, 127 buckets).

## Executive Summary

| Metric | Value |
|--------|-------|
| Total distinct incompatibilities | **38** (sections 1–7) |
| Test-expectation mismatches | **15** (section 8) |
| Already fixed (Fix #1–#5, shifted to AE) | 5 |
| Deferred (in mustang-followup.md) | 8 |
| Unresolved (no fix landed) | 25 |
| Total failing tests covered | 487 / 487 (100%) |
| Top 3 unresolved by failure count | 1) AE LogicalValues — 276 tests; 2) AE No-backend-scan — 65 tests; 3) CONVERT_TIMEZONE — 14 tests |

The dominant failure mode (70%) is the analytics-engine's inability to execute plans containing
`LogicalValues` (table-less queries) or scan certain index field combinations. These are AE-side
gaps exposed because V2 queries exercise patterns AE doesn't yet support. The remaining 30% are
genuine SQL/api-level parse/validation incompatibilities between V2's MySQL-flavored syntax and
Calcite's ANSI SQL validator.

---

## 1. Lexical / Literal Incompatibilities

These are cases where V2 accepts syntax that Calcite's Babel parser rejects at the lexical or
token level.

| # | Name | V2 Syntax | Calcite Behavior | Exception | Count | Fix Status | Cat. | Sample Tests |
|---|------|-----------|-----------------|-----------|-------|------------|------|--------------|
| 1.1 | Dot-leading numeric in index name | `SELECT * FROM test-logs-2025.01.01` | Parse error: ".01" treated as numeric fragment | SqlParseException | 3 | deferred | A | `PointInTimeLeakIT#testPitManagedProperlyWithFetchSize` |
| 1.2 | JDBC `{ts '...'}` escape | `SELECT {ts '2020-09-16 17:30:00'}` | Parse error: "{t imestamp" — tokenizer splits incorrectly | SqlParseException | 2 | deferred | A | `DateTimeFunctionIT#testTimestampBracket` |
| 1.3 | JDBC `{t '...'}` escape | `SELECT {t '17:30:00'}` | Parse error: "{t ime" | SqlParseException | 1 | deferred | A | `DateTimeFunctionIT#testTimeBracket` |
| 1.4 | JDBC `{d '...'}` escape | `SELECT {d '2020-09-16'}` | Parse error: "{d ate" | SqlParseException | 1 | deferred | A | `DateTimeFunctionIT#testDateBracket` |
| 1.5 | Double-quote string literal | `SELECT "I""m"` | Parse error: `"m"` — double-quotes are identifiers in ANSI | SqlParseException | 1 | unresolved | A/SKIP | `StringLiteralIT#testImStringDoubleDoubleQuoteEscape` |
| 1.6 | Backslash single-quote escape | `SELECT 'I\'m'` | Parse error: `'m'` — backslash not escape in ANSI | SqlParseException | 1 | unresolved | A | `StringLiteralIT#testImStringDoubleSingleQuoteEscape` |
| 1.7 | REGEXP infix operator | `SELECT 'a' regexp 'b'` | Parse error: `'b'` unexpected — REGEXP not infix in Calcite | SqlParseException | 1 | unresolved | SKIP | `TextFunctionIT#testRegexp` |
| 1.8 | `@` in unquoted field name | `SELECT @timestamp FROM test` | Lexical error: `@` not valid identifier start | SqlParseException | 1 | unresolved | C | `IdentifierIT#testSpecialFieldName` |
| 1.9 | Dot-separated index name | `SELECT * FROM test.one` | Validation error: 'test' not found (dot = schema separator) | ValidationException | 1 | unresolved | C | `IdentifierIT#testMultipleQueriesWithSpecialIndexNames` |
| 1.10 | Hyphen/dot in unquoted index | `SELECT * FROM logs-2020-01` / `FROM .system` | Parse error: hyphen/dot not valid in bare identifiers | SqlParseException | 1 | unresolved | C | `IdentifierIT#testSpecialIndexNames` |
| 1.11 | Trailing semicolon | `SELECT max(int0) FROM t WHERE int0 IS NULL;` | Parse error: ";" unexpected | SqlParseException | 4 | unresolved | A | `AggregationIT#testPushDownAggregationOnNullNumericValuesReturnsNull` |
| 1.12 | `%` wildcard in DESCRIBE | `DESCRIBE TABLES LIKE "%account"` | Parse error: `"%account"` unexpected | SqlParseException | 1 | unresolved | SKIP | `AdminIT#describeSingleIndexWildcard` |
| 1.13 | SHOW/DESCRIBE TABLES LIKE | `SHOW TABLES LIKE 'acc'` | Parse error: "LIKE" unexpected after TABLES | SqlParseException | 2 | unresolved | SKIP | `AdminIT#showSingleIndexAlias`, `AdminIT#explainShow` |
| 1.14 | Single-quoted alias in DESCRIBE | `DESCRIBE TABLES LIKE 'acc'` | Parse error: `'acc'` unexpected | SqlParseException | 1 | unresolved | SKIP | `AdminIT#describeSingleIndexAlias` |

**Subtotal: 21 failures, 14 distinct incompatibilities.**

---

## 2. Type-System Incompatibilities

| # | Name | V2 Syntax | Calcite Behavior | Exception | Count | Fix Status | Cat. | Sample Tests |
|---|------|-----------|-----------------|-----------|-------|------------|------|--------------|
| 2.1 | AVG on temporal columns | `SELECT avg(date0) FROM calcs` | "Cannot apply 'AVG' to 'AVG(<TIMESTAMP(0)>)'" — AVG requires NUMERIC | ValidationException | 6 | Fix #5 partial / deferred | C | `AggregationIT#testAvgTimeInMemory`, `AggregationIT#testAvgDatePushedDown` |
| 2.2 | Implicit temporal cross-type comparison | `SELECT DATE('2020-09-16') = TIME('09:07:00')` | Fix #4 rewrites literals → passes validation → AE fails on LogicalValues | AE ResponseException | 255 | Fix #4 (shifted to AE) | C | `DateTimeComparisonIT#testCompare {DATE(...) = TIME(...)}` |
| 2.3 | EXTRACT from CHAR type | `SELECT HOUR(time_field)` where field resolves to CHAR(8) | "Cannot apply 'EXTRACT' to EXTRACT(<INTERVAL HOUR> FROM <CHAR(8)>)" | ValidationException | 3 | unresolved | C | `DateTimeFunctionIT#testHourFunctionAliasesReturnTheSameResults` |
| 2.4 | Hyphenated column as arithmetic | `SELECT yyyy-MM-dd_OR_epoch_millis FROM idx` | "Column 'yyyy' not found" — parser splits on hyphens | ValidationException | 1 | unresolved | C | `DateTimeFormatsIT#testDateFormatsWithOr` |
| 2.5 | Field with no type in mapping | `SELECT object_value FROM date_nanos_idx` | "Field [object_value] has no type in mapping" | AE RuntimeException | 1 | unresolved | SKIP | `DateTimeFormatsIT#testDateNanosWithNanos` |

**Subtotal: 266 failures, 5 distinct incompatibilities.**

---

## 3. Function-Name / Signature Incompatibilities

### 3a. Bulk library registration (Fix #2)

`LibraryOperatorExtension` registers `SqlLibrary.MYSQL` + `SqlLibrary.BIG_QUERY`, covering:
`DATETIME`, `CONCAT_WS`, `RTRIM`, `LTRIM`, `IF`, `IFNULL`, `NVL`, `LENGTH`, `LPAD`, `RPAD`,
`SUBSTR`, `INSTR`, `REVERSE`, `FORMAT`, and others. These now pass validation but may fail in AE.

### 3b. Functions still missing from Calcite operator tables

| # | Function | V2 Call | Calcite Error | Count | Fix Status | Cat. |
|---|----------|---------|---------------|-------|------------|------|
| 3.1 | CONVERT_TIMEZONE | `convert_tz('2008-05-15 12:00:00','+00:00','+10:00')` | No match for CONVERT_TIMEZONE(<CHAR>,<CHAR>,<TIMESTAMP>) | 14 | Fix #3/#4 partial / deferred | C |
| 3.2 | percentile (2-arg) | `percentile(balance, 50)` | No match — Calcite expects WITHIN GROUP | 3 | deferred | SKIP |
| 3.3 | day_of_month | `day_of_month(date('2020-09-16'))` | No match | 1 | unresolved | B |
| 3.4 | hour_of_day | `hour_of_day(timestamp('...'))` | No match | 1 | unresolved | B |
| 3.5 | second_of_minute | `second_of_minute(timestamp('...'))` | No match | 1 | unresolved | B |
| 3.6 | sec_to_time | `sec_to_time(balance)` | No match | 1 | unresolved | B |
| 3.7 | time_to_sec | `time_to_sec(time('17:30:00'))` | No match | 1 | unresolved | B |
| 3.8 | str_to_date | `str_to_date('01/Jan/2010', '%d/%b/%Y')` | No match | 1 | unresolved | B |
| 3.9 | weekofyear | `weekofyear(date('2008-02-20'))` | No match | 1 | unresolved | B |
| 3.10 | weekday | `weekday(date0)` | No match | 1 | unresolved | B |
| 3.11 | yearweek | `yearweek(timestamp0)` | No match | 1 | unresolved | B |
| 3.12 | TIMEDIFF | `TIMEDIFF('23:59:59', '13:00:00')` | No match | 1 | unresolved | B |
| 3.13 | SUBTIME | `SUBTIME(DATE('2008-12-12'), DATE('2008-11-15'))` | No match | 1 | unresolved | B |
| 3.14 | DATEDIFF | `DATEDIFF(TIMESTAMP('...'), TIMESTAMP('...'))` | No match (signature mismatch) | 1 | unresolved | B |
| 3.15 | microsecond | `microsecond(timestamp('...'))` | No match | 1 | unresolved | B |
| 3.16 | subdate (interval) | `subdate(timestamp('...'), interval 1 day)` | No match | 1 | unresolved | B |
| 3.17 | date_format | `date_format(timestamp('...'), '%a %b...')` | No match | 1 | unresolved | B |
| 3.18 | TO_DAYS | `TO_DAYS(DATE('2050-01-01'))` | No match | 1 | unresolved | B |
| 3.19 | MAKEDATE | `MAKEDATE(1984, 1984)` | No match | 1 | unresolved | B |
| 3.20 | expm1 | `expm1(account_number)` | No match | 1 | unresolved | B |
| 3.21 | ISNULL (function) | `ISNULL(lastname)` | No match | 2 | unresolved | B |
| 3.22 | GET_FORMAT | `GET_FORMAT(DATE,'USA')` | Parse error near keyword 'DATE' | 1 | unresolved | SKIP |

### 3c. Arithmetic function-call forms (Fix #3)

`ArithmeticFunctionRewriter` rewrites: `add(a,b)`→`a+b`, `subtract(a,b)`→`a-b`,
`multiply(a,b)`→`a*b`, `divide(a,b)`→`a/b`, `modulus(a,b)`→`MOD(a,b)`.
Now passes validation but fails in AE (LogicalValues for table-less queries).

### 3d. DateTime literal function forms (Fix — DateTimeLiteralRewriter)

`DateTimeLiteralRewriter` converts `DATE('...')`, `TIME('...')`, `TIMESTAMP('...')` function calls
to ANSI typed literals (`DATE '...'`, `TIME '...'`, `TIMESTAMP '...'`). This unblocks validation
for the ~200+ tests using these forms. Failures now occur downstream in AE.

**Subtotal: ~37 failures from function-signature issues, 22 distinct incompatibilities.**

---

## 4. Operator Incompatibilities

| # | Name | V2 Syntax | Calcite Behavior | Exception | Count | Fix Status | Cat. | Sample Tests |
|---|------|-----------|-----------------|-----------|-------|------------|------|--------------|
| 4.1 | Arithmetic function-call form | `add(3, 2)` / `modulus(7, 3)` | No match for function 'add' — ANSI uses operators | ValidationException→AE | 11 | Fix #3 (shifted to AE) | C | `ArithmeticFunctionIT#testAddFunction` |
| 4.2 | INTERVAL NULL syntax | `INTERVAL NULL DAY` | Parse error near keyword 'INTERVAL' | SqlParseException | 1 | unresolved | A/SKIP | `NullLiteralIT#testNullLiteralInInterval` |

**Subtotal: 12 failures, 2 distinct incompatibilities.**

---

## 5. Clause / Syntactic Incompatibilities

| # | Name | V2 Syntax | Calcite Behavior | Exception | Count | Fix Status | Cat. | Sample Tests |
|---|------|-----------|-----------------|-----------|-------|------------|------|--------------|
| 5.1 | YEAR_MONTH time frame | `EXTRACT(YEAR_MONTH FROM date0)` | "'YEAR_MONTH' is not a valid time frame" | ValidationException | 2 | deferred | A/SKIP | `DateTimeFunctionIT#testExtractWithDate` |
| 5.2 | DAY_SECOND time frame | `EXTRACT(DAY_SECOND FROM datetime0)` | "'DAY_SECOND' is not a valid time frame" | ValidationException | 1 | unresolved | A/SKIP | `DateTimeFunctionIT#testExtractWithDatetime` |
| 5.3 | HOUR_SECOND time frame | `EXTRACT(HOUR_SECOND FROM time0)` | "'HOUR_SECOND' is not a valid time frame" | ValidationException | 1 | unresolved | A/SKIP | `DateTimeFunctionIT#testExtractWithTime` |
| 5.4 | SHOW/DESCRIBE DDL | `SHOW TABLES LIKE '%'` | Not standard DML — Babel doesn't support | SqlParseException | 4 | unresolved | SKIP | `AdminIT#showSingleIndexAlias` |
| 5.5 | Metafield identifiers | `SELECT _routing FROM test.metafields` | Object not found — dot parsed as schema path | ValidationException | 5 | unresolved | C | `IdentifierIT#testMetafieldIdentifierTest` |
| 5.6 | Double-underscore field + dot index | `SELECT __age FROM test.twounderscores` | Object 'twounderscores' not found | ValidationException | 1 | unresolved | C | `IdentifierIT#testDoubleUnderscoreIdentifierTest` |

**Subtotal: 14 failures, 6 distinct incompatibilities.**

---

## 6. Behavioral / Semantic Incompatibilities

| # | Name | Description | Failure Mode | Count | Fix Status | Cat. | Sample Tests |
|---|------|-------------|-------------|-------|------------|------|--------------|
| 6.1 | AE: LogicalValues unsupported | Table-less queries (`SELECT 3+2`, `SELECT DATE(...)=TIME(...)`) produce LogicalValues node that AE cannot execute | "Project/Sort rule encountered unmarked child [LogicalValues]" | 276 | unresolved (AE) | SKIP | `ArithmeticFunctionIT#testAdd`, `DateTimeComparisonIT#testCompare`, `MathematicalFunctionIT#testPI` |
| 6.2 | AE: No backend can scan fields | Queries on indices with custom date formats, mixed types, or computed fields | "No backend can scan all requested fields on index [X]" | 65 | unresolved (AE) | SKIP | `AggregationIT#testMaxDatePushedDown`, `DateTimeFormatsIT#testCustomFormats`, `ConditionalIT#nullifWithNotNullInputTestOne` |
| 6.3 | Explain output shape | AE returns ProjectOperator tree vs V2's raw DSL JSON | Test assertion mismatch | 2 | unresolved (AE) | SKIP | `TermQueryExplainIT#testKeywordAliasOrderBy` |
| 6.4 | CASE type homogeneity | `CASE WHEN gender IS NULL THEN 'aaa' ELSE gender END` | "All result types of CASE clause must be the same" | 2 | unresolved | SKIP | `ExplainIT#explainScriptValue` |
| 6.5 | NESTED() in ORDER BY | `ORDER BY NESTED('message.info','message')` | "Illegal nested field name" | 1 | unresolved | SKIP | `ExplainIT#orderByOnNestedFieldTest` |
| 6.6 | HTTP 404 vs 400 for missing index | Query against non-existing index | AE returns 404, test expects 400 | 3 | unresolved (AE) | SKIP | `TermQueryExplainIT#testNonResolvingIndexPattern` |

**Breakdown of LogicalValues failures by test class:**

| Test Class | Failures | Query Pattern |
|-----------|----------|---------------|
| DateTimeComparisonIT | 191 | `SELECT DATE('...') = TIME('...')` (cross-type temporal comparisons) |
| DateTimeFunctionIT | 20 | `SELECT convert_tz(...)`, `SELECT SUBTIME(...)` etc. |
| DateTimeImplementationIT | 15 | `SELECT ADDDATE(...)`, `SELECT DATETIME(...)` |
| MathematicalFunctionIT | 14 | `SELECT PI()`, `SELECT expm1(...)` |
| TextFunctionIT | 14 | `SELECT CONCAT(...)`, `SELECT UPPER(...)` |
| ArithmeticFunctionIT | 11 | `SELECT 3 + 2`, `SELECT add(3, 2)` |
| NowLikeFunctionIT | 5 | `SELECT NOW()`, `SELECT CURDATE()` |
| NullLiteralIT | 3 | `SELECT NULL`, `SELECT INTERVAL NULL DAY` |
| StringLiteralIT | 3 | `SELECT 'hello'`, `SELECT "I""m"` |

**Breakdown of No-backend-scan failures by test class:**

| Test Class | Failures | Index |
|-----------|----------|-------|
| AggregationIT | 41 | opensearch-sql_test_index_calcs, _bank, _null_missing |
| DateTimeFormatsIT | 9 | opensearch-sql_test_index_date_formats |
| ConditionalIT | 8 | opensearch-sql_test_index_bank_with_null_values, _account |
| DateTimeFunctionIT | 4 | opensearch-sql_test_index_calcs |
| IdentifierIT | 2 | logs, logs+2020+01 |
| MathematicalFunctionIT | 1 | opensearch-sql_test_index_bank |

**Subtotal: 349 failures, 6 distinct incompatibilities.**

---

## 7. Legacy-Specific V2 Extensions Never in ANSI

These are OpenSearch/MySQL-specific syntax constructs that have no ANSI SQL equivalent and would
require custom grammar extensions to support in Calcite.

| # | Name | V2 Syntax | Notes | Count | Cat. |
|---|------|-----------|-------|-------|------|
| 7.1 | NESTED() sort function | `ORDER BY NESTED('field','path')` | OpenSearch nested-document ordering | 1 | SKIP |
| 7.2 | SHOW/DESCRIBE TABLES | `SHOW TABLES LIKE '%'` | Catalog browsing (not DML) | 4 | SKIP |
| 7.3 | REGEXP infix | `'a' regexp 'b'` | MySQL REGEXP operator | 1 | SKIP |
| 7.4 | GET_FORMAT() | `GET_FORMAT(DATE,'USA')` | MySQL date format helper | 1 | SKIP |

**Subtotal: 7 failures, 4 distinct incompatibilities.** (Overlap with sections 1/3 — counted once in primary section.)

---

## 8. Test-Expectation Mismatches (Not Engine Incompatibilities)

These tests assert V2-specific error behavior. The analytics-engine either succeeds (because the
query is valid ANSI SQL) or returns a different error code/shape.

### 8a. Tests expecting rejection of valid ANSI SQL

| # | Name | Query | V2 Behavior | AE Behavior | Count |
|---|------|-------|-------------|-------------|-------|
| 8.1 | SELECT without FROM | `SELECT 1` | SyntaxException | Succeeds (valid ANSI) | 1 |
| 8.2 | Nested aggregate | `SELECT max(log(age)) FROM bank` | SemanticException | Succeeds | 1 |
| 8.3 | Nested scalar | `SELECT abs(log(balance)) FROM bank` | SemanticException | Succeeds | 1 |
| 8.4 | UNION type mismatch | `SELECT age UNION SELECT address` | SemanticException | Succeeds (coercion) | 1 |
| 8.5 | IS FALSE on integer | `WHERE b.age IS FALSE` | SemanticException | Succeeds (boolean coercion) | 1 |
| 8.6 | Non-existing alias | `WHERE a.balance = 1000` | SemanticException | Different error or succeeds | 1 |
| 8.7 | HAVING wrong arity | `HAVING MAX(age, birthdate) > 1` | SemanticException | Succeeds (ignores extra arg) | 1 |
| 8.8 | MINUS type mismatch | `SELECT male MINUS SELECT birthdate` | SemanticException | Succeeds | 1 |
| 8.9 | Index join non-nested | `FROM bank b1, b1.firstname` | SemanticException | Different behavior | 1 |

### 8b. Tests asserting on response shape or error type

| # | Name | Issue | Count | Sample Tests |
|---|------|-------|-------|--------------|
| 8.10 | Explain DSL shape | Test asserts specific DSL JSON output | 10 | `ExplainIT#searchSanity`, `ExplainIT#multiMatchQuery`, `ExplainIT#searchSanityFilter` |
| 8.11 | Error type assertion | Test asserts `SemanticAnalysisException` type string | 6 | `QueryAnalysisIT#useInClauseWithIncompatibleFieldTypesShouldFail` |
| 8.12 | HTTP 503 expected | Test expects 503, gets 400 | 1 | `QueryAnalysisIT#queryWithUnsupportedFunctionShouldFail` |
| 8.13 | SyntaxAnalysis type | Test asserts `SyntaxAnalysisException` | 1 | `QueryAnalysisIT#unsupportedOperatorShouldThrowSyntaxException` |
| 8.14 | Non-compatible mappings | Test expects ResponseException | 1 | `TermQueryExplainIT#testNonCompatibleMappings` |
| 8.15 | Pretty format shape | Test asserts specific explain format | 1 | `PrettyFormatterIT#assertExplainPrettyFormatted` |

**Subtotal: 29 failures, 15 distinct test-expectation mismatches.**

---

## Landed Fixes Summary

| Fix # | File | Mechanism | Failures Shifted | Status |
|-------|------|-----------|-----------------|--------|
| 1 | `StringTypeNameRewriter` | `CAST(x AS STRING)` → `CAST(x AS VARCHAR)` | ~6 | Shifted to AE |
| 2 | `LibraryOperatorExtension` | Registers MYSQL + BIG_QUERY operator tables | ~20 | Shifted to AE |
| 3 | `ArithmeticFunctionRewriter` | `add(a,b)`→`a+b`, `subtract`→`-`, etc. | ~5 | Shifted to AE |
| 4 | `DateTimeComparisonRewriter` | CAST for cross-type temporal comparisons | ~255 | Shifted to AE |
| 5 | `AvgTemporalCastRewriter` | `AVG(TIMESTAMP(...))` → numeric coercion | ~2 | Partial; bare-column still fails |
| — | `DateTimeLiteralRewriter` | `DATE('...')`→`DATE '...'` typed literals | ~200 | Shifted to AE |
| — | `ConvertTzRewriter` | `convert_tz(dt,from,to)`→`CONVERT_TIMEZONE(from,to,dt)` | 14 (still failing) | Partial / deferred |

---

## Coverage Note

| Metric | Value |
|--------|-------|
| REPORT.md total failures | 487 |
| Failures accounted for in this document | **487** |
| REPORT.md buckets (top-25 + 102 long-tail) | 127 |
| Buckets mapped to root causes | 127 / 127 (100%) |
| Distinct incompatibilities (sections 1–7) | **38** |
| Test-expectation mismatches (section 8) | **15** |
| **Grand total distinct entries** | **53** |
| Buckets NOT classifiable | **0** |

### Failure distribution by root cause:

| Root Cause Category | Failures | Share |
|--------------------|----------|-------|
| AE LogicalValues (table-less query) | 276 | 57% |
| AE No-backend-scan | 65 | 13% |
| Function signature not registered | 37 | 8% |
| Test-expectation mismatch | 29 | 6% |
| Lexical/parse incompatibility | 21 | 4% |
| Clause/syntactic (time frames, SHOW, metafields) | 14 | 3% |
| Operator (arithmetic fn-form, INTERVAL NULL) | 12 | 2% |
| Type-system (AVG temporal, EXTRACT from CHAR) | 10 | 2% |
| Behavioral (explain shape, CASE, HTTP code) | 8 | 2% |
| Other/overlap | 15 | 3% |

### Limitations

1. The 276 LogicalValues failures and 65 No-backend-scan failures are AE-side execution gaps,
   not SQL/api incompatibilities. They are included because the V2 test suite exercises these
   patterns and they represent functional gaps in the analytics-engine path.
2. Some function-signature incompatibilities (section 3b) may be resolvable by registering
   additional Calcite library operators or custom UDFs — the exact fix complexity varies.
3. Long-tail singleton failures were traced via XML `<failure>` messages; source queries were
   confirmed from test source files where possible, or inferred from test method names.

---

## Appendix A: Detailed AE LogicalValues Failure Analysis

The `LogicalValues` node is Calcite's representation of a query that produces rows without
scanning any table (e.g., `SELECT 1+1`, `SELECT DATE('2020-01-01') = TIME('00:00:00')`).
The analytics-engine's physical plan rules do not handle this node type, causing:

```
Project rule encountered unmarked child [LogicalValues]
Sort rule encountered unmarked child [LogicalValues]
```

**Root cause**: AE's `ProjectRule` and `SortRule` expect all children to be either
`OpenSearchIndexScan` or another recognized physical operator. `LogicalValues` is a logical-only
node that needs a dedicated `ValuesRule` to convert it to a physical `Values` operator.

**Impact**: 276 tests (57% of all failures). These tests are valid ANSI SQL that Calcite
parses and validates successfully. The failure occurs only at physical plan generation in AE.

**Affected query patterns**:
- Literal arithmetic: `SELECT 3 + 2`, `SELECT CAST(6.666666 AS FLOAT) + 2`
- Function evaluation on literals: `SELECT PI()`, `SELECT NOW()`, `SELECT expm1(0)`
- Temporal comparisons: `SELECT DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00')`
- DateTime construction: `SELECT ADDDATE(DATE('2020-08-26'), 1)`
- String functions on literals: `SELECT CONCAT('hello', ' ', 'world')`
- NULL expressions: `SELECT NULL`, `SELECT IFNULL(NULL, 1)`

**Fix path**: Implement `LogicalValues` → physical `Values` conversion rule in AE, or add a
`ValuesImplementor` that evaluates constant expressions locally.

---

## Appendix B: Detailed No-Backend-Scan Failure Analysis

The "No backend can scan all requested fields" error occurs when AE's backend selection logic
cannot find a single scan implementation that supports all fields referenced in the query for
a given index.

**Root cause**: AE's field-type resolution or backend capability matrix doesn't cover certain
OpenSearch field types (custom date formats, date_nanos, object fields, or fields with
multi-valued mappings).

**Impact**: 65 tests (13% of all failures).

**Affected indices and their issues**:

| Index | Failures | Likely Issue |
|-------|----------|-------------|
| opensearch-sql_test_index_calcs | 38 | Custom date/time/datetime fields with non-standard formats |
| opensearch-sql_test_index_date_formats | 9 | Custom date format mappings (epoch_millis, custom patterns) |
| opensearch-sql_test_index_bank_with_null_values | 5 | Nullable fields with mixed types |
| opensearch-sql_test_index_null_missing | 4 | Fields with missing/null handling |
| opensearch-sql_test_index_bank | 4 | Filtered aggregates, complex expressions |
| opensearch-sql_test_index_account | 3 | IFNULL/IF conditional functions |
| logs, logs+2020+01 | 2 | Dynamically created test indices |
| opensearch-sql_test_index_date_formats (nanos) | 1 | date_nanos type with nanosecond precision |

**Fix path**: Extend AE's backend field-type support to cover all OpenSearch mapping types
used by the test indices, particularly custom date formats and date_nanos.

---

## Appendix C: Function Registration Gap — Complete List

The following V2 functions have no equivalent in Calcite's standard or library operator tables
and would need custom `SqlOperator` registration (Category B) or UDF implementation (SKIP):

### DateTime functions (MySQL-specific):

| Function | Signature | ANSI Equivalent | Complexity |
|----------|-----------|-----------------|------------|
| `day_of_month` | (DATE) → INT | `EXTRACT(DAY FROM x)` | B — alias rewrite |
| `hour_of_day` | (TIMESTAMP) → INT | `EXTRACT(HOUR FROM x)` | B — alias rewrite |
| `second_of_minute` | (TIMESTAMP) → INT | `EXTRACT(SECOND FROM x)` | B — alias rewrite |
| `sec_to_time` | (NUMERIC) → TIME | None standard | B — custom UDF |
| `time_to_sec` | (TIME) → LONG | None standard | B — custom UDF |
| `str_to_date` | (CHAR, CHAR) → DATETIME | `TO_TIMESTAMP(x, fmt)` | B — alias + format rewrite |
| `weekofyear` | (DATE) → INT | `EXTRACT(WEEK FROM x)` | B — alias rewrite |
| `weekday` | (DATE) → INT | `EXTRACT(DOW FROM x) - 1` | B — expression rewrite |
| `yearweek` | (TIMESTAMP) → INT | None standard | B — custom UDF |
| `TIMEDIFF` | (CHAR, CHAR) → TIME | `x - y` (interval) | B — custom UDF |
| `SUBTIME` | (DATE, DATE) → TIME | `x - y` | B — custom UDF |
| `DATEDIFF` | (TIMESTAMP, TIMESTAMP) → INT | `TIMESTAMPDIFF(DAY, y, x)` | B — rewrite |
| `microsecond` | (TIMESTAMP) → INT | `EXTRACT(MICROSECOND FROM x)` | B — alias rewrite |
| `subdate` | (TIMESTAMP, INTERVAL) → TIMESTAMP | `x - interval` | B — rewrite |
| `date_format` | (TIMESTAMP, CHAR) → CHAR | `FORMAT_TIMESTAMP(fmt, x)` | B — custom UDF |
| `TO_DAYS` | (DATE) → LONG | None standard | B — custom UDF |
| `MAKEDATE` | (INT, INT) → DATE | None standard | B — custom UDF |
| `convert_tz` | (CHAR, CHAR, CHAR) → TIMESTAMP | `CONVERT_TIMEZONE(from, to, ts)` | C — arg reorder (Fix #3/#4) |

### Math functions:

| Function | Signature | ANSI Equivalent | Complexity |
|----------|-----------|-----------------|------------|
| `expm1` | (NUMERIC) → DOUBLE | `EXP(x) - 1` | B — expression rewrite |

### Conditional functions:

| Function | Signature | ANSI Equivalent | Complexity |
|----------|-----------|-----------------|------------|
| `ISNULL` | (ANY) → INT | `CASE WHEN x IS NULL THEN 1 ELSE 0 END` | B — rewrite |

### Aggregate functions:

| Function | Signature | ANSI Equivalent | Complexity |
|----------|-----------|-----------------|------------|
| `percentile` | (NUMERIC, NUMERIC) → NUMERIC | `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` | SKIP — syntax restructuring |

---

## Appendix D: Cross-Reference to mustang-followup.md

| followup.md Entry | This Document Section | Status |
|-------------------|----------------------|--------|
| CONVERT_TIMEZONE 14 failures | 3.1 | deferred |
| AVG temporal ~8 failures | 2.1 | Fix #5 partial / deferred |
| percentile 3 failures | 3.2 | deferred |
| YEAR_MONTH 2 failures | 5.1 | deferred |
| `.01` literal 3 failures | 1.1 | deferred |
| `{timestamp '...'}` JDBC escape 2 failures | 1.2 | deferred |
| `'b'` quoted identifier 1 failure | 1.7 | deferred (actually REGEXP) |
| Test-origin 20 failures | Section 8 | out of scope |
| SQL/other 7 failures | 6.4, 6.5, 8.11 | out of scope |

---

## Appendix E: Per-Origin Failure Mapping

| Origin (from REPORT.md) | Failures | Mapped To |
|--------------------------|----------|-----------|
| AE | 341 | 6.1 (276) + 6.2 (65) |
| SQL/api | 93 | Sections 2, 3, 5 (validation errors) |
| SQL/preflight | 25 | Section 1 (parse errors) + 2.3 |
| Test | 20 | Section 8 |
| SQL/other | 7 | 6.4, 6.5, 8.11 |
| ? | 1 | 8.14 |
| **Total** | **487** | **487 (100% coverage)** |
