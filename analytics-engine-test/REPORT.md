# SQL V2 + Legacy IT — Analytics Engine Compatibility Report

**Total Tests:** 1416 | ✅ Passed: 381 (26.9%) | ❌ Failed: 555 (39.2%) | ⏭️ Skipped: 480 (33.9%)


| Area | Passed | Failed | Skipped | Pass Rate |
|------|-------:|-------:|--------:|----------:|
| SQL V2 (`sql.sql.*`) | 329 | 441 | 10 | 42.7% |
| Legacy (`sql.legacy.*`) | 52 | 114 | 470 | 31.3% |

## Failure Categories

| Category | Count | % |
|----------|------:|--:|
| Other Error | 343 | 61.8% |
| Result Mismatch | 212 | 38.2% |

## ✅ Fully Passing Test Classes

- **sql.PaginationBlackboxIT** (12 tests)
- **sql.ArithmeticFunctionIT** (11 tests)
- **sql.MatchPhraseIT** (8 tests)
- **sql.MatchPhrasePrefixIT** (8 tests)
- **legacy.PluginIT** (7 tests)
- **sql.RelevanceFunctionIT** (6 tests)
- **sql.QueryIT** (5 tests)
- **sql.StringLiteralIT** (5 tests)
- **sql.MatchBoolPrefixIT** (3 tests)
- **legacy.JoinTimeoutHintIT** (2 tests)
- **legacy.GetEndpointQueryIT** (1 tests)
- **sql.MetricsIT** (1 tests)

## 🟡 Partially Passing (>50%)

| Class | Passed | Failed | Rate |
|-------|-------:|-------:|-----:|
| sql.LikeQueryIT | 10 | 1 | 91% |
| sql.LegacyAPICompatibilityIT | 6 | 1 | 86% |
| sql.WildcardQueryIT | 14 | 4 | 78% |
| sql.MultiMatchIT | 9 | 3 | 75% |
| sql.QueryStringIT | 3 | 1 | 75% |
| sql.SimpleQueryStringIT | 3 | 1 | 75% |
| sql.DateTimeComparisonIT | 142 | 49 | 74% |
| sql.MathematicalFunctionIT | 16 | 6 | 73% |
| sql.MatchIT | 10 | 4 | 71% |
| sql.PositionFunctionIT | 5 | 2 | 71% |
| sql.NowLikeFunctionIT | 8 | 4 | 67% |
| legacy.OrdinalAliasRewriterIT | 7 | 4 | 64% |
| legacy.TypeInformationIT | 8 | 5 | 62% |
| sql.TextFunctionIT | 8 | 7 | 53% |

## ❌ Failing Test Classes

| Class | Passed | Failed | Skipped | Top Failure |
|-------|-------:|-------:|--------:|-------------|
| sql.DateTimeFunctionIT | 6 | 68 | 0 | Other Error |
| sql.AggregationIT | 8 | 46 | 0 | Other Error |
| sql.VectorSearchIT | 3 | 38 | 0 | Result Mismatch |
| sql.NestedIT | 0 | 33 | 0 | Result Mismatch |
| legacy.CsvFormatResponseIT | 1 | 21 | 3 | Other Error |
| sql.ConvertTZFunctionIT | 0 | 17 | 0 | Other Error |
| legacy.AggregationExpressionIT | 1 | 15 | 0 | Other Error |
| sql.DateTimeImplementationIT | 0 | 15 | 0 | Other Error |
| legacy.PrettyFormatResponseIT | 12 | 14 | 4 | Result Mismatch |
| sql.VectorSearchSubqueryIT | 0 | 14 | 0 | Other Error |
| sql.PaginationFallbackIT | 2 | 13 | 0 | Result Mismatch |
| sql.VectorSearchExplainIT | 0 | 13 | 0 | Other Error |
| legacy.JoinAliasWriterRuleIT | 0 | 11 | 2 | Result Mismatch |
| legacy.MetaDataQueriesIT | 0 | 11 | 2 | Other Error |
| legacy.CursorIT | 8 | 10 | 3 | Other Error |
| sql.ConditionalIT | 0 | 10 | 2 | Result Mismatch |
| sql.DateTimeFormatsIT | 1 | 10 | 0 | Other Error |
| sql.IdentifierIT | 0 | 10 | 0 | Other Error |
| sql.PaginationFilterIT | 9 | 9 | 0 | Result Mismatch |
| sql.HighlightFunctionIT | 0 | 8 | 0 | Other Error |
| sql.PaginationIT | 1 | 7 | 1 | Result Mismatch |
| legacy.ObjectFieldSelectIT | 0 | 6 | 0 | Other Error |
| sql.WindowFunctionIT | 2 | 6 | 0 | Other Error |
| legacy.MethodQueryIT | 0 | 5 | 2 | Other Error |
| sql.PaginationWindowIT | 0 | 5 | 0 | Other Error |
| legacy.JdbcTestIT | 4 | 4 | 4 | Other Error |
| sql.AdminIT | 0 | 4 | 0 | Other Error |
| sql.ComplexTimestampQueryIT | 1 | 4 | 1 | Result Mismatch |
| sql.ScoreQueryIT | 0 | 4 | 0 | Other Error |
| legacy.MalformedQueryIT | 0 | 3 | 0 | Other Error |
| legacy.PointInTimeLeakIT | 0 | 3 | 0 | Other Error |
| sql.CsvFormatIT | 0 | 3 | 0 | Result Mismatch |
| sql.NullLiteralIT | 1 | 3 | 0 | Other Error |
| sql.QueryValidationIT | 1 | 3 | 1 | Result Mismatch |
| sql.RawFormatIT | 0 | 3 | 0 | Result Mismatch |
| legacy.SqlLegacyEngineSanityIT | 1 | 2 | 0 | Other Error |
| sql.ExistsPushdownIT | 0 | 2 | 0 | Result Mismatch |
| sql.GeopointFormatsIT | 0 | 2 | 0 | Other Error |
| sql.JdbcFormatIT | 0 | 2 | 0 | Result Mismatch |
| sql.SystemFunctionIT | 0 | 2 | 0 | Result Mismatch |
| sql.PreparedStatementIT | 0 | 1 | 0 | Other Error |
| sql.SQLConcurrencyIT | 0 | 1 | 0 | Other Error |
| sql.SQLCorrectnessIT | 0 | 1 | 0 | Result Mismatch |
| sql.StandalonePaginationIT | 1 | 1 | 0 | Other Error |

## 🔍 Result Mismatch Details

### legacy.AggregationExpressionIT
- `groupByDateShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=birthdate, alias=null, type=timestamp), (name=count(*), alias=count, type=long)] in an
- `groupByDateWithAliasShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=birthdate, alias=birth, type=timestamp), (name=count(*), alias=count, type=long)] in a
- `hasGroupKeyAvgOnIntegerShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=gender, alias=null, type=string), (name=AVG(age), alias=avg, type=double)] in any orde
- `aggregateCastStatementShouldNotReturnZero`: java.lang.AssertionError: 
Expected: iterable with items [(name=SUM(CAST(male AS INT)), alias=male_sum, type=integer)] in any order
     but: not matc
- `noGroupKeyAvgOnIntegerShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=AVG(age), alias=avg, type=double)] in any order
     but: not matched: <{"name":"AVG(a

### legacy.CsvFormatResponseIT
- `simpleSearchResultWithNestedWithFlatNoAggs`: java.lang.AssertionError: expected:<2> but was:<1>
- `simpleSearchResultNotNestedNotFlatNoAggs`: java.lang.AssertionError: expected:<2> but was:<1>
- `aggAfterTermsGroupBy`: java.lang.AssertionError: 
Expected: iterable containing ["COUNT(*)"]
     but: item 0: was "{"
- `scriptedField`: java.lang.AssertionError: expected:<3> but was:<1>
- `sanitizeTest`: java.lang.AssertionError: expected:<5> but was:<35>

### legacy.CursorIT
- `noPaginationWithNonJDBCFormat`: java.lang.AssertionError: 
Expected: <1001>
     but: was <5020>

### legacy.JoinAliasWriterRuleIT
- `oneTableAliasNoCommonColumns`: java.lang.AssertionError: 
Expected: "{  \"calcite\": {    \"logical\": \"LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\\n  LogicalProjec
- `sameTablesNoAliasWithTableNameAsAliasOnColumns`: java.lang.AssertionError: Expected test to throw (an instance of org.opensearch.client.ResponseException and exception with message a string containin
- `sameTablesWithExplicitAliasOnFirst`: java.lang.AssertionError: 
Expected: "{  \"calcite\": {    \"logical\": \"LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\\n  LogicalProjec
- `commonColumnWithoutTableAliasDifferentTables`: java.lang.AssertionError: 
Expected: (an instance of org.opensearch.client.ResponseException and exception with message a string containing "Field nam
- `bothTableAliasNoCommonColumns`: java.lang.AssertionError: 
Expected: "{  \"calcite\": {    \"logical\": \"LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\\n  LogicalProjec

### legacy.MethodQueryIT
- `matchQueryTest`: java.lang.AssertionError: 
Expected: a string containing "{\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"880 Holmes Lane\\\""
     but: was "{  "c
- `queryTest`: java.lang.AssertionError: 
Expected: a string containing "query_string\\\":{\\\"query\\\":\\\"address:880 Holmes Lane"
     but: was "{  "calcite": { 

### legacy.OrdinalAliasRewriterIT
- `selectFieldiWithBacticksAndTableAliasOrderByOrdinalAndNull`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"b`.`lastname\",\n      \"type\": \"string\"\n    },\n    {\n      \
- `selectFieldiWithBacticksAndTableAliasGroupByOrdinal`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"b`.`lastname\",\n      \"type\": \"string\"\n    },\n    {\n      \
- `selectFieldiWithBacticksGroupByOrdinal`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"lastname\",\n      \"type\": \"string\"\n    }\n  ],\n  \"datarows\
- `simpleGroupByOrdinal`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"lastname\",\n      \"type\": \"string\"\n    }\n  ],\n  \"datarows\

### legacy.PrettyFormatResponseIT
- `fieldsWithAlias`: java.lang.AssertionError
- `testSizeWithGroupBy`: java.lang.AssertionError: 
Expected: <5>
     but: was <21>
- `joinQueryWithAlias`: java.lang.AssertionError

### legacy.TypeInformationIT
- `testUpperWithStringFieldReturnsString`: java.lang.AssertionError: 
Expected: iterable with items [(name=UPPER(firstname), alias=firstname_alias, type=string)] in any order
     but: not matc
- `testAddWithIntReturnsInt`: java.lang.AssertionError: 
Expected: iterable with items [(name=(balance + 5), alias=balance_add_five, type=long)] in any order
     but: not matched:

### sql.AggregationIT
- `testMaxDatePushedDown`: java.lang.AssertionError: 
Expected: iterable with items [(name=max(date0), alias=null, type=date)] in any order
     but: not matched: <{"name":"max(
- `testMinDateTimePushedDown`: java.lang.AssertionError: 
Expected: iterable with items [(name=min(timestamp(CAST(time0 AS STRING))), alias=null, type=timestamp)] in any order
     
- `testMaxTimePushedDown`: java.lang.AssertionError: 
Expected: iterable with items [(name=max(time1), alias=null, type=time)] in any order
     but: not matched: <{"name":"max(
- `testMaxTimeStampPushedDown`: java.lang.AssertionError: 
Expected: iterable with items [(name=max(CAST(datetime0 AS timestamp)), alias=null, type=timestamp)] in any order
     but:
- `testFilteredAggregateNotPushDown`: java.lang.AssertionError: 
Expected: iterable with items [[3]] in any order
     but: not matched: <[7]>

### sql.ComplexTimestampQueryIT
- `nonJoinTimestampFieldsSchema`: org.junit.ComparisonFailure: expected:<[timestamp]> but was:<[string]>
- `selectDatetimeWithoutNested`: org.junit.ComparisonFailure: expected:<[timestamp]> but was:<[string]>

### sql.ConditionalIT
- `isnullWithNullInputTest`: java.lang.AssertionError: 
Expected: iterable with items [(name=ISNULL(1/0), alias=ISNULL1, type=boolean), (name=ISNULL(firstname), alias=ISNULL2, typ
- `isnullShouldPassJDBC`: org.junit.ComparisonFailure: expected:<[ISNULL(lastname)]> but was:<[name]>
- `nullifWithNullInputTest`: java.lang.AssertionError: 
Expected: iterable with items [(name=NULLIF(1/0, 123), alias=nullif1, type=integer), (name=NULLIF(123, 1/0), alias=nullif2,
- `nullifShouldPassJDBC`: org.junit.ComparisonFailure: expected:<[NULLIF(lastname, 'unknown')]> but was:<[name]>
- `ifWithTrueAndFalseCondition`: java.lang.AssertionError: 
Expected: iterable with items [(name=IF(2 < 0, firstname, lastname), alias=IF0, type=string), (name=IF(2 > 0, firstname, la

### sql.CsvFormatIT
- `sanitizeTest`: java.lang.AssertionError: Line count is different. expected=firstname,lastname
'+Amber JOHnny,Duke Willmington+
'-Hattie,Bond-
'=Nanette,Bates=
'@Dale
- `escapeSanitizeTest`: java.lang.AssertionError: Line count is different. expected=firstname,lastname
+Amber JOHnny,Duke Willmington+
-Hattie,Bond-
=Nanette,Bates=
@Dale,Ada
- `contentHeaderTest`: org.junit.ComparisonFailure: expected:<[application/json]; charset=UTF-8> but was:<[plain/text]; charset=UTF-8>

### sql.DateTimeComparisonIT
- `testCompare {DATE('2026-05-19') = TIME('00:00:00') => true}`: java.lang.AssertionError: 
Expected: iterable with items [[true]] in any order
     but: not matched: <[false]>
- `testCompare {TIME('00:00:00') = DATE('2026-05-19') => true}`: java.lang.AssertionError: 
Expected: iterable with items [[true]] in any order
     but: not matched: <[false]>
- `testCompare {TIME('10:20:30') = TIMESTAMP('2026-05-19 10:20:30') => true}`: java.lang.AssertionError: 
Expected: iterable with items [[true]] in any order
     but: not matched: <[false]>
- `testCompare {TIME('00:00:00') = DATE('2026-05-19') => true #2}`: java.lang.AssertionError: 
Expected: iterable with items [[true]] in any order
     but: not matched: <[false]>
- `testCompare {TIMESTAMP('2026-05-19 10:20:30') = TIME('10:20:30') => true}`: java.lang.AssertionError: 
Expected: iterable with items [[true]] in any order
     but: not matched: <[false]>

### sql.DateTimeFormatsIT
- `testIncompleteFormats`: java.lang.AssertionError: 
Expected: iterable with items [(name=incomplete_1, alias=null, type=timestamp), (name=incomplete_2, alias=null, type=date),
- `testCustomFormats`: java.lang.AssertionError: 
Expected: iterable with items [(name=custom_time, alias=null, type=time), (name=custom_timestamp, alias=null, type=timestam
- `testDateNanosOrderBy`: java.lang.AssertionError: 
Expected: iterable with items [(name=hour_minute_second_OR_t_time, alias=null, type=time)] in any order
     but: not match
- `testDateFormatsWithOr`: java.lang.AssertionError: 
Expected: iterable with items [[1984-04-12 00:00:00], [1984-04-12 09:07:42.000123456]] in any order
     but: not matched: 
- `testNumericFormats`: java.lang.AssertionError: 
Expected: iterable with items [(name=epoch_sec, alias=null, type=timestamp), (name=epoch_milli, alias=null, type=timestamp)

### sql.DateTimeFunctionIT
- `testWeekOfYearUnderscores`: java.lang.AssertionError: 
Expected: iterable with items [[7]] in any order
     but: not matched: <[8]>
- `testDateFormat`: java.lang.AssertionError: 
Expected: iterable with items [(name=date_format(timestamp('1998-01-31 13:14:15.012345'), '%a %b %c %D %d %e %f %H %h %I %i
- `testStrToDate`: java.lang.AssertionError: 
Expected: iterable with items [[2017-10-23 00:00:00], [2017-11-20 00:00:00]] in any order
     but: not matched: <[null]>
- `testDateBracket`: java.lang.AssertionError: 
Expected: iterable with items [(name={date '2020-09-16'}, alias=null, type=date)] in any order
     but: not matched: <{"na
- `testWeek`: java.lang.AssertionError: 
Expected: iterable with items [[7]] in any order
     but: not matched: <[8]>

### sql.DateTimeImplementationIT
- `inRangeZeroNoTZ`: java.lang.AssertionError: 
Expected: iterable with items [(name=DATETIME('2008-01-01 02:00:00'), alias=null, type=timestamp)] in any order
     but: n
- `inRangeZeroNoToTZ`: java.lang.AssertionError: 
Expected: iterable with items [(name=DATETIME('2008-01-01 02:00:00+10:00'), alias=null, type=timestamp)] in any order
     

### sql.ExistsPushdownIT
- `testIsNotNullPushesDownAsExistsQuery`: java.lang.AssertionError: Explain should contain sourceBuilder JSON:
{  "calcite": {    "logical": "LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE
- `testIsNullPushesDownAsMustNotExistsQuery`: java.lang.AssertionError: Explain should contain sourceBuilder JSON:
{  "calcite": {    "logical": "LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE

### sql.JdbcFormatIT
- `testAliasInSchema`: java.lang.AssertionError: 
Expected: iterable with items [(name=account_number, alias=acc, type=long)] in any order
     but: not matched: <{"name":"a
- `testSimpleDataTypesInSchema`: java.lang.AssertionError: 
Expected: iterable with items [(name=account_number, alias=null, type=long), (name=address, alias=null, type=string), (name

### sql.LikeQueryIT
- `test_convert_field_text_to_keyword`: java.lang.AssertionError

### sql.MatchIT
- `missing_quoted_field_test`: java.lang.AssertionError
- `missing_field_test`: java.lang.AssertionError
- `missing_backtick_field_test`: java.lang.AssertionError

### sql.MathematicalFunctionIT
- `testTruncate`: java.lang.AssertionError: 
Expected: iterable with items [(name=truncate(-56, 1), alias=null, type=long)] in any order
     but: not matched: <{"name"
- `testRound`: java.lang.AssertionError: 
Expected: iterable with items [(name=round(-56), alias=null, type=long)] in any order
     but: not matched: <{"name":"roun
- `testCeil`: java.lang.AssertionError: 
Expected: iterable with items [(name=ceil(0), alias=null, type=long)] in any order
     but: not matched: <{"name":"ceil(0)
- `testSign`: java.lang.AssertionError: 
Expected: iterable with items [(name=sign(1.1), alias=null, type=integer)] in any order
     but: not matched: <{"name":"si
- `testSignum`: java.lang.AssertionError: 
Expected: iterable with items [(name=signum(1.1), alias=null, type=integer)] in any order
     but: not matched: <{"name":"

### sql.MultiMatchIT
- `verify_wildcard_test`: java.lang.AssertionError: expected:<10> but was:<0>
- `multimatch_alternate_syntax`: java.lang.AssertionError: expected:<8> but was:<0>
- `multi_match_alternate_syntax`: java.lang.AssertionError: expected:<8> but was:<0>

### sql.NestedIT
- `nested_missing_path`: java.lang.AssertionError
- `nested_function_all_subfields_in_wrong_clause`: java.lang.AssertionError
- `invalid_multiple_nested_all_function_in_a_function_in_select_test`: java.lang.AssertionError
- `nested_missing_path_argument`: java.lang.AssertionError
- `nested_with_non_nested_type_test`: java.lang.AssertionError

### sql.PaginationFallbackIT
- `testWhereClause`: java.lang.AssertionError: 'cursor' property does not exist
- `testLimitOffset`: java.lang.AssertionError: 'cursor' property does not exist
- `testLimit`: java.lang.AssertionError: 'cursor' property does not exist
- `testSelectColumnReference`: java.lang.AssertionError: 'cursor' property does not exist
- `testOrderBy`: java.lang.AssertionError: 'cursor' property does not exist

### sql.PaginationFilterIT
- `test_pagination_with_where {query = SELECT * FROM opensearch-sql_test_index_beer WHERE true, total_hits = 60, page_size = 5}`: java.lang.AssertionError: query: SELECT * FROM opensearch-sql_test_index_beer WHERE true; total hits: 60; page size: 5 || Last page expected:<0> but w
- `test_pagination_with_where {query = SELECT * FROM opensearch-sql_test_index_bank, total_hits = 7, page_size = 5}`: java.lang.AssertionError: query: SELECT * FROM opensearch-sql_test_index_bank; total hits: 7; page size: 5 || Last page expected:<2> but was:<7>
- `test_pagination_with_where {query = SELECT * FROM opensearch-sql_test_index_account WHERE match(address, 'street'), total_hits = 385, page_size = 5}`: java.lang.AssertionError: query: SELECT * FROM opensearch-sql_test_index_account WHERE match(address, 'street'); total hits: 385; page size: 5 || Last
- `test_pagination_with_where {query = SELECT * FROM opensearch-sql_test_index_account, total_hits = 1000, page_size = 1000}`: java.lang.AssertionError: query: SELECT * FROM opensearch-sql_test_index_account; total hits: 1000; page size: 1000 || Last page expected:<0> but was:
- `test_pagination_with_where {query = SELECT * FROM opensearch-sql_test_index_account, total_hits = 1000, page_size = 5}`: java.lang.AssertionError: query: SELECT * FROM opensearch-sql_test_index_account; total hits: 1000; page size: 5 || Last page expected:<0> but was:<10

### sql.PaginationIT
- `testLargeDataSetV2`: java.lang.AssertionError: expected:<4> but was:<9936>
- `testLargeDataSetV2WithWhere`: java.lang.AssertionError: expected:<4> but was:<9936>
- `testSmallDataSet`: java.lang.AssertionError
- `testCloseCursor`: java.lang.AssertionError
- `testAlias`: java.lang.AssertionError: expected:<9936> but was:<10>

### sql.PaginationWindowIT
- `testFetchSizeLargerThanResultWindowFails`: java.lang.AssertionError: expected org.opensearch.client.ResponseException to be thrown, but nothing was thrown
- `testQuerySizeLimitDoesNotEffectPageSize`: java.lang.AssertionError: expected:<4> but was:<6>

### sql.QueryStringIT
- `wildcard_test`: java.lang.AssertionError: expected:<10> but was:<0>

### sql.QueryValidationIT
- `aggregationFunctionInSelectGroupByMultipleFields`: java.lang.AssertionError: 
Expected: (an instance of org.opensearch.client.ResponseException and statusCode is <BAD_REQUEST> and exception with messag
- `testNonAggregatedSelectColumnPresentWithoutGroupByClause`: java.lang.AssertionError: 
Expected: (an instance of org.opensearch.client.ResponseException and statusCode is <BAD_REQUEST> and exception with messag
- `testQueryFieldWithKeyword`: java.lang.AssertionError: 
Expected: (an instance of org.opensearch.client.ResponseException and statusCode is <BAD_REQUEST> and exception with messag

### sql.RawFormatIT
- `rawFormatWithPipeFieldTest`: java.lang.AssertionError: Line count is different. expected=firstname|lastname
+Amber JOHnny|Duke Willmington+
-Hattie|Bond-
=Nanette|Bates=
@Dale|Ada
- `rawFormatPrettyWithPipeFieldTest`: org.junit.ComparisonFailure: expected:<[firstname    |lastname         
+Amber JOHnny|Duke Willmington+
-Hattie      |Bond-            
=Nanette     |
- `contentHeaderTest`: org.junit.ComparisonFailure: expected:<[application/json]; charset=UTF-8> but was:<[plain/text]; charset=UTF-8>

### sql.SQLCorrectnessIT
- `runAllTests`: java.lang.AssertionError: Comparison test failed on queries: {
  "summary": {
    "total": 14,
    "failure": 3,
    "success": 11
  },
  "tests": [
 

### sql.SimpleQueryStringIT
- `verify_wildcard_test`: java.lang.AssertionError: expected:<10> but was:<0>

### sql.SystemFunctionIT
- `typeof_opensearch_types`: java.lang.AssertionError: 
Expected: iterable with items [[DOUBLE, LONG, INTEGER, BYTE, SHORT, FLOAT, FLOAT, DOUBLE]] in any order
     but: not match

### sql.TextFunctionIT
- `testConcat`: java.lang.AssertionError: 
Expected: iterable with items [(name=concat('hello', 'whole', 'world', '!', '!'), alias=null, type=keyword)] in any order
 
- `testConcat_ws`: java.lang.AssertionError: 
Expected: iterable with items [(name=concat_ws(',', 'hello', 'world'), alias=null, type=keyword)] in any order
     but: no
- `testRight`: java.lang.AssertionError: 
Expected: iterable with items [(name=right('variable', 4), alias=null, type=keyword)] in any order
     but: not matched: <
- `testLeft`: java.lang.AssertionError: 
Expected: iterable with items [(name=left('variable', 3), alias=null, type=keyword)] in any order
     but: not matched: <{
- `testRegexp`: java.lang.AssertionError: 
Expected: iterable with items [(name='a' regexp 'b', alias=null, type=integer)] in any order
     but: not matched: <{"name

### sql.VectorSearchIT
- `testNegativeMinScoreRejected`: java.lang.AssertionError: 
Expected: a string containing "min_score"
     but: was "method [POST], host [http://localhost:9200], URI [/_plugins/_sql?f
- `testWildcardTableRejectedWithDedicatedMessage`: java.lang.AssertionError: 
Expected: a string containing "Invalid table name"
     but: was "method [POST], host [http://localhost:9200], URI [/_plugi
- `testMutualExclusivityRejectsKAndMaxDistance`: java.lang.AssertionError: 
Expected: a string containing "Only one of"
     but: was "method [POST], host [http://localhost:9200], URI [/_plugins/_sql
- `testDuplicateNamedArgRejected`: java.lang.AssertionError: 
Expected: a string containing "Duplicate argument name"
     but: was "method [POST], host [http://localhost:9200], URI [/_
- `testInvalidFilterTypeRejects`: java.lang.AssertionError: 
Expected: a string containing "filter_type must be one of"
     but: was "method [POST], host [http://localhost:9200], URI 

### sql.VectorSearchSubqueryIT
- `testOuterDistinctOnSubqueryRejected`: java.lang.AssertionError: 
Expected: a string containing "Outer GROUP BY / aggregation / DISTINCT on a vectorSearch() subquery is not supported"
     
- `testOuterWhereOnSubqueryRejected`: java.lang.AssertionError: 
Expected: a string containing "Outer WHERE on a vectorSearch() subquery is not supported"
     but: was "method [POST], hos
- `testOuterWhereWithInnerWhereStillRejected`: java.lang.AssertionError: 
Expected: a string containing "Outer WHERE on a vectorSearch() subquery is not supported"
     but: was "method [POST], hos
- `testOuterAggregationOnSubqueryRejected`: java.lang.AssertionError: 
Expected: a string containing "Outer GROUP BY / aggregation / DISTINCT on a vectorSearch() subquery is not supported"
     
- `testOuterGroupByOnSubqueryRejected`: java.lang.AssertionError: 
Expected: a string containing "Outer GROUP BY / aggregation / DISTINCT on a vectorSearch() subquery is not supported"
     

### sql.WildcardQueryIT
- `test_escaping_wildcard_percent_in_text`: java.lang.AssertionError: expected:<4> but was:<0>
- `test_wildcard_query_sql_wildcard_percent_conversion`: java.lang.AssertionError: expected:<8> but was:<0>
- `test_wildcard_query_sql_wildcard_underscore_conversion`: java.lang.AssertionError: expected:<7> but was:<0>
- `test_backslash_wildcard`: java.lang.AssertionError: expected:<1> but was:<0>

### sql.WindowFunctionIT
- `testDistinctCountOverNull`: java.lang.AssertionError: 
Expected: iterable with items [[Duke Willmington, 2], [Bond, 2], [Bates, 2], [Adams, 2], [Ratliff, 2], [Ayala, 2], [Mcphers
