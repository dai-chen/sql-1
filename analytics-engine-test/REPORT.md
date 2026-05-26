# SQL V2 + Legacy IT — Analytics Engine Compatibility Report

**Total Tests:** 1388 | ✅ Passed: 332 (23.9%) | ❌ Failed: 385 (27.7%) | ⏭️ Skipped: 671 (48.3%)


| Area | Passed | Failed | Skipped | Pass Rate |
|------|-------:|-------:|--------:|----------:|
| SQL V2 (`sql.sql.*`) | 275 | 319 | 158 | 46.3% |
| Legacy (`sql.legacy.*`) | 57 | 66 | 513 | 46.3% |

## Failure Categories

| Category | Count | % |
|----------|------:|--:|
| Other Error | 289 | 75.1% |
| Result Mismatch | 96 | 24.9% |

## ✅ Fully Passing Test Classes

- **sql.ArithmeticFunctionIT** (11 tests)
- **legacy.PluginIT** (7 tests)
- **sql.StringLiteralIT** (5 tests)
- **legacy.JoinTimeoutHintIT** (2 tests)
- **legacy.GetEndpointQueryIT** (1 tests)
- **sql.MetricsIT** (1 tests)
- **sql.SQLConcurrencyIT** (1 tests)

## 🟡 Partially Passing (>50%)

| Class | Passed | Failed | Rate |
|-------|-------:|-------:|-----:|
| legacy.JoinAliasWriterRuleIT | 10 | 1 | 91% |
| sql.LikeQueryIT | 10 | 1 | 91% |
| sql.DateTimeComparisonIT | 166 | 25 | 87% |
| sql.LegacyAPICompatibilityIT | 6 | 1 | 86% |
| sql.PositionFunctionIT | 6 | 1 | 86% |
| sql.MathematicalFunctionIT | 16 | 6 | 73% |
| legacy.TypeInformationIT | 9 | 4 | 69% |
| sql.NowLikeFunctionIT | 8 | 4 | 67% |
| legacy.OrdinalAliasRewriterIT | 7 | 4 | 64% |
| sql.TextFunctionIT | 8 | 7 | 53% |

## ❌ Failing Test Classes

| Class | Passed | Failed | Skipped | Top Failure |
|-------|-------:|-------:|--------:|-------------|
| sql.DateTimeFunctionIT | 6 | 68 | 0 | Other Error |
| sql.AggregationIT | 20 | 34 | 0 | Other Error |
| sql.WildcardQueryIT | 0 | 18 | 0 | Other Error |
| sql.ConvertTZFunctionIT | 0 | 17 | 0 | Other Error |
| sql.DateTimeImplementationIT | 0 | 15 | 0 | Other Error |
| legacy.AggregationExpressionIT | 2 | 14 | 0 | Result Mismatch |
| legacy.PrettyFormatResponseIT | 12 | 14 | 4 | Other Error |
| sql.MatchIT | 0 | 14 | 0 | Other Error |
| sql.MultiMatchIT | 0 | 12 | 0 | Other Error |
| legacy.MetaDataQueriesIT | 0 | 11 | 2 | Other Error |
| sql.DateTimeFormatsIT | 1 | 10 | 0 | Result Mismatch |
| sql.IdentifierIT | 0 | 10 | 0 | Other Error |
| sql.ConditionalIT | 2 | 8 | 2 | Result Mismatch |
| sql.MatchPhraseIT | 0 | 8 | 0 | Other Error |
| sql.MatchPhrasePrefixIT | 0 | 8 | 0 | Other Error |
| sql.PaginationIT | 1 | 7 | 1 | Other Error |
| legacy.ObjectFieldSelectIT | 0 | 6 | 0 | Other Error |
| sql.RelevanceFunctionIT | 0 | 6 | 0 | Other Error |
| sql.WindowFunctionIT | 2 | 6 | 0 | Result Mismatch |
| sql.QueryIT | 0 | 5 | 0 | Other Error |
| legacy.JdbcTestIT | 4 | 4 | 4 | Other Error |
| sql.ComplexTimestampQueryIT | 1 | 4 | 1 | Other Error |
| sql.QueryStringIT | 0 | 4 | 0 | Other Error |
| sql.SimpleQueryStringIT | 0 | 4 | 0 | Other Error |
| legacy.MalformedQueryIT | 0 | 3 | 0 | Other Error |
| legacy.MethodQueryIT | 2 | 3 | 2 | Other Error |
| sql.MatchBoolPrefixIT | 0 | 3 | 0 | Other Error |
| sql.NullLiteralIT | 1 | 3 | 0 | Other Error |
| sql.QueryValidationIT | 1 | 3 | 1 | Result Mismatch |
| legacy.SqlLegacyEngineSanityIT | 1 | 2 | 0 | Other Error |
| sql.GeopointFormatsIT | 0 | 2 | 0 | Other Error |
| sql.SystemFunctionIT | 0 | 2 | 0 | Other Error |
| sql.JdbcFormatIT | 1 | 1 | 0 | Result Mismatch |
| sql.SQLCorrectnessIT | 0 | 1 | 0 | Result Mismatch |
| sql.StandalonePaginationIT | 1 | 1 | 0 | Other Error |

## 🔍 Result Mismatch Details

### legacy.AggregationExpressionIT
- `hasGroupKeyMaxAddMinShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=gender, alias=null, type=string), (name=MAX(age) + MIN(age), alias=addValue, type=long
- `logWithAddLiteralOnGroupKeyAndMaxSubtractLiteralShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=gender, alias=null, type=string), (name=Log(age+10), alias=logAge, type=double), (name
- `AddLiteralOnGroupKeyShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=gender, alias=null, type=string), (name=age+10, alias=null, type=long), (name=max(bala
- `aggregateCastStatementShouldNotReturnZero`: java.lang.AssertionError: 
Expected: iterable with items [(name=SUM(CAST(male AS INT)), alias=male_sum, type=integer)] in any order
     but: not matc
- `noGroupKeyAvgOnIntegerShouldPass`: java.lang.AssertionError: 
Expected: iterable with items [(name=AVG(age), alias=avg, type=double)] in any order
     but: not matched: <{"name":"AVG(a

### legacy.JoinAliasWriterRuleIT
- `sameTablesNoAliasWithTableNameAsAliasOnColumns`: java.lang.AssertionError: Expected test to throw (an instance of org.opensearch.client.ResponseException and exception with message a string containin

### legacy.OrdinalAliasRewriterIT
- `multipleGroupByOrdinal`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"lastname\",\n      \"type\": \"string\"\n    },\n    {\n      \"nam
- `selectFieldiWithBacticksAndTableAliasOrderByOrdinalAndNull`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"b`.`lastname\",\n      \"type\": \"string\"\n    },\n    {\n      \
- `selectFieldiWithBacticksAndTableAliasGroupByOrdinal`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"b`.`lastname\",\n      \"type\": \"string\"\n    },\n    {\n      \
- `selectFieldiWithBacticksGroupByOrdinal`: java.lang.AssertionError: 
Expected: "{\n  \"schema\": [\n    {\n      \"name\": \"lastname\",\n      \"type\": \"string\"\n    }\n  ],\n  \"datarows\

### legacy.PrettyFormatResponseIT
- `testSizeWithGroupBy`: java.lang.AssertionError: 
Expected: <5>
     but: was <21>
- `fieldsWithAlias`: java.lang.AssertionError
- `joinQueryWithAlias`: java.lang.AssertionError

### legacy.TypeInformationIT
- `testAddWithIntReturnsInt`: java.lang.AssertionError: 
Expected: iterable with items [(name=(balance + 5), alias=balance_add_five, type=long)] in any order
     but: not matched:

### sql.AggregationIT
- `testMaxTimeInMemory`: java.lang.AssertionError: 
Expected: iterable with items [(name=max(time1) OVER(PARTITION BY datetime1), alias=null, type=time)] in any order
     but
- `testMaxDateInMemory`: java.lang.AssertionError: 
Expected: iterable with items [(name=max(date0) OVER(PARTITION BY datetime1), alias=null, type=date)] in any order
     but
- `testFilteredAggregateNotPushDown`: java.lang.AssertionError: 
Expected: iterable with items [[3]] in any order
     but: not matched: <[7]>
- `testMaxDatePushedDown`: java.lang.AssertionError: 
Expected: iterable with items [(name=max(date0), alias=null, type=date)] in any order
     but: not matched: <{"name":"max(
- `testMinDateTimePushedDown`: java.lang.AssertionError: 
Expected: iterable with items [(name=min(timestamp(CAST(time0 AS STRING))), alias=null, type=timestamp)] in any order
     

### sql.ComplexTimestampQueryIT
- `nonJoinTimestampFieldsSchema`: org.junit.ComparisonFailure: expected:<[timestamp]> but was:<[string]>
- `selectDatetimeWithoutNested`: org.junit.ComparisonFailure: expected:<[timestamp]> but was:<[string]>

### sql.ConditionalIT
- `ifnullWithMissingInputTest`: java.lang.AssertionError: 
Expected: iterable with items [(name=IFNULL(balance, 100), alias=IFNULL1, type=long), (name=IFNULL(200, balance), alias=IFN
- `isnullWithNullInputTest`: java.lang.AssertionError: 
Expected: iterable with items [(name=ISNULL(1/0), alias=ISNULL1, type=boolean), (name=ISNULL(firstname), alias=ISNULL2, typ
- `ifShouldPassJDBC`: java.lang.AssertionError: expected:<name> but was:<null>
- `nullifWithNullInputTest`: java.lang.AssertionError: 
Expected: iterable with items [(name=NULLIF(1/0, 123), alias=nullif1, type=integer), (name=NULLIF(123, 1/0), alias=nullif2,
- `nullifShouldPassJDBC`: java.lang.AssertionError: expected:<name> but was:<null>

### sql.DateTimeFormatsIT
- `testReadingDateFormats`: java.lang.AssertionError: 
Expected: iterable with items [(name=weekyear_week_day, alias=null, type=date), (name=hour_minute_second_millis, alias=null
- `testCustomFormats`: java.lang.AssertionError: 
Expected: iterable with items [(name=custom_time, alias=null, type=time), (name=custom_timestamp, alias=null, type=timestam
- `testDateFormatsWithOr`: java.lang.AssertionError: 
Expected: iterable with items [[1984-04-12 00:00:00], [1984-04-12 09:07:42.000123456]] in any order
     but: not matched: 
- `testNumericFormats`: java.lang.AssertionError: 
Expected: iterable with items [(name=epoch_sec, alias=null, type=timestamp), (name=epoch_milli, alias=null, type=timestamp)
- `testIncompleteFormats`: java.lang.AssertionError: 
Expected: iterable with items [(name=incomplete_1, alias=null, type=timestamp), (name=incomplete_2, alias=null, type=date),

### sql.DateTimeFunctionIT
- `testUnixTimeStamp`: java.lang.AssertionError: 
Expected: iterable with items [(name=UNIX_TIMESTAMP(MAKEDATE(1984, 1984)), alias=f1, type=double), (name=UNIX_TIMESTAMP(TIM
- `testWeekOfYear`: java.lang.AssertionError: 
Expected: iterable with items [[7]] in any order
     but: not matched: <[8]>
- `testWeek`: java.lang.AssertionError: 
Expected: iterable with items [[7]] in any order
     but: not matched: <[8]>
- `testTimeFormat`: java.lang.AssertionError: 
Expected: iterable with items [(name=time_format(timestamp('1998-01-31 13:14:15.012345'), '%f %H %h %I %i %p %r %S %s %T'),
- `testMakeDate`: java.lang.AssertionError: 
Expected: iterable with items [(name=MAKEDATE(1945, 5.9), alias=f1, type=date), (name=MAKEDATE(1984, 1984), alias=f2, type=

### sql.DateTimeImplementationIT
- `inRangeZeroNoToTZ`: java.lang.AssertionError: 
Expected: iterable with items [(name=DATETIME('2008-01-01 02:00:00+10:00'), alias=null, type=timestamp)] in any order
     
- `inRangeZeroNoTZ`: java.lang.AssertionError: 
Expected: iterable with items [(name=DATETIME('2008-01-01 02:00:00'), alias=null, type=timestamp)] in any order
     but: n

### sql.JdbcFormatIT
- `testSimpleDataTypesInSchema`: java.lang.AssertionError: 
Expected: iterable with items [(name=account_number, alias=null, type=long), (name=address, alias=null, type=string), (name

### sql.LikeQueryIT
- `test_convert_field_text_to_keyword`: java.lang.AssertionError

### sql.MatchIT
- `missing_backtick_field_test`: java.lang.AssertionError
- `missing_field_test`: java.lang.AssertionError
- `missing_quoted_field_test`: java.lang.AssertionError

### sql.MathematicalFunctionIT
- `testTruncate`: java.lang.AssertionError: 
Expected: iterable with items [(name=truncate(-56, 1), alias=null, type=long)] in any order
     but: not matched: <{"name"
- `testRound`: java.lang.AssertionError: 
Expected: iterable with items [(name=round(-56), alias=null, type=long)] in any order
     but: not matched: <{"name":"roun
- `testSignum`: java.lang.AssertionError: 
Expected: iterable with items [(name=signum(1.1), alias=null, type=integer)] in any order
     but: not matched: <{"name":"
- `testSign`: java.lang.AssertionError: 
Expected: iterable with items [(name=sign(1.1), alias=null, type=integer)] in any order
     but: not matched: <{"name":"si
- `testCeil`: java.lang.AssertionError: 
Expected: iterable with items [(name=ceil(0), alias=null, type=long)] in any order
     but: not matched: <{"name":"ceil(0)

### sql.PaginationIT
- `testLargeDataSetV2`: java.lang.AssertionError: expected:<4> but was:<9936>
- `testCloseCursor`: java.lang.AssertionError
- `testAlias`: java.lang.AssertionError: expected:<9936> but was:<10>
- `testQueryWithOrderBy`: java.lang.AssertionError
- `testLargeDataSetV2WithWhere`: java.lang.AssertionError: expected:<4> but was:<9936>

### sql.QueryValidationIT
- `testQueryFieldWithKeyword`: java.lang.AssertionError: 
Expected: (an instance of org.opensearch.client.ResponseException and statusCode is <BAD_REQUEST> and exception with messag
- `aggregationFunctionInSelectGroupByMultipleFields`: java.lang.AssertionError: 
Expected: (an instance of org.opensearch.client.ResponseException and statusCode is <BAD_REQUEST> and exception with messag
- `testNonAggregatedSelectColumnPresentWithoutGroupByClause`: java.lang.AssertionError: 
Expected: (an instance of org.opensearch.client.ResponseException and statusCode is <BAD_REQUEST> and exception with messag

### sql.SQLCorrectnessIT
- `runAllTests`: java.lang.AssertionError: Comparison test failed on queries: {
  "summary": {
    "total": 14,
    "success": 11,
    "failure": 3
  },
  "tests": [
 

### sql.SystemFunctionIT
- `typeof_opensearch_types`: java.lang.AssertionError: 
Expected: iterable with items [[DOUBLE, LONG, INTEGER, BYTE, SHORT, FLOAT, FLOAT, DOUBLE]] in any order
     but: not match

### sql.TextFunctionIT
- `testConcat`: java.lang.AssertionError: 
Expected: iterable with items [(name=concat('hello', 'whole', 'world', '!', '!'), alias=null, type=keyword)] in any order
 
- `testSubstring`: java.lang.AssertionError: 
Expected: iterable with items [(name=substring('hello', 2), alias=null, type=keyword)] in any order
     but: not matched: 
- `testConcat_ws`: java.lang.AssertionError: 
Expected: iterable with items [(name=concat_ws(',', 'hello', 'world'), alias=null, type=keyword)] in any order
     but: no
- `testRight`: java.lang.AssertionError: 
Expected: iterable with items [(name=right('variable', 4), alias=null, type=keyword)] in any order
     but: not matched: <
- `testRegexp`: java.lang.AssertionError: 
Expected: iterable with items [(name='a' regexp 'b', alias=null, type=integer)] in any order
     but: not matched: <{"name

### sql.WindowFunctionIT
- `testDistinctCountOver`: java.lang.AssertionError: 
Expected: iterable containing [[Adams, 1], [Ayala, 2], [Bates, 2], [Bond, 2], [Duke Willmington, 2], [Mcpherson, 2], [Ratli
- `testDistinctCountPartition`: java.lang.AssertionError: 
Expected: iterable containing [[Ayala, 1], [Bates, 1], [Mcpherson, 1], [Adams, 1], [Bond, 1], [Duke Willmington, 1], [Ratli
- `testDistinctCountOverNull`: java.lang.AssertionError: 
Expected: iterable with items [[Duke Willmington, 2], [Bond, 2], [Bates, 2], [Adams, 2], [Ratliff, 2], [Ayala, 2], [Mcphers
