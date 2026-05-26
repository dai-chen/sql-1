# Analytics Engine SQL Feature Compatibility Report (May 27)

Summary: 130/179 passed (72%)

## SQL Statements

| Category | Passed | Failed | Total |
|----------|--------|--------|-------|
| SELECT | 6 | 0 | 6 |
| WHERE | 5 | 0 | 5 |
| ORDER BY | 3 | 0 | 3 |
| LIMIT / OFFSET | 3 | 0 | 3 |
| GROUP BY | 3 | 0 | 3 |
| HAVING | 1 | 0 | 1 |
| CASE | 2 | 0 | 2 |
| LIKE / REGEXP | 3 | 0 | 3 |
| Set Operations | 0 | 3 | 3 |
| JOIN | 4 | 0 | 4 |
| Subqueries | 1 | 2 | 3 |
| EXPLAIN | 2 | 0 | 2 |
| SHOW / DESCRIBE | 0 | 2 | 2 |

## SQL Functions

| Family | Total | Pass | Fail | Coverage |
|--------|-------|------|------|----------|
| String | 18 | 18 | 0 | 100% |
| Math | 32 | 30 | 2 | 93% |
| DateTime | 54 | 20 | 34 | 37% |
| Conditional | 4 | 3 | 1 | 75% |
| Type Conversion | 5 | 4 | 1 | 80% |
| Aggregate | 14 | 12 | 2 | 85% |
| Window | 3 | 1 | 2 | 33% |
| Relevance | 9 | 9 | 0 | 100% |
| **Total** | **139** | **97** | **42** | **69%** |

## Results: SQL Statements

| Category | Name | Query | Status | Result / Error |
|----------|------|-------|--------|----------------|
| SELECT | select_where | SELECT * FROM sanity_test WHERE age > 30 | PASS | [32, "Seattle", 1, "Alice"], [35, "Portland", 2, "Dave"] |
| SELECT | select_star | SELECT * FROM sanity_test | PASS | [32, "Seattle", 1, "Alice"], [25, "Portland", 2, "Bob"], [28, "Seattle", 1, "Carol"], [35, "Portl... |
| SELECT | select_expression | SELECT name, age * 2 AS double_age, age + 10 AS age_plus FROM sanity_test | PASS | ["Alice", 64, 42], ["Bob", 50, 35], ["Carol", 56, 38], ["Dave", 70, 45], ["Eve", 44, 32] |
| SELECT | select_distinct | SELECT DISTINCT city FROM sanity_test | PASS | ["Seattle"], ["Portland"], ["Denver"] |
| SELECT | select_alias | SELECT name AS employee_name, city AS location FROM sanity_test | PASS | ["Alice", "Seattle"], ["Bob", "Portland"], ["Carol", "Seattle"], ["Dave", "Portland"], ["Eve", "D... |
| SELECT | select_literal | SELECT 1 + 1 AS two, 'hello' AS greeting | PASS | [2, "hello"] |
| WHERE | where_in | SELECT name, city FROM sanity_test WHERE city IN ('Seattle', 'Denver') | PASS | ["Alice", "Seattle"], ["Carol", "Seattle"], ["Eve", "Denver"] |
| WHERE | where_between | SELECT name, age FROM sanity_test WHERE age BETWEEN 25 AND 32 | PASS | ["Alice", 32], ["Bob", 25], ["Carol", 28] |
| WHERE | where_and_or | SELECT name FROM sanity_test WHERE (city = 'Seattle' OR city = 'Denver') AND ... | PASS | ["Alice"], ["Carol"] |
| WHERE | where_not | SELECT name FROM sanity_test WHERE NOT city = 'Portland' | PASS | ["Alice"], ["Carol"], ["Eve"] |
| WHERE | where_is_null | SELECT name FROM sanity_test WHERE city IS NOT NULL | PASS | ["Alice"], ["Bob"], ["Carol"], ["Dave"], ["Eve"] |
| ORDER BY | order_by_limit | SELECT name, age FROM sanity_test ORDER BY age DESC LIMIT 3 | PASS | ["Dave", 35], ["Alice", 32], ["Carol", 28] |
| ORDER BY | order_by_multi | SELECT name, city, age FROM sanity_test ORDER BY city ASC, age DESC | PASS | ["Eve", "Denver", 22], ["Dave", "Portland", 35], ["Bob", "Portland", 25], ["Alice", "Seattle", 32... |
| ORDER BY | order_by_asc | SELECT name, age FROM sanity_test ORDER BY age ASC LIMIT 3 | PASS | ["Eve", 22], ["Bob", 25], ["Carol", 28] |
| LIMIT / OFFSET | limit_only | SELECT name, age FROM sanity_test ORDER BY age LIMIT 2 | PASS | ["Eve", 22], ["Bob", 25] |
| LIMIT / OFFSET | offset_skip | SELECT name, age FROM sanity_test ORDER BY age LIMIT 2 OFFSET 2 | PASS | ["Carol", 28], ["Alice", 32] |
| LIMIT / OFFSET | offset_zero | SELECT name FROM sanity_test ORDER BY name LIMIT 3 OFFSET 0 | PASS | ["Alice"], ["Bob"], ["Carol"] |
| GROUP BY | group_by_sum_count | SELECT city, SUM(age), COUNT(*) FROM sanity_test GROUP BY city | PASS | ["Denver", 22, 1], ["Seattle", 60, 2], ["Portland", 60, 2] |
| GROUP BY | group_by_max | SELECT city, MAX(age) FROM sanity_test GROUP BY city | PASS | ["Seattle", 32], ["Portland", 35], ["Denver", 22] |
| GROUP BY | filtered_agg | SELECT city, SUM(age) FILTER(WHERE age > 25) FROM sanity_test GROUP BY city | PASS | ["Denver", 22], ["Seattle", 60], ["Portland", 60] |
| HAVING | group_by_having | SELECT city, COUNT(*) AS cnt FROM sanity_test GROUP BY city HAVING cnt > 1 | PASS | ["Seattle", 2], ["Portland", 2] |
| CASE | case_simple | SELECT name, CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END AS level FROM... | PASS | ["Alice", "senior"], ["Bob", "junior"], ["Carol", "junior"], ["Dave", "senior"], ["Eve", "junior"] |
| CASE | case_in_agg | SELECT city, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS senior_count FROM s... | PASS | ["Seattle", 1], ["Portland", 1], ["Denver", 0] |
| LIKE / REGEXP | like_prefix | SELECT name FROM sanity_test WHERE city LIKE 'Sea%' | PASS | ["Alice"], ["Carol"] |
| LIKE / REGEXP | like_contains | SELECT name FROM sanity_test WHERE city LIKE '%ort%' | PASS | ["Bob"], ["Dave"] |
| LIKE / REGEXP | like_negated | SELECT name FROM sanity_test WHERE NOT city LIKE 'Sea%' | PASS | ["Bob"], ["Dave"], ["Eve"] |
| Set Operations | union_basic | SELECT name, age FROM sanity_test WHERE city = 'Seattle' UNION SELECT name, a... | FAIL | HTTP 400: [UNION] is not a valid term at this part of the query: '...RE city = 'Seattle' UNION' <--  |
| Set Operations | union_all_basic | SELECT name, age FROM sanity_test WHERE city = 'Seattle' UNION ALL SELECT nam... | FAIL | HTTP 400: [UNION] is not a valid term at this part of the query: '...RE city = 'Seattle' UNION' <--  |
| Set Operations | minus_basic | SELECT name FROM sanity_test WHERE city = 'Seattle' MINUS SELECT name FROM sa... | FAIL | HTTP 400: [MINUS] is not a valid term at this part of the query: '...RE city = 'Seattle' MINUS' <--  |
| JOIN | inner_join | SELECT a.name, b.dept FROM sanity_test a JOIN sanity_dept b ON a.dept_id = b.id | PASS | ["Alice", "Engineering"], ["Carol", "Engineering"], ["Eve", "Engineering"], ["Bob", "Marketing"],... |
| JOIN | left_join | SELECT a.name, b.dept FROM sanity_test a LEFT JOIN sanity_dept b ON a.dept_id... | PASS | ["Alice", "Engineering"], ["Carol", "Engineering"], ["Eve", "Engineering"], ["Bob", "Marketing"],... |
| JOIN | right_join | SELECT a.name, b.dept FROM sanity_test a RIGHT JOIN sanity_dept b ON a.dept_i... | PASS | ["Alice", "Engineering"], ["Bob", "Marketing"], ["Carol", "Engineering"], ["Dave", "Marketing"], ... |
| JOIN | cross_join | SELECT a.name, b.dept FROM sanity_test a CROSS JOIN sanity_dept b LIMIT 3 | PASS | ["Alice", "Engineering"], ["Alice", "Marketing"], ["Bob", "Engineering"] |
| Subqueries | subquery_where | SELECT name FROM sanity_test WHERE dept_id IN (SELECT id FROM sanity_dept WHE... | FAIL | HTTP 500: Failed to create exchange sink for stageId=2 |
| Subqueries | subquery_from | SELECT t.city, t.total FROM (SELECT city, COUNT(*) AS total FROM sanity_test ... | PASS | ["Seattle", 2], ["Portland", 2], ["Denver", 1] |
| Subqueries | exists_subquery | SELECT name FROM sanity_test a WHERE EXISTS (SELECT 1 FROM sanity_dept b WHER... | FAIL | HTTP 500: Failed to create exchange sink for stageId=2 |
| EXPLAIN | explain_select | SELECT name, age FROM sanity_test WHERE age > 25 | PASS | (plan output) |
| EXPLAIN | explain_join | SELECT a.name, b.dept FROM sanity_test a JOIN sanity_dept b ON a.dept_id = b.id | PASS | (plan output) |
| SHOW / DESCRIBE | show_tables | SHOW TABLES LIKE 'sanity%' | FAIL | HTTP 500: Failed to plan query: unexpected error |
| SHOW / DESCRIBE | describe_table | DESCRIBE TABLES LIKE 'sanity_test' COLUMNS LIKE '%' | FAIL | HTTP 500: Failed to plan query: unexpected error |

## Results: SQL Functions

| Family | Function | Query | Status | Result / Error |
|--------|----------|-------|--------|----------------|
| String | UPPER | SELECT UPPER('hello') | PASS | [["HELLO"]] |
| String | LOWER | SELECT LOWER('WORLD') | PASS | [["world"]] |
| String | LENGTH | SELECT LENGTH('test') | PASS | [[4]] |
| String | CONCAT | SELECT CONCAT('hello', ' ', 'world') | PASS | [["hello world"]] |
| String | SUBSTRING | SELECT SUBSTRING('hello', 2, 3) | PASS | [["ell"]] |
| String | TRIM | SELECT TRIM('  hello  ') | PASS | [["hello"]] |
| String | REPLACE | SELECT REPLACE('hello world', 'world', 'there') | PASS | [["hello there"]] |
| String | LEFT | SELECT LEFT('hello', 3) | PASS | [["hel"]] |
| String | RIGHT | SELECT RIGHT('hello', 3) | PASS | [["llo"]] |
| String | LOCATE | SELECT LOCATE('l', 'hello') | PASS | [[3]] |
| String | REVERSE | SELECT REVERSE('hello') | PASS | [["olleh"]] |
| String | LTRIM | SELECT LTRIM('  hello') | PASS | [["hello"]] |
| String | RTRIM | SELECT RTRIM('hello  ') | PASS | [["hello"]] |
| String | ASCII | SELECT ASCII('A') | PASS | [[65]] |
| String | CONCAT_WS | SELECT CONCAT_WS(',', 'a', 'b', 'c') | PASS | [["a,b,c"]] |
| String | STRCMP | SELECT STRCMP('a', 'b') | PASS | [[-1]] |
| String | POSITION | SELECT POSITION('l' IN 'hello') | PASS | [[3]] |
| String | SUBSTR | SELECT SUBSTR('hello', 2, 3) | PASS | [["ell"]] |
| Math | ABS | SELECT ABS(-5) | PASS | [[5]] |
| Math | CEIL | SELECT CEIL(3.2) | PASS | [[4]] |
| Math | FLOOR | SELECT FLOOR(3.8) | PASS | [[3]] |
| Math | ROUND | SELECT ROUND(3.56, 1) | PASS | [[3.6]] |
| Math | MOD | SELECT MOD(10, 3) | PASS | [[1]] |
| Math | SQRT | SELECT SQRT(16) | PASS | [[4]] |
| Math | POW | SELECT POW(2, 3) | PASS | [[8]] |
| Math | LOG | SELECT LOG(E()) | PASS | [[1]] |
| Math | LOG2 | SELECT LOG2(8) | PASS | [[3]] |
| Math | LOG10 | SELECT LOG10(100) | PASS | [[2]] |
| Math | EXP | SELECT EXP(0) | PASS | [[1]] |
| Math | SIGN | SELECT SIGN(-5) | PASS | [[-1]] |
| Math | TRUNCATE | SELECT TRUNCATE(3.789, 2) | PASS | [[3.78]] |
| Math | RAND | SELECT RAND() IS NOT NULL AS ok | PASS | [[true]] |
| Math | PI | SELECT PI() > 3.14 AS ok | PASS | [[true]] |
| Math | DEGREES | SELECT DEGREES(0) | PASS | [[0]] |
| Math | RADIANS | SELECT RADIANS(0) | PASS | [[0]] |
| Math | LN | SELECT LN(1) | PASS | [[0]] |
| Math | CBRT | SELECT CBRT(27) | PASS | [[3]] |
| Math | RINT | SELECT RINT(3.5) | PASS | [[4]] |
| Math | ATAN | SELECT ATAN(0) | PASS | [[0]] |
| Math | ATAN2 | SELECT ATAN2(0, 1) | PASS | [[0]] |
| Math | COS | SELECT COS(0) | PASS | [[1]] |
| Math | SIN | SELECT SIN(0) | PASS | [[0]] |
| Math | TAN | SELECT TAN(0) | FAIL | HTTP 500: Failed to plan query: unexpected error |
| Math | COT | SELECT COT(1) IS NOT NULL AS ok | PASS | [[true]] |
| Math | COSH | SELECT COSH(0) | PASS | [[1]] |
| Math | SINH | SELECT SINH(0) | PASS | [[0]] |
| Math | CONV | SELECT CONV(10, 10, 2) | FAIL | HTTP 500: Execution error: This feature is not implemented: Unsupported function |
| Math | CRC32 | SELECT CRC32('hello') IS NOT NULL AS ok | PASS | [[true]] |
| Math | E | SELECT E() > 2.71 AS ok | PASS | [[true]] |
| Math | EXPM1 | SELECT EXPM1(0) | PASS | [[0]] |
| DateTime | NOW | SELECT NOW() IS NOT NULL AS ok | PASS | [[true]] |
| DateTime | CURDATE | SELECT CURDATE() IS NOT NULL AS ok | PASS | [[true]] |
| DateTime | CURTIME | SELECT CURTIME() IS NOT NULL AS ok | PASS | [[true]] |
| DateTime | DATE | SELECT DATE('2024-03-15 10:30:00') | FAIL | Result mismatch (ordered). Expected: [["2024-03-15"]] Got: [[19797]] |
| DateTime | TIME | SELECT TIME('2024-03-15 10:30:00') | FAIL | HTTP 500: java.lang.RuntimeException: Execution error: Execution error: Error pa |
| DateTime | YEAR | SELECT YEAR(DATE('2024-03-15')) | PASS | [[2024]] |
| DateTime | MONTH | SELECT MONTH(DATE('2024-03-15')) | PASS | [[3]] |
| DateTime | DAY | SELECT DAY(DATE('2024-03-15')) | PASS | [[15]] |
| DateTime | HOUR | SELECT HOUR(TIME('10:30:00')) | FAIL | HTTP 400: Unable to convert call DATE_PART(string, precision_time<9>?). |
| DateTime | MINUTE | SELECT MINUTE(TIME('10:30:00')) | FAIL | HTTP 400: Unable to convert call DATE_PART(string, precision_time<9>?). |
| DateTime | SECOND | SELECT SECOND(TIME('10:30:45')) | FAIL | HTTP 400: Unable to convert call DATE_PART(string, precision_time<9>?). |
| DateTime | DAYOFWEEK | SELECT DAYOFWEEK(DATE('2024-03-15')) | PASS | [[6]] |
| DateTime | DAYOFYEAR | SELECT DAYOFYEAR(DATE('2024-03-15')) | PASS | [[75]] |
| DateTime | WEEK | SELECT WEEK(DATE('2024-03-15')) | PASS | [[11]] |
| DateTime | QUARTER | SELECT QUARTER(DATE('2024-03-15')) | PASS | [[1]] |
| DateTime | DATE_FORMAT | SELECT DATE_FORMAT('2024-01-15', '%Y-%m') | PASS | [["2024-01"]] |
| DateTime | DATE_ADD | SELECT DATE_ADD(DATE('2024-03-15'), INTERVAL 1 DAY) | FAIL | HTTP 500: No backend supports scalar function [DATE_ADD] among [datafusion] |
| DateTime | DATE_SUB | SELECT DATE_SUB(DATE('2024-03-15'), INTERVAL 1 DAY) | FAIL | HTTP 500: No backend supports scalar function [DATE_SUB] among [datafusion] |
| DateTime | DATEDIFF | SELECT DATEDIFF(DATE('2024-03-15'), DATE('2024-03-10')) | FAIL | HTTP 500: No backend supports scalar function [DATEDIFF] among [datafusion] |
| DateTime | TIMESTAMPDIFF | SELECT TIMESTAMPDIFF(DAY, '2024-03-10', '2024-03-15') | FAIL | HTTP 400: Unable to convert call TIMESTAMPDIFF(string, string, string). |
| DateTime | TIMESTAMPADD | SELECT TIMESTAMPADD(DAY, 1, '2024-03-15') | FAIL | HTTP 400: Unable to convert call TIMESTAMPADD(string, i32, string). |
| DateTime | UNIX_TIMESTAMP | SELECT UNIX_TIMESTAMP('2024-01-01 00:00:00') > 0 AS ok | PASS | [[true]] |
| DateTime | FROM_UNIXTIME | SELECT FROM_UNIXTIME(0) | PASS | [["1970-01-01 00:00:00"]] |
| DateTime | STR_TO_DATE | SELECT STR_TO_DATE('2024-03-15', '%Y-%m-%d') | PASS | [["2024-03-15 00:00:00"]] |
| DateTime | ADDDATE | SELECT ADDDATE(DATE('2024-03-15'), INTERVAL 1 DAY) | FAIL | HTTP 500: No backend supports scalar function [ADDDATE] among [datafusion] |
| DateTime | SUBDATE | SELECT SUBDATE(DATE('2024-03-15'), INTERVAL 1 DAY) | FAIL | HTTP 500: No backend supports scalar function [SUBDATE] among [datafusion] |
| DateTime | ADDTIME | SELECT ADDTIME('10:00:00', '01:30:00') | FAIL | HTTP 500: No backend supports scalar function [ADDTIME] among [datafusion] |
| DateTime | SUBTIME | SELECT SUBTIME('10:00:00', '01:30:00') | FAIL | HTTP 500: No backend supports scalar function [SUBTIME] among [datafusion] |
| DateTime | PERIOD_ADD | SELECT PERIOD_ADD(202403, 2) | FAIL | HTTP 500: No backend supports scalar function [PERIOD_ADD] among [datafusion] |
| DateTime | PERIOD_DIFF | SELECT PERIOD_DIFF(202403, 202401) | FAIL | HTTP 500: No backend supports scalar function [PERIOD_DIFF] among [datafusion] |
| DateTime | SEC_TO_TIME | SELECT SEC_TO_TIME(3661) | FAIL | HTTP 500: No backend supports scalar function [SEC_TO_TIME] among [datafusion] |
| DateTime | TIME_TO_SEC | SELECT TIME_TO_SEC('01:01:01') | FAIL | HTTP 500: No backend supports scalar function [TIME_TO_SEC] among [datafusion] |
| DateTime | TO_DAYS | SELECT TO_DAYS('2024-03-15') | FAIL | HTTP 500: No backend supports scalar function [TO_DAYS] among [datafusion] |
| DateTime | FROM_DAYS | SELECT FROM_DAYS(739000) | FAIL | HTTP 500: No backend supports scalar function [FROM_DAYS] among [datafusion] |
| DateTime | LAST_DAY | SELECT LAST_DAY('2024-02-15') | FAIL | HTTP 500: No backend supports scalar function [LAST_DAY] among [datafusion] |
| DateTime | DAYNAME | SELECT DAYNAME(DATE('2024-03-15')) | FAIL | HTTP 500: No backend supports scalar function [DAYNAME] among [datafusion] |
| DateTime | MONTHNAME | SELECT MONTHNAME(DATE('2024-03-15')) | FAIL | HTTP 500: No backend supports scalar function [MONTHNAME] among [datafusion] |
| DateTime | YEARWEEK | SELECT YEARWEEK('2024-03-15') | FAIL | HTTP 500: No backend supports scalar function [YEARWEEK] among [datafusion] |
| DateTime | WEEKDAY | SELECT WEEKDAY(DATE('2024-03-15')) | FAIL | HTTP 500: No backend supports scalar function [WEEKDAY] among [datafusion] |
| DateTime | GET_FORMAT | SELECT GET_FORMAT(DATE, 'USA') | FAIL | HTTP 500: No backend supports scalar function [GET_FORMAT] among [datafusion] |
| DateTime | CONVERT_TZ | SELECT CONVERT_TZ('2024-03-15 10:00:00', '+00:00', '+05:30') | FAIL | HTTP 400: Unable to convert call convert_tz(string, string, string). |
| DateTime | UTC_TIMESTAMP | SELECT UTC_TIMESTAMP() IS NOT NULL AS ok | FAIL | HTTP 500: No backend supports scalar function [UTC_TIMESTAMP] among [datafusion] |
| DateTime | UTC_DATE | SELECT UTC_DATE() IS NOT NULL AS ok | FAIL | HTTP 500: No backend supports scalar function [UTC_DATE] among [datafusion] |
| DateTime | UTC_TIME | SELECT UTC_TIME() IS NOT NULL AS ok | FAIL | HTTP 500: No backend supports scalar function [UTC_TIME] among [datafusion] |
| DateTime | MAKEDATE | SELECT MAKEDATE(2024, 75) | PASS | [[19797]] |
| DateTime | MAKETIME | SELECT MAKETIME(10, 30, 0) | PASS | [[37800000000]] |
| DateTime | MICROSECOND | SELECT MICROSECOND(TIME('10:30:00.123456')) | FAIL | HTTP 400: Unable to convert call DATE_PART(string, precision_time<9>?). |
| DateTime | SYSDATE | SELECT SYSDATE() IS NOT NULL AS ok | PASS | [[true]] |
| DateTime | EXTRACT | SELECT EXTRACT(YEAR FROM DATE('2024-03-15')) | PASS | [[2024]] |
| DateTime | TIMEDIFF | SELECT TIMEDIFF('10:00:00', '08:30:00') | FAIL | HTTP 500: No backend supports scalar function [TIME_DIFF] among [datafusion] |
| DateTime | TO_SECONDS | SELECT TO_SECONDS('2024-03-15') | FAIL | HTTP 500: No backend supports scalar function [TO_SECONDS] among [datafusion] |
| DateTime | TIMESTAMP | SELECT TIMESTAMP('2024-03-15 10:30:00') IS NOT NULL AS ok | PASS | [[true]] |
| DateTime | TIME_FORMAT | SELECT TIME_FORMAT('10:30:00', '%H:%i') | FAIL | HTTP 500: Execution error: Optimizer rule 'simplify_expressions' failed
caused b |
| DateTime | DATETIME | SELECT DATETIME('2024-03-15 10:30:00') IS NOT NULL AS ok | PASS | [[true]] |
| Conditional | IF | SELECT IF(1 > 0, 'yes', 'no') | PASS | [["yes"]] |
| Conditional | IFNULL | SELECT IFNULL(NULL, 'default') | FAIL | HTTP 400: Cannot resolve function: IFNULL, arguments: [UNDEFINED,STRING], caused |
| Conditional | NULLIF | SELECT NULLIF(1, 1) | PASS | [[null]] |
| Conditional | ISNULL | SELECT ISNULL(NULL) | PASS | [[true]] |
| Type Conversion | CAST_INT | SELECT CAST('123' AS INT) | PASS | [[123]] |
| Type Conversion | CAST_DOUBLE | SELECT CAST('3.14' AS DOUBLE) | PASS | [[3.14]] |
| Type Conversion | CAST_STRING | SELECT CAST(123 AS STRING) | PASS | [["123"]] |
| Type Conversion | CAST_DATE | SELECT CAST('2024-03-15' AS DATE) | FAIL | Result mismatch (ordered). Expected: [["2024-03-15"]] Got: [[19797]] |
| Type Conversion | TYPEOF | SELECT TYPEOF(123) | PASS | [["INT"]] |
| Aggregate | COUNT | SELECT COUNT(*) FROM sanity_test | PASS | [[5]] |
| Aggregate | SUM | SELECT SUM(age) FROM sanity_test | PASS | [[142]] |
| Aggregate | AVG | SELECT AVG(age) FROM sanity_test | PASS | [[28.4]] |
| Aggregate | MIN | SELECT MIN(age) FROM sanity_test | PASS | [[22]] |
| Aggregate | MAX | SELECT MAX(age) FROM sanity_test | PASS | [[35]] |
| Aggregate | COUNT_DISTINCT | SELECT COUNT(DISTINCT city) FROM sanity_test | PASS | [[3]] |
| Aggregate | STDDEV_POP | SELECT STDDEV_POP(age) IS NOT NULL AS ok FROM sanity_test | PASS | [[true]] |
| Aggregate | STDDEV_SAMP | SELECT STDDEV_SAMP(age) IS NOT NULL AS ok FROM sanity_test | PASS | [[true]] |
| Aggregate | VAR_POP | SELECT VAR_POP(age) IS NOT NULL AS ok FROM sanity_test | PASS | [[true]] |
| Aggregate | VAR_SAMP | SELECT VAR_SAMP(age) IS NOT NULL AS ok FROM sanity_test | PASS | [[true]] |
| Aggregate | PERCENTILE_APPROX | SELECT PERCENTILE_APPROX(age, 50) FROM sanity_test | FAIL | HTTP 500: Unrecognized aggregate function [percentile_approx] |
| Aggregate | VARIANCE | SELECT VARIANCE(age) IS NOT NULL AS ok FROM sanity_test | PASS | [[true]] |
| Aggregate | STDDEV | SELECT STDDEV(age) IS NOT NULL AS ok FROM sanity_test | PASS | [[true]] |
| Aggregate | PERCENTILE | SELECT PERCENTILE(age, 50) FROM sanity_test | FAIL | HTTP 500: Unrecognized aggregate function [percentile_approx] |
| Window | ROW_NUMBER | SELECT name, ROW_NUMBER() OVER(ORDER BY age) AS rn FROM s... | PASS | [["Eve", 1], ["Bob", 2]] |
| Window | RANK | SELECT name, RANK() OVER(ORDER BY age DESC) AS rnk FROM s... | FAIL | HTTP 500: Unexpected window function: RANK |
| Window | DENSE_RANK | SELECT name, DENSE_RANK() OVER(ORDER BY age DESC) AS dr F... | FAIL | HTTP 500: Unexpected window function: DENSE_RANK |
| Relevance | MATCH | SELECT name FROM sanity_test WHERE match(name, 'Alice') | PASS | [["Alice"]] |
| Relevance | MATCH_PHRASE | SELECT name FROM sanity_test WHERE match_phrase(name, 'Al... | PASS | [["Alice"]] |
| Relevance | MULTI_MATCH | SELECT name FROM sanity_test WHERE multi_match(['name', '... | PASS | [["Alice"]] |
| Relevance | QUERY_STRING | SELECT name FROM sanity_test WHERE query_string(['name'],... | PASS | [["Alice"]] |
| Relevance | SIMPLE_QUERY_STRING | SELECT name FROM sanity_test WHERE simple_query_string(['... | PASS | [["Alice"]] |
| Relevance | MATCH_BOOL_PREFIX | SELECT name FROM sanity_test WHERE match_bool_prefix(name... | PASS | [["Alice"]] |
| Relevance | MATCH_PHRASE_PREFIX | SELECT name FROM sanity_test WHERE match_phrase_prefix(na... | PASS | [["Alice"]] |
| Relevance | WILDCARD_QUERY | SELECT name FROM sanity_test WHERE wildcard_query(city, '... | PASS | [["Alice"], ["Carol"]] |
| Relevance | QUERY | SELECT name FROM sanity_test WHERE query('name:Alice') | PASS | [["Alice"]] |

## Appendix

### Test Setup

* OpenSearch core: latest main with LuceneFilterDelegationHandle fix
* SQL Plugin: feature/analytics-engine-compat-report branch
* Cluster: single-node with analytics-engine + 9 plugins, `-da -dsa`

### Test Data

**sanity_test** (composite/parquet + lucene):

| name | age | city | dept_id |
|------|-----|------|---------|
| Alice | 32 | Seattle | 1 |
| Bob | 25 | Portland | 2 |
| Carol | 28 | Seattle | 1 |
| Dave | 35 | Portland | 2 |
| Eve | 22 | Denver | 1 |

**sanity_dept** (composite/parquet + lucene):

| id | dept |
|----|------|
| 1 | Engineering |
| 2 | Marketing |

**sanity_datetime** (composite/parquet + lucene):

| event | ts | event_date | event_time | amount |
|-------|-----|------------|------------|--------|
| login | 2024-03-15T10:30:00 | 2024-03-15 | 10:30:00 | 100 |
| purchase | 2024-03-15T14:00:00 | 2024-03-15 | 14:00:00 | 250 |
| login | 2024-03-16T09:00:00 | 2024-03-16 | 09:00:00 | 50 |
| purchase | 2024-03-16T11:30:00 | 2024-03-16 | 11:30:00 | 300 |

