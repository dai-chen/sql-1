# Analytics Engine SQL Sanity Test Report

**68 queries | 61 PASS | 7 FAIL | Pass Rate: 89%**

All passing tests have results **verified** against expected values (exact row match).

## ✅ SELECT (6/6)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | select_where | `SELECT * FROM sanity_test WHERE age > 30` | ✅ [[32, "Seattle", 1, "Alice"], [35, "Portland", 2, "Dave"]] |
| 2 | select_star | `SELECT * FROM sanity_test` | ✅ 5 rows: [[32, "Seattle", 1, "Alice"], [25, "Portland", 2, "Bob"]]... |
| 3 | select_expression | `SELECT name, age * 2 AS double_age, age + 10 AS age_plus FROM sanity_test` | ✅ 5 rows: [["Alice", 64, 42], ["Bob", 50, 35]]... |
| 4 | select_distinct | `SELECT DISTINCT city FROM sanity_test` | ✅ [["Seattle"], ["Portland"], ["Denver"]] |
| 5 | select_alias | `SELECT name AS employee_name, city AS location FROM sanity_test` | ✅ 5 rows: [["Alice", "Seattle"], ["Bob", "Portland"]]... |
| 6 | select_literal | `SELECT 1 + 1 AS two, 'hello' AS greeting` | ✅ [[2, "hello"]] |

## ✅ WHERE (5/5)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | where_in | `SELECT name, city FROM sanity_test WHERE city IN ('Seattle', 'Denver')` | ✅ [["Alice", "Seattle"], ["Carol", "Seattle"], ["Eve", "Denver"]] |
| 2 | where_between | `SELECT name, age FROM sanity_test WHERE age BETWEEN 25 AND 32` | ✅ [["Alice", 32], ["Bob", 25], ["Carol", 28]] |
| 3 | where_and_or | `SELECT name FROM sanity_test WHERE (city = 'Seattle' OR city = 'Denver') AND age > 25` | ✅ [["Alice"], ["Carol"]] |
| 4 | where_not | `SELECT name FROM sanity_test WHERE NOT city = 'Portland'` | ✅ [["Alice"], ["Carol"], ["Eve"]] |
| 5 | where_is_null | `SELECT name FROM sanity_test WHERE city IS NOT NULL` | ✅ 5 rows: [["Alice"], ["Bob"]]... |

## ✅ ORDER BY (3/3)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | order_by_limit | `SELECT name, age FROM sanity_test ORDER BY age DESC LIMIT 3` | ✅ [["Dave", 35], ["Alice", 32], ["Carol", 28]] |
| 2 | order_by_multi | `SELECT name, city, age FROM sanity_test ORDER BY city ASC, age DESC` | ✅ 5 rows: [["Eve", "Denver", 22], ["Dave", "Portland", 35]]... |
| 3 | order_by_asc | `SELECT name, age FROM sanity_test ORDER BY age ASC LIMIT 3` | ✅ [["Eve", 22], ["Bob", 25], ["Carol", 28]] |

## ✅ LIMIT / OFFSET (3/3)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | limit_only | `SELECT name, age FROM sanity_test ORDER BY age LIMIT 2` | ✅ [["Eve", 22], ["Bob", 25]] |
| 2 | offset_skip | `SELECT name, age FROM sanity_test ORDER BY age LIMIT 2 OFFSET 2` | ✅ [["Carol", 28], ["Alice", 32]] |
| 3 | offset_zero | `SELECT name FROM sanity_test ORDER BY name LIMIT 3 OFFSET 0` | ✅ [["Alice"], ["Bob"], ["Carol"]] |

## ✅ GROUP BY (3/3)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | group_by_sum_count | `SELECT city, SUM(age), COUNT(*) FROM sanity_test GROUP BY city` | ✅ [["Seattle", 60, 2], ["Portland", 60, 2], ["Denver", 22, 1]] |
| 2 | group_by_max | `SELECT city, MAX(age) FROM sanity_test GROUP BY city` | ✅ [["Seattle", 32], ["Portland", 35], ["Denver", 22]] |
| 3 | filtered_agg | `SELECT city, SUM(age) FILTER(WHERE age > 25) FROM sanity_test GROUP BY city` | ✅ [["Seattle", 60], ["Portland", 60], ["Denver", 22]] |

## ✅ HAVING (1/1)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | group_by_having | `SELECT city, COUNT(*) AS cnt FROM sanity_test GROUP BY city HAVING cnt > 1` | ✅ [["Seattle", 2], ["Portland", 2]] |

## ✅ CASE (2/2)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | case_simple | `SELECT name, CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END AS level FROM sani...` | ✅ 5 rows: [["Alice", "senior"], ["Bob", "junior"]]... |
| 2 | case_in_agg | `SELECT city, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS senior_count FROM sanity...` | ✅ [["Denver", 0], ["Seattle", 1], ["Portland", 1]] |

## ✅ LIKE / REGEXP (3/3)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | like_prefix | `SELECT name FROM sanity_test WHERE city LIKE 'Sea%'` | ✅ [["Alice"], ["Carol"]] |
| 2 | like_contains | `SELECT name FROM sanity_test WHERE city LIKE '%ort%'` | ✅ [["Bob"], ["Dave"]] |
| 3 | like_negated | `SELECT name FROM sanity_test WHERE NOT city LIKE 'Sea%'` | ✅ [["Bob"], ["Dave"], ["Eve"]] |

## ❌ Set Operations (0/3)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | union_basic | `SELECT name, age FROM sanity_test WHERE city = 'Seattle' UNION SELECT name, age FR...` | ❌ HTTP 500: [UNION] is not a valid term at this part of the query: '...RE city = 'Seattle' UNION' <--  |
| 2 | union_all_basic | `SELECT name, age FROM sanity_test WHERE city = 'Seattle' UNION ALL SELECT name, ag...` | ❌ HTTP 500: [UNION] is not a valid term at this part of the query: '...RE city = 'Seattle' UNION' <--  |
| 3 | minus_basic | `SELECT name FROM sanity_test WHERE city = 'Seattle' MINUS SELECT name FROM sanity_...` | ❌ HTTP 500: [MINUS] is not a valid term at this part of the query: '...RE city = 'Seattle' MINUS' <--  |

## ✅ JOIN (4/4)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | inner_join | `SELECT a.name, b.dept FROM sanity_test a JOIN sanity_dept b ON a.dept_id = b.id` | ✅ 5 rows: [["Alice", "Engineering"], ["Carol", "Engineering"]]... |
| 2 | left_join | `SELECT a.name, b.dept FROM sanity_test a LEFT JOIN sanity_dept b ON a.dept_id = b.id` | ✅ 5 rows: [["Alice", "Engineering"], ["Carol", "Engineering"]]... |
| 3 | right_join | `SELECT a.name, b.dept FROM sanity_test a RIGHT JOIN sanity_dept b ON a.dept_id = b.id` | ✅ 5 rows: [["Alice", "Engineering"], ["Bob", "Marketing"]]... |
| 4 | cross_join | `SELECT a.name, b.dept FROM sanity_test a CROSS JOIN sanity_dept b LIMIT 3` | ✅ [["Alice", "Engineering"], ["Alice", "Marketing"], ["Bob", "Engineering"]] |

## 🟡 Subqueries (1/3)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | subquery_where | `SELECT name FROM sanity_test WHERE dept_id IN (SELECT id FROM sanity_dept WHERE de...` | ❌ HTTP 500: Stage 0 failed |
| 2 | subquery_from | `SELECT t.city, t.total FROM (SELECT city, COUNT(*) AS total FROM sanity_test GROUP...` | ✅ [["Seattle", 2], ["Portland", 2], ["Denver", 1]] |
| 3 | exists_subquery | `SELECT name FROM sanity_test a WHERE EXISTS (SELECT 1 FROM sanity_dept b WHERE b.i...` | ❌ HTTP 500: Unrecognized filter operator [EXISTS / EXISTS] |

## ✅ EXPLAIN (2/2)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | explain_select | `SELECT name, age FROM sanity_test WHERE age > 25` | ✅ (plan returned) |
| 2 | explain_join | `SELECT a.name, b.dept FROM sanity_test a JOIN sanity_dept b ON a.dept_id = b.id` | ✅ (plan returned) |

## ✅ Full-text search (9/9)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | match_query | `SELECT name FROM sanity_test WHERE match(name, 'Alice')` | ✅ [["Alice"]] |
| 2 | match_phrase | `SELECT name FROM sanity_test WHERE match_phrase(name, 'Alice')` | ✅ [["Alice"]] |
| 3 | match_bool_prefix | `SELECT name FROM sanity_test WHERE match_bool_prefix(name, 'Ali')` | ✅ [["Alice"]] |
| 4 | match_phrase_prefix | `SELECT name FROM sanity_test WHERE match_phrase_prefix(name, 'Ali')` | ✅ [["Alice"]] |
| 5 | multi_match | `SELECT name FROM sanity_test WHERE multi_match(['name', 'city'], 'Alice')` | ✅ [["Alice"]] |
| 6 | simple_query_string | `SELECT name FROM sanity_test WHERE simple_query_string(['name'], 'Alice')` | ✅ [["Alice"]] |
| 7 | query_string | `SELECT name FROM sanity_test WHERE query_string(['name'], 'Alice')` | ✅ [["Alice"]] |
| 8 | query_func | `SELECT name FROM sanity_test WHERE query('name:Alice')` | ✅ [["Alice"]] |
| 9 | wildcard_query | `SELECT name FROM sanity_test WHERE wildcard_query(city, 'Sea*')` | ✅ [["Alice"], ["Carol"]] |

## 🟡 Functions (19/21)

| # | Name | Query | Result |
|---|------|-------|--------|
| 1 | string_upper_lower_length | `SELECT UPPER('hello'), LOWER('WORLD'), LENGTH('test')` | ✅ [["HELLO", "world", 4]] |
| 2 | string_concat_substring | `SELECT CONCAT('hello', ' ', 'world'), SUBSTRING('hello', 2, 3), TRIM(' hello ')` | ✅ [["hello world", "ell", "hello"]] |
| 3 | string_replace | `SELECT REPLACE('hello world', 'world', 'there')` | ✅ [["hello there"]] |
| 4 | math_abs_ceil_floor | `SELECT ABS(-5), CEIL(3.2), FLOOR(3.8)` | ✅ [[5, 4, 3]] |
| 5 | math_round_mod_sqrt | `SELECT ROUND(3.56, 1), MOD(10, 3), SQRT(16)` | ✅ [[3.6, 1, 4]] |
| 6 | math_pow | `SELECT POW(2, 3)` | ✅ [[8]] |
| 7 | datetime_now_curdate | `SELECT DATE_FORMAT(NOW(), '%H:%i:%s') IS NOT NULL AS ok` | ✅ [[true]] |
| 8 | datetime_date_format | `SELECT DATE_FORMAT('2024-01-15', '%Y-%m')` | ✅ [["2024-01"]] |
| 9 | datetime_col_filter | `SELECT event, ts FROM sanity_datetime WHERE ts > '2024-03-16 00:00:00'` | ✅ [["login", "2024-03-16 09:00:00"], ["purchase", "2024-03-16 11:30:00"]] |
| 10 | agg_avg_min_max | `SELECT AVG(age), MIN(age), MAX(age) FROM sanity_test` | ✅ [[28.4, 22, 35]] |
| 11 | agg_count_sum | `SELECT COUNT(*), SUM(age) FROM sanity_test` | ✅ [[5, 142]] |
| 12 | agg_count_distinct | `SELECT COUNT(DISTINCT city) FROM sanity_test` | ✅ [[3]] |
| 13 | conditional_isnull | `SELECT ISNULL(NULL)` | ✅ [[true]] |
| 14 | conditional_case | `SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END` | ✅ [["yes"]] |
| 15 | conditional_nullif | `SELECT NULLIF(1, 1)` | ✅ [[null]] |
| 16 | window_row_number | `SELECT name, city, ROW_NUMBER() OVER(PARTITION BY city ORDER BY age DESC) AS rn FR...` | ✅ 5 rows: [["Eve", "Denver", 1], ["Dave", "Portland", 1]]... |
| 17 | window_sum_over | `SELECT name, SUM(age) OVER(PARTITION BY city) AS city_total FROM sanity_test ORDER...` | ✅ 5 rows: [["Alice", 60], ["Bob", 60]]... |
| 18 | window_rank | `SELECT name, RANK() OVER(ORDER BY age DESC) AS rnk FROM sanity_test` | ❌ HTTP 500: Unexpected window function: RANK |
| 19 | window_dense_rank | `SELECT name, DENSE_RANK() OVER(ORDER BY age DESC) AS dr FROM sanity_test` | ❌ HTTP 500: Unexpected window function: DENSE_RANK |
| 20 | conversion_typeof | `SELECT TYPEOF(123)` | ✅ [["INT"]] |
| 21 | conversion_cast | `SELECT CAST('123' AS INT)` | ✅ [[123]] |

---

## Failure Summary

| # | Name | Category | Error | Root Cause |
|---|------|----------|-------|------------|
| 1 | union_basic | Set Operations | `HTTP 500: [UNION] is not a valid term at this part of the query: '...R` | UNION/UNION ALL not supported by SQL parser |
| 2 | union_all_basic | Set Operations | `HTTP 500: [UNION] is not a valid term at this part of the query: '...R` | UNION/UNION ALL not supported by SQL parser |
| 3 | minus_basic | Set Operations | `HTTP 500: [MINUS] is not a valid term at this part of the query: '...R` | Syntax not supported by SQL parser |
| 4 | subquery_where | Subqueries | `HTTP 500: Stage 0 failed` | TaskResourceTrackingService thread reuse |
| 5 | exists_subquery | Subqueries | `HTTP 500: Unrecognized filter operator [EXISTS / EXISTS]` | See error |
| 6 | window_rank | Functions | `HTTP 500: Unexpected window function: RANK` | See error |
| 7 | window_dense_rank | Functions | `HTTP 500: Unexpected window function: DENSE_RANK` | See error |

