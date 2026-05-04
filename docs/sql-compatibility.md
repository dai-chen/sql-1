# OpenSearch SQL Compatibility

This document describes the SQL dialect accepted by OpenSearch's SQL frontend:
its conformance to the SQL standard, its extensions beyond the standard, and its
differences from the previous OpenSearch SQL (V2, legacy) engine.

It is modeled on the [MySQL Standards Compliance](https://dev.mysql.com/doc/refman/9.7/en/compatibility.html)
and [PostgreSQL SQL Conformance](https://www.postgresql.org/docs/current/features.html)
references.

- [SQL Standards Compliance](#sql-standards-compliance)
- [Extensions to Standard SQL](#extensions-to-standard-sql)
- [Differences from Standard SQL](#differences-from-standard-sql)
- [Differences from OpenSearch SQL V2 (Legacy)](#differences-from-opensearch-sql-v2-legacy)
  - [Lexical and literal differences](#lexical-and-literal-differences)
  - [Identifier differences](#identifier-differences)
  - [Type-system differences](#type-system-differences)
  - [Function differences](#function-differences)
  - [Operator differences](#operator-differences)
  - [Clause and syntactic differences](#clause-and-syntactic-differences)
  - [Catalog statement differences](#catalog-statement-differences)
  - [Semantic differences](#semantic-differences)

---

## SQL Standards Compliance

OpenSearch SQL targets **ANSI SQL-92** with selected features from SQL:1999,
SQL:2003, SQL:2008, and SQL:2016. The dialect supports a broad subset of
Data Query Language (DQL) — `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`,
`LIMIT`, joins, subqueries, CTEs, window functions, `UNION`, `INTERSECT`,
`EXCEPT`, `CASE`, and `CAST` — with BigQuery-flavored lexical rules to support
OpenSearch-style identifiers (hyphens, backticks, dotted names).

Data Definition Language (`CREATE` / `ALTER` / `DROP`) and Data Manipulation
Language (`INSERT` / `UPDATE` / `DELETE`) are not supported through SQL —
OpenSearch data is created and modified through the document APIs.

## Extensions to Standard SQL

OpenSearch SQL accepts the following constructs that are not part of standard
ANSI SQL.

| Category | Example |
|---|---|
| Hyphenated identifiers | `SELECT * FROM logs-2024-01-01` |
| Backtick-quoted identifiers | `` SELECT `name` FROM employees `` |
| Backslash-escaped string literals | `SELECT 'it\'s'` |
| `GROUP BY` by select-list alias | `SELECT dept FROM t GROUP BY dept` |
| `GROUP BY` by ordinal position | `SELECT name FROM t GROUP BY 1` |
| `LIMIT N` syntax | `SELECT * FROM t LIMIT 10` |
| Optional `FROM` clause | `SELECT 1` |
| Lenient type coercion | `CAST(true AS INTEGER)`, `WHERE age > '30'` |
| Full-text search functions | `match()`, `match_phrase()`, `match_bool_prefix()`, `match_phrase_prefix()`, `multi_match()`, `query_string()`, `simple_query_string()` |
| Named arguments for search functions | `match(name, 'John', operator='AND')` |
| Function-call date/time literals | `DATE('2020-09-16')`, `TIME('09:07:00')`, `TIMESTAMP('2020-09-16 10:20:30')` |
| Cross-type temporal comparisons | `DATE(x) = TIME(y)`, `TIMESTAMP(x) > DATE(y)` |
| MySQL/BigQuery library functions | `DATETIME()`, `IF()`, `IFNULL()`, `NVL()`, `LENGTH()`, `LPAD()`, `RPAD()`, `RTRIM()`, `LTRIM()`, `CONCAT_WS()`, `SUBSTR()`, `INSTR()`, `REVERSE()`, `FORMAT()`, and others |
| Arithmetic function-call aliases | `add(a,b)`, `subtract(a,b)`, `multiply(a,b)`, `divide(a,b)`, `modulus(a,b)` |
| `STRING` as a cast target type | `CAST(x AS STRING)` |
| MySQL-style `convert_tz()` | `convert_tz('2008-05-15 12:00:00', '+00:00', '+10:00')` |
| `AVG()` on date/time expressions | `AVG(CAST(ts AS TIMESTAMP))`, `AVG(timestamp(expr))` |
| Original-text column names | `SELECT COUNT(*)` returns a column labeled `COUNT(*)` rather than `EXPR$0` |

## Differences from Standard SQL

The following are deliberate deviations from ANSI SQL, adopted to preserve
OpenSearch-idiomatic behavior.

- **Double-quoted identifiers vs. string literals.** ANSI SQL reserves double
  quotes for identifiers. OpenSearch SQL follows BigQuery conventions: some
  double-quoted forms are string literals (e.g. `SELECT "Hello"`) as a side
  effect of accepting hyphenated identifiers like `logs-2024-01`.

- **`GROUP BY` alias wins over column.** If a `GROUP BY` item matches both a
  select-list alias and an underlying column name, the alias is used. This
  matches BigQuery and PostgreSQL.

- **Lenient string-to-number coercion.** Expressions like `WHERE age > '30'`
  silently coerce the string operand to a number, matching MySQL. Standard SQL
  requires an explicit `CAST`.

- **Backslash escapes in single-quoted strings.** Patterns like `'it\'s'` are
  accepted for backward compatibility with OpenSearch SQL V2. Standard SQL uses
  doubled single quotes (`'it''s'`) instead.

- **`MATCH` is not reserved.** `SELECT * FROM t WHERE match(name, 'John')`
  parses without quoting `MATCH`, to support full-text search in a natural
  function-call form.

---

## Differences from OpenSearch SQL V2 (Legacy)

The following constructs are accepted by SQL V2 but **not** accepted by
OpenSearch SQL. Where a standard-SQL equivalent exists, it is shown in the
Comment column.

### Lexical and literal differences

| V2 Syntax | Comment |
|---|---|
| `FROM test-logs-2025.01.01` | Dot-prefixed numeric fragments inside hyphenated names are not supported. |
| `{ts '2020-09-16 17:30:00'}`, `{t '17:30:00'}`, `{d '2020-09-16'}` | JDBC escape syntax for temporal literals is not supported. Use `TIMESTAMP '…'`, `TIME '…'`, `DATE '…'` instead. |
| `SELECT "I""m"` | Doubled inner double-quotes inside a string are not supported. Use `'I''m'` or `'I\'m'`. |
| `SELECT 'I\'m'` (some escape patterns) | A small set of backslash-escape edge cases is rejected. |
| `'a' REGEXP 'b'` | Infix `REGEXP` is not supported. Use a `REGEXP_*` function. |
| `SELECT max(int0) FROM t WHERE int0 IS NULL;` | Trailing semicolons are not accepted. |
| `# comment` | `#` is not a comment starter. Use `--` (SQL) or `/* … */` (block). |
| `multi_match([f1, f2], 'text')` | Square-bracket field lists are not supported. Use standard function-argument form. |

### Identifier differences

| V2 Syntax | Comment |
|---|---|
| `FROM test.one` | Dot-separated names are interpreted as `schema.table`. Use `` `test.one` `` or rename the index. |
| `SELECT @timestamp FROM t` | Identifiers cannot start with `@`. Quote the name: `` `@timestamp` ``. |
| `SELECT _routing FROM test.metafields` | Metafield identifiers combined with dot-pathed index names are not resolved. |
| `SELECT __age FROM test.twounderscores` | Double-underscore columns combined with dot-pathed index names are not resolved. |

### Type-system differences

| V2 Syntax | Comment |
|---|---|
| `SELECT avg(date0) FROM t` | `AVG()` on a bare column whose type is temporal is not supported. Wrap the column in `CAST(… AS TIMESTAMP)` or use `AVG(timestamp(col))`. |
| `SELECT HOUR(char_field) FROM t` | `EXTRACT` / `HOUR` / `MINUTE` / `SECOND` on a `CHAR`/`VARCHAR` column is not supported. Cast the value to `TIME` first. |
| `SELECT yyyy-MM-dd_OR_epoch_millis FROM t` | A hyphenated column name in an expression position is parsed as subtraction. Quote it: `` `yyyy-MM-dd_OR_epoch_millis` ``. |

### Function differences

The following V2 function names are not recognized. Each row shows the standard
SQL equivalent where one exists.

| V2 Function | Standard SQL Equivalent |
|---|---|
| `convert_tz(dt, from_tz, to_tz)` | `CONVERT_TIMEZONE(from_tz, to_tz, dt)` (argument order differs; a datetime cast is required) |
| `percentile(col, pct)` | `PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY col)` |
| `day_of_month(x)` | `EXTRACT(DAY FROM x)` |
| `hour_of_day(x)` | `EXTRACT(HOUR FROM x)` |
| `second_of_minute(x)` | `EXTRACT(SECOND FROM x)` |
| `weekofyear(x)` | `EXTRACT(WEEK FROM x)` |
| `weekday(x)` | `EXTRACT(DOW FROM x) - 1` |
| `microsecond(x)` | `EXTRACT(MICROSECOND FROM x)` |
| `DATEDIFF(a, b)` | `TIMESTAMPDIFF(DAY, b, a)` |
| `subdate(ts, interval N unit)` | `ts - INTERVAL 'N' UNIT` |
| `str_to_date(s, fmt)` | `TO_TIMESTAMP(s, fmt)` |
| `date_format(ts, fmt)` | `FORMAT_TIMESTAMP(fmt, ts)` (format-pattern differences apply) |
| `expm1(x)` | `EXP(x) - 1` |
| `ISNULL(x)` (function form) | `CASE WHEN x IS NULL THEN 1 ELSE 0 END`, or `x IS NULL` as a predicate |
| `sec_to_time(n)`, `time_to_sec(t)`, `yearweek(ts)`, `TIMEDIFF(t1, t2)`, `SUBTIME(t1, t2)`, `TO_DAYS(d)`, `MAKEDATE(year, day)` | No direct standard SQL equivalent. |
| `GET_FORMAT(DATE, 'USA')` | No standard SQL equivalent. |

### Operator differences

| V2 Syntax | Comment |
|---|---|
| `INTERVAL NULL DAY` | `INTERVAL` requires a numeric literal or expression, not `NULL`. Use `CAST(NULL AS INTERVAL DAY)` if a null interval is needed. |

### Clause and syntactic differences

| V2 Syntax | Comment |
|---|---|
| `EXTRACT(YEAR_MONTH FROM x)`, `EXTRACT(DAY_SECOND FROM x)`, `EXTRACT(HOUR_SECOND FROM x)` | MySQL compound time-frame names are not recognized. Extract the component parts individually or use `TIMESTAMPDIFF`. |
| `SELECT * FROM t GROUP BY t.age` | Qualified column references in `GROUP BY` combined with `SELECT *` are rejected in strict validation. Use `GROUP BY age` or list explicit columns. |
| `FROM bank b1, b1.firstname` | Implicit nested-field joins via an alias path are not supported. |
| `ORDER BY NESTED('field', 'path')` | The OpenSearch-specific `NESTED()` sort function is not supported. |

### Catalog statement differences

The following catalog-browse statements are not accepted. Data-definition and
catalog browsing are not part of the SQL surface in the new engine.

| V2 Syntax | Comment |
|---|---|
| `SHOW TABLES LIKE '…'` | Use the OpenSearch `_cat/indices` or `_mapping` REST APIs. |
| `DESCRIBE TABLES LIKE '…'` | Use the OpenSearch `_mapping` API. |
| `SHOW FUNCTIONS` | Not supported. |

### Semantic differences

| V2 Behavior | New Engine Behavior |
|---|---|
| `CASE WHEN gender IS NULL THEN 'aaa' ELSE gender END` is accepted with implicit type coercion. | Validation fails with "All result types of CASE clause must be the same." Cast all branches to a common type. |

---

## References

- [MySQL 9.7 — Standards Compliance](https://dev.mysql.com/doc/refman/9.7/en/compatibility.html)
- [PostgreSQL — SQL Conformance](https://www.postgresql.org/docs/current/features.html)
- [opensearch-project/sql — Issue #5346: Unified SQL language](https://github.com/opensearch-project/sql/issues/5346)
- [opensearch-project/sql — PR #5360: Unified SQL language spec with composable extensions](https://github.com/opensearch-project/sql/pull/5360)
- [opensearch-project/sql — PR #5392: Preserve original SQL expression text as column name](https://github.com/opensearch-project/sql/pull/5392)
