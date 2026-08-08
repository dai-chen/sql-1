===================
SQL Compatibility
===================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Introduction
============

This document describes the SQL dialect accepted by OpenSearch's new Calcite-based
SQL frontend (the unified query API). It covers conformance to the ANSI SQL
standard, extensions beyond the standard, and differences from the prior OpenSearch
SQL (V2, legacy) engine.

.. note::

   The unified SQL path is not enabled by default and is currently reached only
   through an experimental routing flag. Queries submitted through the default
   code path continue to use OpenSearch SQL V2 and are unaffected by this
   document.


SQL Standards Compliance
========================

OpenSearch SQL targets ANSI SQL-92 with selected features from SQL:1999, SQL:2003,
SQL:2008, and SQL:2016. The dialect supports a broad subset of Data Query Language
(DQL):

* ``SELECT``, ``WHERE``, ``GROUP BY``, ``HAVING``, ``ORDER BY``, ``LIMIT``
* Inner, left, right, and full joins
* Subqueries and Common Table Expressions (``WITH ...``)
* Set operations: ``UNION``, ``INTERSECT``, ``EXCEPT``
* Window functions
* ``CASE`` expressions and ``CAST``

Data Definition Language (``CREATE`` / ``ALTER`` / ``DROP``) and Data Manipulation
Language (``INSERT`` / ``UPDATE`` / ``DELETE``) are **not** supported through SQL.
OpenSearch data is created and modified through the document REST APIs.


Extensions to Standard SQL
==========================

OpenSearch SQL accepts the following constructs that are not part of standard
ANSI SQL.

Lexical and identifier extensions
---------------------------------

* **Hyphenated identifiers.** Identifiers may contain embedded hyphens, which is
  useful for OpenSearch index names that encode dates::

      SELECT * FROM logs-2024-01-01

* **Backtick-quoted identifiers.** Identifiers may be delimited with backticks in
  addition to double quotes::

      SELECT `name` FROM employees

Query syntax extensions
-----------------------

* **GROUP BY by select-list alias.** A ``GROUP BY`` item may refer to a select-list
  alias as well as a column::

      SELECT department AS dept, COUNT(*) FROM employees GROUP BY dept

* **GROUP BY by ordinal position.** A ``GROUP BY`` item may be an integer referring
  to a select-list position::

      SELECT name, COUNT(*) FROM employees GROUP BY 1

* **LIMIT N syntax.** ``LIMIT`` is a widely-used MySQL/PostgreSQL extension and
  is not part of standard ANSI SQL (the standard uses
  ``OFFSET m ROWS FETCH FIRST n ROWS ONLY``). OpenSearch SQL accepts both
  forms::

      SELECT * FROM employees LIMIT 10

* **Optional FROM clause.** Constant-only queries may omit ``FROM``::

      SELECT 1

Full-text search extensions
---------------------------

* **Relevance search functions.** OpenSearch-native full-text search functions
  are registered as standard SQL functions. Options may be passed as
  ``name=value`` named arguments::

      SELECT * FROM employees WHERE match(name, 'John')
      SELECT * FROM employees WHERE match_phrase(bio, 'quick brown fox')
      SELECT * FROM employees WHERE match(name, 'John', operator='AND')

  The supported functions are: ``match``, ``match_phrase``, ``match_bool_prefix``,
  ``match_phrase_prefix``, ``multi_match``, ``query_string``,
  ``simple_query_string``.


Differences from Standard SQL
=============================

The deviations below are not design choices in isolation — each exists to keep
queries that worked on OpenSearch SQL V2 working unchanged on the new engine.

* **Double-quoted identifiers vs. string literals.** ANSI SQL reserves double
  quotes for identifiers. OpenSearch SQL follows BigQuery conventions: some
  double-quoted forms are treated as string literals (for example,
  ``SELECT "Hello"``). This is a side effect of accepting hyphenated identifiers
  such as ``logs-2024-01`` without quoting.

* **GROUP BY alias resolves before column.** If a ``GROUP BY`` item matches both a
  select-list alias and an underlying column name, the alias is used. This matches
  BigQuery and PostgreSQL.

* **Lenient string-to-number coercion.** Expressions like ``WHERE age > '30'``
  silently coerce the string operand to a number, matching MySQL. Standard SQL
  requires an explicit ``CAST``.

* **Backslash escapes in single-quoted strings.** Patterns like ``'it\'s'`` are
  accepted for backward compatibility with OpenSearch SQL V2. Standard SQL uses
  doubled single quotes (``'it''s'``) instead.

* **MATCH is not reserved.** ``SELECT * FROM t WHERE match(name, 'John')`` parses
  without quoting ``MATCH``. In standard SQL ``MATCH`` is a reserved keyword (used
  in foreign-key subquery predicates). OpenSearch SQL de-reserves it so that the
  full-text search function has a natural function-call form.


Migrating from SQL V2
=====================

The following constructs are accepted by the legacy V2 engine but are **not**
accepted by the new dialect. Where a standard-SQL equivalent exists,
it is shown in the comment.

Lexical and literal differences
-------------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - V2 syntax
     - Comment
   * - ``FROM test-logs-2025.01.01``
     - Index names containing a ``.<digit>`` segment (for example,
       ``test-logs-2025.01.01``, ``test.1``, ``my-idx.0``) are not supported
       because the parser tokenizes ``.01`` as a numeric fragment. Rename the
       index, or delimit the name with backticks.
   * - ``{ts '2020-09-16 17:30:00'}``, ``{t '17:30:00'}``, ``{d '2020-09-16'}``
     - JDBC escape syntax for temporal literals is not supported. Use
       ``TIMESTAMP '...'``, ``TIME '...'``, ``DATE '...'`` instead.
   * - ``SELECT "I""m"``
     - Doubled inner double-quotes inside a string are not supported. Use
       ``'I''m'`` or ``'I\'m'``.
   * - ``'a' REGEXP 'b'``
     - Infix ``REGEXP`` is not supported. Use a ``REGEXP_*`` function.
   * - ``SELECT max(int0) FROM t WHERE int0 IS NULL;``
     - Trailing semicolons are not accepted.
   * - ``# comment``
     - ``#`` is not a comment starter. Use ``--`` (SQL line comment) or ``/* ... */``
       (block comment).
   * - ``multi_match([f1, f2], 'text')``
     - Square-bracket field lists are not supported. Use standard function-argument
       form.
   * - ``INTERVAL NULL DAY``
     - ``INTERVAL`` requires a numeric literal or expression, not ``NULL``. Use
       ``CAST(NULL AS INTERVAL DAY)`` if a null interval is needed.

Identifier differences
----------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - V2 syntax
     - Comment
   * - ``FROM test.one``
     - Dot-separated names are interpreted as ``schema.table``. Delimit the index
       name with backticks, or rename the index.
   * - ``SELECT @timestamp FROM t``
     - Identifiers cannot start with ``@``. Delimit the name with backticks.
   * - ``FROM *ccounts``, ``FROM *cc*nts`` (wildcard index patterns)
     - Unquoted wildcard (``*``) identifiers are not supported. Delimit the
       pattern with backticks, or resolve to a concrete index name.
   * - ``FROM .opensearch_dashboards`` (dot-prefixed hidden index)
     - Dot-prefixed hidden-index names are parsed as schema-path fragments.
       Delimit the name with backticks.
   * - ``FROM `accounts,account2``` (comma-separated multi-index)
     - V2 accepts a single backtick-delimited identifier containing commas as
       a multi-index reference. The new engine parses each listed name as a
       separate table; use ``FROM accounts UNION ALL SELECT ... FROM account2``
       or target a single alias that covers both indices.
   * - ``SELECT _routing FROM test.metafields``
     - Metafield identifiers combined with dot-pathed index names are not resolved.
   * - ``SELECT __age FROM test.twounderscores``
     - Double-underscore columns combined with dot-pathed index names are not
       resolved.

Type-system differences
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - V2 syntax
     - Comment
   * - ``DATE('2020-09-16')``, ``TIME('09:07:00')``, ``TIMESTAMP('2020-09-16 10:20:30')``
     - Function-call date/time literal forms are not supported. Use ANSI typed
       literals: ``DATE '2020-09-16'``, ``TIME '09:07:00'``,
       ``TIMESTAMP '2020-09-16 10:20:30'``.
   * - ``SELECT DATE(x) = TIME(y) FROM t``
     - Comparing values of different temporal types (``DATE``, ``TIME``,
       ``TIMESTAMP``) is not supported. Cast both sides to a common type first:
       ``SELECT CAST(x AS TIMESTAMP) = CAST(y AS TIMESTAMP) FROM t``.
   * - ``CAST(x AS STRING)``
     - ``STRING`` is not a recognized type name. Use ``CAST(x AS VARCHAR)`` or
       ``CAST(x AS CHAR)``.
   * - ``SELECT avg(date0) FROM t``
     - ``AVG()`` over a temporal column or expression is not supported. Cast to a
       numeric type first, for example
       ``SELECT AVG(CAST(date0 AS BIGINT)) FROM t``.
   * - ``SELECT HOUR(char_field) FROM t``
     - ``EXTRACT`` / ``HOUR`` / ``MINUTE`` / ``SECOND`` on a ``CHAR``/``VARCHAR``
       column is not supported. Cast the value to ``TIME`` first.
   * - ``SELECT yyyy-MM-dd_OR_epoch_millis FROM t``
     - A hyphenated column name in an expression position is parsed as subtraction.
       Delimit the column name with backticks.

Function differences
--------------------

V2 accepts a wide set of MySQL-flavored and OpenSearch-specific functions that are
not registered in the new engine. The tables below group them by purpose and show
a standard SQL equivalent where one exists.

**Current date and time**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``NOW()``, ``SYSDATE()``
     - ``CURRENT_TIMESTAMP``
   * - ``CURDATE()``
     - ``CURRENT_DATE``
   * - ``CURTIME()``
     - ``CURRENT_TIME``
   * - ``UTC_DATE()``, ``UTC_TIME()``, ``UTC_TIMESTAMP()``
     - No direct equivalent. Convert from ``CURRENT_TIMESTAMP`` with a timezone
       offset, or perform the conversion client-side.

**Date/time component extraction**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``DAY_OF_MONTH(x)``, ``DAY(x)``
     - ``EXTRACT(DAY FROM x)``
   * - ``DAY_OF_WEEK(x)``
     - ``EXTRACT(DOW FROM x)``
   * - ``DAY_OF_YEAR(x)``
     - ``EXTRACT(DOY FROM x)``
   * - ``HOUR_OF_DAY(x)``
     - ``EXTRACT(HOUR FROM x)``
   * - ``MINUTE_OF_DAY(x)``, ``MINUTE_OF_HOUR(x)``
     - ``EXTRACT(MINUTE FROM x)`` (with arithmetic for ``_OF_DAY``)
   * - ``MONTH_OF_YEAR(x)``
     - ``EXTRACT(MONTH FROM x)``
   * - ``SECOND_OF_MINUTE(x)``
     - ``EXTRACT(SECOND FROM x)``
   * - ``WEEK_OF_YEAR(x)``, ``WEEKOFYEAR(x)``
     - ``EXTRACT(WEEK FROM x)``
   * - ``WEEKDAY(x)``
     - ``EXTRACT(DOW FROM x) - 1``
   * - ``MICROSECOND(x)``
     - ``EXTRACT(MICROSECOND FROM x)``
   * - ``DAYNAME(x)``, ``MONTHNAME(x)``
     - No standard equivalent. Use a ``CASE`` over ``EXTRACT(DOW FROM x)`` or
       ``EXTRACT(MONTH FROM x)``.

**Date/time arithmetic**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``DATE_ADD(d, INTERVAL N unit)``, ``ADDDATE(d, ...)``,
       ``ADDTIME(t1, t2)``
     - ``d + INTERVAL 'N' UNIT``
   * - ``DATE_SUB(d, INTERVAL N unit)``, ``SUBDATE(d, ...)``,
       ``SUBTIME(t1, t2)``
     - ``d - INTERVAL 'N' UNIT``
   * - ``DATEDIFF(a, b)``
     - ``TIMESTAMPDIFF(DAY, b, a)``
   * - ``TIMEDIFF(t1, t2)``
     - No direct equivalent. Compute with ``TIMESTAMPDIFF`` and format.
   * - ``TIMESTAMPADD(unit, n, ts)``, ``TIMESTAMPDIFF(unit, a, b)``
     - Standard SQL uses the same function names; however these are not
       registered as plain function calls in the new engine. Use
       ``ts + INTERVAL 'N' UNIT`` for ``TIMESTAMPADD`` and the duration-based
       interval arithmetic for ``TIMESTAMPDIFF``.
   * - ``PERIOD_ADD(yyyymm, n)``, ``PERIOD_DIFF(a, b)``
     - No standard equivalent. Work with explicit year/month arithmetic.

**Date/time formatting and conversion**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``CONVERT_TZ(dt, from_tz, to_tz)``
     - No direct equivalent. Convert in the client or via offset arithmetic.
   * - ``DATE_FORMAT(ts, fmt)``, ``TIME_FORMAT(t, fmt)``
     - ``FORMAT_TIMESTAMP(fmt, ts)`` (format-pattern syntax differs; see the
       engine reference).
   * - ``STR_TO_DATE(s, fmt)``
     - ``TO_TIMESTAMP(s, fmt)``
   * - ``FROM_UNIXTIME(n)``, ``UNIX_TIMESTAMP(ts)``
     - No standard equivalent. Use explicit epoch arithmetic or client-side
       conversion.
   * - ``FROM_DAYS(n)``, ``TO_DAYS(d)``, ``TO_SECONDS(ts)``,
       ``YEARWEEK(ts)``, ``SEC_TO_TIME(n)``, ``TIME_TO_SEC(t)``,
       ``MAKEDATE(year, day)``, ``MAKETIME(h, m, s)``, ``GET_FORMAT(DATE, 'USA')``
     - No direct standard SQL equivalent.

**Arithmetic function-call form**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``ADD(a, b)``, ``SUBTRACT(a, b)``, ``MULTIPLY(a, b)``, ``DIVIDE(a, b)``,
       ``MODULUS(a, b)``
     - Use the ``+``, ``-``, ``*``, ``/``, and ``MOD`` operators respectively.

**Math**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``ATAN(y, x)`` (two-argument form)
     - ``ATAN`` is single-argument. Use ``ATAN2(y, x)``.
   * - ``CEILING(x)``
     - ``CEIL(x)``
   * - ``POW(x, y)``
     - ``POWER(x, y)``
   * - ``LOG(x)``, ``LOG2(x)``
     - ``LN(x)`` for natural log (``LOG(x)``); ``LN(x) / LN(2)`` for base-2.
   * - ``SINH(x)``, ``COSH(x)``
     - No standard equivalent. Compute with ``EXP``:
       ``(EXP(x) - EXP(-x))/2`` for sinh, ``(EXP(x) + EXP(-x))/2`` for cosh.
   * - ``EXPM1(x)``
     - ``EXP(x) - 1``
   * - ``CONV(x, from_base, to_base)``, ``CRC32(s)``, ``RINT(x)``,
       ``SIGNUM(x)``, ``E()``, ``STD(x)``
     - No direct standard equivalent.

**String**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``LENGTH(s)``
     - ``CHAR_LENGTH(s)``
   * - ``SUBSTR(s, n, m)``
     - ``SUBSTRING(s FROM n FOR m)`` or ``SUBSTRING(s, n, m)``
   * - ``LTRIM(s)``, ``RTRIM(s)``
     - ``TRIM(LEADING FROM s)``, ``TRIM(TRAILING FROM s)``
   * - ``CONCAT_WS(sep, a, b, ...)``
     - ``a || sep || b || sep || ...`` or explicit ``CONCAT`` chain
   * - ``LEFT(s, n)``, ``RIGHT(s, n)``
     - ``SUBSTRING(s, 1, n)``, ``SUBSTRING(s, CHAR_LENGTH(s) - n + 1)``
   * - ``LOCATE(sub, s)``, ``LOCATE(sub, s, start)``
     - ``POSITION(sub IN s)`` (no start-offset form in standard SQL)
   * - ``REVERSE(s)``
     - No standard equivalent.
   * - ``STRCMP(a, b)``
     - ``CASE WHEN a = b THEN 0 WHEN a < b THEN -1 ELSE 1 END``

**Conditional / null-handling**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``IF(c, a, b)``
     - ``CASE WHEN c THEN a ELSE b END``
   * - ``IFNULL(x, y)``, ``NVL(x, y)``
     - ``COALESCE(x, y)``
   * - ``ISNULL(x)`` (function form)
     - ``x IS NULL`` as a predicate, or
       ``CASE WHEN x IS NULL THEN 1 ELSE 0 END`` for an integer result.

**Aggregate**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Standard SQL equivalent
   * - ``PERCENTILE(col, pct)``, ``PERCENTILE_APPROX(col, pct)``
     - ``PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY col)``

**OpenSearch-specific functions**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Comment
   * - ``NESTED(field)``, ``NESTED(field, path)``, ``REVERSE_NESTED(...)`` as a
       scalar
     - OpenSearch-specific nested-document projection/filter is not supported
       in ``SELECT`` / ``WHERE`` positions. Flatten the field at index time, or
       query the nested document with the OpenSearch DSL.
   * - ``GEO_INTERSECTS(field, 'polygon')``, ``GEO_BOUNDING_BOX(field, ...)``
     - Geospatial predicates are not supported. Use the corresponding
       OpenSearch geo DSL filters through the search API.
   * - ``TERMS(field, values)``
     - OpenSearch terms-list predicate is not supported. Use ``field IN (...)``
       or the ``terms`` query through the DSL.
   * - ``HIGHLIGHT(field)``, ``SCORE(query)``
     - Relevance metadata accessors are not supported through SQL. Use the
       OpenSearch search API to retrieve highlights and relevance scores.
   * - Legacy camelCase relevance aliases: ``MATCHQUERY``, ``MATCHPHRASE``,
       ``MATCHPHRASEQUERY``, ``MULTIMATCH``, ``MULTIMATCHQUERY``,
       ``WILDCARDQUERY``, ``WILDCARD_QUERY``, ``SCOREQUERY``, ``SCORE_QUERY``,
       ``QUERY``, ``MATCH_QUERY``
     - Use the canonical snake_case forms listed in `Extensions to Standard SQL`_:
       ``match``, ``match_phrase``, ``multi_match``, ``query_string``, etc.
   * - Alternative relevance-function syntax:
       ``field = MATCH_QUERY('text')``,
       ``(field_1, field_2) = MULTI_MATCH('text')``
     - V2 accepts this ``field = FUNCTION(query)`` form as shorthand for a
       standard relevance-function call. The new engine does not support it.
       Use the standard function-call form: ``match(field, 'text')``,
       ``multi_match([field_1, field_2], 'text')`` (note: the square-bracket
       form is itself not supported; see Lexical and literal differences).

**Type introspection**

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - V2 function
     - Comment
   * - ``TYPEOF(x)``, ``TYPE(x)``, ``FIELD(x)``
     - No standard equivalent.

Clause and syntactic differences
--------------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - V2 syntax
     - Comment
   * - ``EXTRACT(YEAR_MONTH FROM x)``, ``EXTRACT(DAY_SECOND FROM x)``,
       ``EXTRACT(HOUR_SECOND FROM x)``
     - MySQL compound time-frame names are not recognized. Extract the component
       parts individually or use ``TIMESTAMPDIFF``.
   * - ``SELECT * FROM t GROUP BY t.age``
     - Qualified column references in ``GROUP BY`` combined with ``SELECT *`` are
       rejected in strict validation. Use ``GROUP BY age`` or list explicit
       columns.
   * - ``FROM bank b1, b1.firstname``
     - Implicit nested-field joins via an alias path are not supported.

Metadata statement differences
------------------------------

The following metadata (catalog-browsing) statements are not accepted. Metadata
browsing is not part of the SQL surface in the new engine.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - V2 syntax
     - Comment
   * - ``SHOW TABLES LIKE '...'``
     - Use the OpenSearch ``_cat/indices`` or ``_mapping`` REST APIs.
   * - ``DESCRIBE TABLES LIKE '...'``
     - Use the OpenSearch ``_mapping`` API.

Output format differences
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - V2 behavior
     - New engine behavior
   * - Unaliased function or arithmetic columns are labeled with the original SQL
       text (for example, ``SELECT COUNT(*)`` returns a column named ``COUNT(*)``).
     - Columns are labeled with a synthetic name (``EXPR$0``, ``EXPR$1``, ...).
       Add an explicit alias to control the name: ``SELECT COUNT(*) AS total``.
   * - ``EXPLAIN`` returns a V2 plan-operator tree (for example,
       ``ProjectOperator`` → ``FilterOperator`` → ``OpenSearchIndexScan``), with
       the OpenSearch DSL JSON embedded in the leaf scan's ``request`` field.
     - ``EXPLAIN`` returns a relational-algebra plan in text form, separated into
       ``logical`` and ``physical`` sections. The format is different from V2
       and is considered internal to the new engine.
