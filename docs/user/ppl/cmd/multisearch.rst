==========
multisearch
==========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``multisearch`` command to execute multiple independent subsearches and combine their results using UNION ALL with automatic timestamp-based ordering.
| The command runs two or more subsearches and globally interleaves results by timestamp (DESC) when the ``@timestamp`` field is available.
| Falls back to sequential concatenation when timestamp field is missing. All PPL commands are supported in subsearches.

Version
=======
3.3.0

Syntax
======
multisearch [ <subsearch1> ] [ <subsearch2> ] ... [ <subsearchN> ]

* subsearch: mandatory. At least 2 subsearches required. Each executes PPL commands independently.
* Result combination: UNION ALL of all subsearch outputs, followed by ORDER BY @timestamp DESC when available.
* Schema merging: Automatically handles different field schemas across subsearches with NULL padding.

Example 1: Basic multisearch with age filtering
==============================================

This example combines results from different age groups and adds category labels.

PPL query::

    os> source=accounts | multisearch 
        [ source=accounts | where age < 30 | eval age_group = 'young' ] 
        [ source=accounts | where age >= 30 | eval age_group = 'adult' ];
    fetched rows / total rows = 4/4
    +------+-----------+---------+-----------+
    | age  | firstname | gender  | age_group |
    |------+-----------+---------+-----------|
    | 32   | Amber     | M       | adult     |
    | 36   | Hattie    | M       | adult     |
    | 28   | Nanette   | F       | young     |
    | null | Virginia  | null    | null      |
    +------+-----------+---------+-----------+

Example 2: Multisearch with statistical aggregation in subsearches
================================================================

This example runs separate statistics for male and female accounts, then combines results.

PPL query::

    os> source=accounts | multisearch 
        [ source=accounts | where gender = 'M' | stats count(*) as count, avg(age) as avg_age by state | eval type = 'male' ]
        [ source=accounts | where gender = 'F' | stats count(*) as count, avg(age) as avg_age by state | eval type = 'female' ]
        | sort state;
    fetched rows / total rows = 8/8
    +-------+---------+--------+--------+
    | count | avg_age | state  | type   |
    |-------+---------+--------+--------|
    | 1     | 36      | IL     | male   |
    | 1     | 32      | IL     | male   |
    | 1     | 28      | VA     | female |
    | 2     | 34      | null   | male   |
    +-------+---------+--------+--------+

Example 3: Time-based interleaving with timestamp ordering
========================================================

This example shows automatic timestamp-based ordering when ``@timestamp`` field is present.

PPL query::

    os> source=logs | multisearch
        [ source=service_logs | where level = 'ERROR' ]
        [ source=access_logs | where status >= 400 ]
        | head 5;
    fetched rows / total rows = 5/5
    +---------------------+------------+--------+--------+
    | @timestamp          | message    | level  | status |
    |---------------------|------------|--------|--------|
    | 2021-07-02T10:00:05 | DB timeout | ERROR  | null   |
    | 2021-07-02T10:00:04 | Not found  | null   | 404    |
    | 2021-07-02T10:00:03 | Auth fail  | ERROR  | null   |
    | 2021-07-02T10:00:02 | Server err | null   | 500    |
    | 2021-07-02T10:00:01 | Bad req    | null   | 400    |
    +---------------------+------------+--------+--------+

Example 4: Three subsearches with complex pipeline operations
============================================================

This example demonstrates using three subsearches with subsequent pipeline operations.

PPL query::

    os> source=accounts | multisearch
        [ source=accounts | where age < 25 | eval category = 'young' ]
        [ source=accounts | where age between 25 and 35 | eval category = 'middle' ]  
        [ source=accounts | where age > 35 | eval category = 'senior' ]
        | stats count(*) by category;
    fetched rows / total rows = 3/3
    +----------+---------+
    | count(*) | category|
    |----------|---------|
    | 1        | young   |
    | 2        | middle  |
    | 1        | senior  |
    +----------+---------+

Performance Considerations
=========================
* **Schema Merging**: Different field schemas are automatically merged with NULL padding for missing fields
* **Memory Usage**: All subsearch results are materialized in memory before union operations  
* **Execution**: Subsearches execute independently, allowing for different optimization strategies
* **Ordering**: Timestamp ordering adds minimal overhead when ``@timestamp`` field exists

Limitations
===========
* Requires minimum 2 subsearches (validation enforced)
* Memory consumption scales with total result set size across all subsearches
* Cross-cluster subsearches supported but may have additional latency
* Performance impact increases with number of subsearches and result set sizes
