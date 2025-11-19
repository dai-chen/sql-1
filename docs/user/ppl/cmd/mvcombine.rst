=============
mvcombine
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
============
| The ``mvcombine`` command combines multiple rows with identical field values into a single row by aggregating a specified field into a multivalue field (array). All rows that share the same values for all fields except the target field will be combined.


Syntax
============
mvcombine [delim=<string>] <field>

* delim: optional. Specifies a delimiter for string representation of the multivalue field. Default is a space (" ").
* field: mandatory. The field name to combine into a multivalue field.


Example 1: Combine by Aggregation Result
=========================================

Combine state values that have the same count::

    os> source=accounts | stats count() by state, account_number | stats count() by state | mvcombine state;
    fetched rows / total rows = 1/1
    +---------+---------------+
    | count() | state         |
    |---------+---------------|
    | 1       | [IL,MD,TN,VA] |
    +---------+---------------+


Example 2: Combine Fields with Same Value
==========================================

Group records and combine the field values. Each account_number has a unique state::

    os> source=accounts | fields account_number, state | mvcombine state;
    fetched rows / total rows = 4/4
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    | 1              | [IL]  |
    | 18             | [MD]  |
    | 6              | [TN]  |
    | 13             | [VA]  |
    +----------------+-------+


Example 3: No Matching Rows to Combine
=======================================

When no rows match for combining, each row remains separate::

    os> source=accounts | stats count() by gender | mvcombine gender;
    fetched rows / total rows = 2/2
    +---------+--------+
    | count() | gender |
    |---------+--------|
    | 1       | [F]    |
    | 3       | [M]    |
    +---------+--------+


Example 4: Invalid Field
=========================

Attempting to use mvcombine on a non-existent field::

    os> source=accounts | fields gender | mvcombine nonexistent_field;
    {'reason': 'Invalid Query', 'details': 'Field [nonexistent_field] not found.', 'type': 'IllegalArgumentException'}
    Error: Query returned no data
