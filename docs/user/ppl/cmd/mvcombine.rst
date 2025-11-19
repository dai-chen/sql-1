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


Example 1: Basic Field Combining
=================================

Combine rows that have identical max/min values by aggregating the host field::

    os> source=logs | stats max(bytes) AS max, min(bytes) AS min BY host | mvcombine host;
    fetched rows / total rows = 2/2
    +-------+-------+------------------+
    | max   | min   | host             |
    |-------+-------+------------------|
    | 1000  | 100   | ["host1","host2"]|
    | 2000  | 200   | ["host3"]        |
    +-------+-------+------------------+


Example 2: Custom Delimiter
============================

Use a custom delimiter to separate combined values::

    os> source=logs | stats count() BY category | mvcombine delim="," category;
    fetched rows / total rows = 2/2
    +-------+------------------+
    | count | category         |
    |-------+------------------|
    | 5     | "cat1,cat2"      |
    | 3     | "cat3"           |
    +-------+------------------+


Example 3: Non-Consecutive Matches
===================================

Combine non-consecutive rows with matching aggregation values::

    os> source=security_logs | stats count() BY EventCode | mvcombine EventCode;
    fetched rows / total rows = 2/2
    +-------+------------------+
    | count | EventCode        |
    |-------+------------------|
    | 10    | ["4624","4634"]  |
    | 5     | ["4625"]         |
    +-------+------------------+


Example 4: Invalid Syntax
===========================

Attempting to use mvcombine with an invalid field throws an error::

    os> source=logs | mvcombine nonexistent_field;
    Error: Field 'nonexistent_field' not found in the input schema
