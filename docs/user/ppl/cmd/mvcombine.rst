==========
mvcombine
==========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``mvcombine`` command to combine multiple values of a specified field into either a multivalue array or a delimited string. The command groups results by all other fields (except the specified field and system metadata fields) and aggregates the values of the specified field.


Syntax
============
mvcombine [delim=<string>] <field>

* delim: optional. A string delimiter to use when combining values. If not specified, values are combined into an array.
* field: mandatory. The field whose values should be combined.


Example 1: Combining host values without delimiter
===================================================

The example shows combining host values into an array after aggregating by max and min bytes.

PPL query::

    os> source=accounts | stats max(balance) as max, min(balance) as min by account_number | head 2 | mvcombine account_number | fields max, min, account_number;
    fetched rows / total rows = 1/1
    +-------+-------+------------------+
    | max   | min   | account_number   |
    |-------+-------+------------------|
    | 44313 | 44313 | [1, 6]           |
    +-------+-------+------------------+


Example 2: Combining with custom delimiter
===========================================

The example shows combining values into a delimited string using a comma separator.

PPL query::

    os> source=accounts | stats count() as cnt by state | head 2 | mvcombine delim="," state | fields cnt, state;
    fetched rows / total rows = 1/1
    +-------+---------+
    | cnt   | state   |
    |-------+---------|
    | 6     | ID,TX   |
    +-------+---------+


Example 3: Combining numeric values
====================================

The example shows combining numeric values into an array, preserving their numeric type.

PPL query::

    os> source=accounts | stats avg(balance) as avg by age | head 2 | mvcombine age | fields avg, age;
    fetched rows / total rows = 1/1
    +-------------------+---------+
    | avg               | age     |
    |-------------------+---------|
    | 28727.333333333332| [28, 32]|
    +-------------------+---------+


Example 4: Invalid field name (Negative Case)
==============================================

The example shows that specifying a non-existent field results in an error.

PPL query::

    os> source=accounts | stats count() by state | mvcombine invalid_field;
    Error: Field [Field(field=invalid_field, fieldArgs=[])] not found.


Limitations
===========
- The ``mvcombine`` command must follow aggregation operations (like ``stats``) since it operates on aggregated results.
- System metadata fields (like ``_id``, ``_index``, etc.) are automatically excluded from grouping.
- Field ordering in the result may vary as mvcombine performs internal aggregation.
