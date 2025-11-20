==========
mvcombine
==========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============

The ``mvcombine`` command combines multiple events that have identical field values (except for one specified field) into a single event. The specified field is converted into a multivalue field containing all the individual values from the combined events.


Syntax
============

.. code-block:: none

    mvcombine [delim=<string>] <field>

* ``field``: Required. The name of the field to combine into a multivalue field.
* ``delim``: Optional. A delimiter string to join values. If not specified, output is a multivalue array preserving original data types. If specified, output is a single delimited string with all values converted to strings.


Parameters
============

.. list-table::
   :widths: 20 15 15 50
   :header-rows: 1

   * - Parameter
     - Type
     - Required
     - Description
   * - ``field``
     - field name
     - Yes
     - Name of the field to combine into multivalue field
   * - ``delim``
     - string
     - No
     - Delimiter string to join values. Default behavior (no delimiter) preserves types in array.


Behavioral Notes
================

**Grouping Logic**

Events are grouped when ALL fields except the specified field have identical values. The comparison is exact match (case-sensitive for strings). System/internal fields (like ``_id``, ``_index``) are automatically excluded from the grouping logic.

**NULL and Missing Value Handling**

* NULL values in the combining field are included in the multivalue result as NULL
* If grouping fields contain NULL, those are treated as a distinct value for grouping

**Type Handling**

* **Without** ``delim`` **parameter**: Values are preserved in their original types as a multivalue array

  * String fields: preserved as strings
  * Numeric fields: preserved as numbers
  * Boolean fields: preserved as boolean values
  
* **With** ``delim`` **parameter**: All values are converted to strings and joined with the delimiter into a single string value

**Ordering**

The order of values in the resulting multivalue field is unspecified. Do not rely on any particular ordering. If order matters, sort before using ``mvcombine``.

**Single Value Case**

If only one event matches the grouping criteria, the field is still converted to a multivalue array with a single element (or single string if delimiter is specified).


Examples
============

Example 1: Consolidate host values after stats
------------------------------------------------

Combine host values when max and min bytes are identical::

    os> source=accounts | stats max(account_number) AS max_acc, min(account_number) AS min_acc BY state | mvcombine state;
    fetched rows / total rows = 6/6
    +-----------+-----------+---------------------+
    | max_acc   | min_acc   | state               |
    |-----------+-----------+---------------------|
    | 25        | 6         | ["CA", "NC"]        |
    | 49        | 13        | ["TX", "WA"]        |
    | 44        | 1         | ["ID", "RI", "VA"]  |
    +-----------+-----------+---------------------+

Example 2: Combine with custom delimiter
------------------------------------------

Use a comma delimiter to create a delimited string::

    os> source=accounts | fields state, gender | where state = 'CA' OR state = 'WA' | mvcombine delim="," gender;
    fetched rows / total rows = 2/2
    +---------+--------+
    | state   | gender |
    |---------+--------|
    | CA      | F,M    |
    | WA      | M,F    |
    +---------+--------+

Example 3: Combine numeric values
-----------------------------------

Preserve numeric types when combining without delimiter::

    os> source=accounts | fields state, account_number | where state = 'TX' | mvcombine account_number;
    fetched rows / total rows = 1/1
    +---------+---------------------+
    | state   | account_number      |
    |---------+---------------------|
    | TX      | [13, 49]            |
    +---------+---------------------+

Example 4: Invalid field name (negative case)
----------------------------------------------

Attempting to combine a non-existent field results in an error::

    os> source=accounts | mvcombine nonexistent_field;
    fetched rows / total rows = 0/0
    ERROR: field [Field(field=nonexistent_field, fieldArgs=[])] not found


Limitations
============

* The order of values in the multivalue result is unspecified
* No support for nested field paths in v1
* System/internal fields are automatically excluded from grouping
