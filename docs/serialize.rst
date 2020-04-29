==============================
JSON Serialization of Archives
==============================

Use single character labels by default to reduces storage space.


Basic Structures
----------------

Timestamps are represented as array of 2-dimensional arrays.

.. code-block:: json

    {
        "$TIMESTAMP": [[1,2], [4,5]]
    }


Single value:

.. code-block:: json

    {
        "$TIMESTAMP": [[1,2], [4,5]],
        "$VALUE": scalar
    }


Multi value is a list of single values with a mandatory time stamp.



Archive Data File
-----------------

Entries in the archive row data file have the following structure.

.. code-block:: json

    {
        "t": [[1,2], [4,5]],
        "v": [{
            "c": 1,
            "t": [[1,2]],
            "v": {}
        }],
        "i": {}
    }


ArchiveRow ::=
- $timestamp ::= Timestamp
- $rowid ::= int
- $index ::= TimestampedValue
- $cells ::= [CellValue]

Timestamp ::= [[int, int]]

TimestampedValue ::= SingleVersionValue || MultiVersionValue

SingleVersionValue ::=
- $timestamp ::= Timestamp (*optional)
- $value :: <scalar>

MultiVersionValue ::= [SingleVersionValue]

CellValue ::= {
    $column ::= SingleVersionValue || MultiVersionValue
}


ArchiveColumn ::=
- $timestamp ::= Timestamp
- $colid ::= int
- $index ::= TimestampedValue
- $name ::= TimestampedValue
