==============================
Json Serialization of Archives
==============================

The persistent archive serializes the archive of dataset snapshots as two Json documents. The first document (``rows.dat``) contains an array of serialized archive rows. Each row represents an identifiable row that was present in one or more dataset snapshots. The second document (``metadata.dat``) contains the archive schema and snapshot information.

Here are two example serailizations for a row and a column in a dataset archive. The structure of the serialization is defined below. The default implementation of the serializer uses single character labels to reduce storage space.

**Archive Row** ('r': row-id, 't': timestamp, 'p': position, 'c': cells, 'v': value, 'k': row key value)

.. code-block:: json

    {
        "r": 0,
        "t": [[0, 3]],
        "p": [
            {
                "t": [[0, 2]],
                "v": 0
            },
            {
                "t": [[3, 3]
                ],
                "v": 3
            }
        ],
        "c": {
            "0": {
                "v": "Alice"
            },
            "1": [
                {
                    "t": [[0,0],[2,3]],
                    "v": 32
                },
                {
                    "t": [[1,1]],
                    "v": 33
                }
            ]
        },
        "k": "Alice"
    }


**Archive Column** ('c': column-id, 'n': name, 'p': position, 't': timestamp)

.. code-block:: json

        {
            "c": 0,
            "n": {
                "v": "Name"
            },
            "p": {
                "v": 0
            },
            "t": [[0, 3]]
        }
    

Basic Structures
----------------

**Timestamps** are represented as arrays of 2-dimensional arrays. The inner arrays represent the start and end version of each timestamp interval.

.. code-block:: console

    TIMESTAMP ::= [([int, int],)+]


**Single version values** are data frame cell values, column names, or column/row index positions that never changed throught their existince in the archive. These values are represented as Json objects with an optional timestamp and the scalar value. The timestamp for a single version value is only present if it differs from the timestamp of the parent element (e.g., a data frame row).

.. code-block:: console

    SINGLE-VALUE ::= {("t": TIMESTAMP,)? "v": scalar}


**Multi-version values** are lists of single version values, one value for each of the different versions.

.. code-block:: console

    MULTI-VALUE ::= [(SINGLE-VALUE,)+]



Archive Data File
-----------------

The archive data file contains an array of **archive row** objects. Rows are Json objects with the following structure:

.. code-block:: console

    ARCHIVE-ROW ::= {
        '$TIMESTAMP': TIMESTAMP,
        '$ROWID': int,
        '$POSITION': TIMESTAMPED-VALUE,
        '$CELLS': [CELL-VALUE]
    }
    
    TIMESTAMPED-VALUE ::= SINGLE_VALUE | MULTI-VALUE

    CELL-VALUE ::= {'int': TIMESTAMPED-VALUE}

Metadata File
-------------

The metadata file contains arrays of **schema columns** and snapshot information. Columns are represented as Json object with the following structure:

.. code-block:: console

    ARCHIVE-COLUMN ::= {
        '$TIMESTAMP': TIMESTAMP,
        '$COLID': int,
        '$POSITION': TIMESTAMPED-VALUE,
        '$NAME': TIMESTAMPED-VALUE
    }
