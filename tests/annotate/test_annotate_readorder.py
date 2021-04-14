# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""unit tests for annotating documents that are keyed by a list of numeric row
index values.
"""

import histore.document.schema as schema
import histore.key.annotate as annotate


def test_primarykey_keys():
    """Test getting key values for a document that is keyed by a list of
    primary key columns.
    """
    rows = [
        [1, 'Alice'],
        [2, 1.3],
        ['Claire', 3],
        [4, None],
        [5, ['Eve']]
    ]
    # Single column key
    readorder = annotate.pk_readorder(
        rows=rows,
        keys=schema.column_index(schema=['ID', 'Name'], columns='Name')
    )
    assert len(readorder) == 5
    assert [r[0] for r in readorder] == [1, 2, 0, 4, 3]
    assert [r[2] for r in readorder] == [1, 2, 0, 4, 3]
    assert readorder[0][1].is_number()
    assert readorder[1][1].is_number()
    assert readorder[2][1].is_string()
    assert readorder[3][1].is_string()
    assert readorder[4][1].is_null()
    # Multi column key
    pk = schema.column_index(schema=['ID', 'Name'], columns=['Name', 'ID'])
    readorder = annotate.pk_readorder(rows=rows, keys=pk)
    assert [r[0] for r in readorder] == [1, 2, 0, 4, 3]
    assert [r[2] for r in readorder] == [1, 2, 0, 4, 3]
    assert readorder[0][1][0].is_number()
    assert readorder[0][1][1].is_number()
    assert readorder[1][1][0].is_number()
    assert readorder[1][1][1].is_string()
    assert readorder[2][1][0].is_string()
    assert readorder[2][1][1].is_number()
    assert readorder[3][1][0].is_string()
    assert readorder[3][1][1].is_number()
    assert readorder[4][1][0].is_null()
    assert readorder[4][1][1].is_number()
    # No error if duplicate column is not part of the primary key.
    pk = schema.column_index(schema=['SSN', 'Name', 'SSN'], columns=['Name'])
    annotate.pk_readorder(rows=rows, keys=pk)


def test_rowindex_keys():
    """Test getting key values for a document that is keyed by a row index."""
    readorder = annotate.rowindex_readorder(index=[1, 2, -1, 'A', None])
    assert len(readorder) == 5
    for i in range(5):
        rowidx, key, pos = readorder[i]
        assert rowidx == i
        assert pos == i
        if i >= 2:
            assert key.is_new()
        else:
            assert key.value == i + 1
