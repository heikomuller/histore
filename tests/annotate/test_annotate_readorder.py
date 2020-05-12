# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""unit tests for annotating documents that are keyed by a list of numeric row
index values.
"""

import pytest

import histore.annotate.base as annotate


def test_primarykey_keys():
    """Test getting key values for a document that is keyed by a list of
    primary key columns.
    """
    rows=[
        [1, 'Alice'],
        [2, 1.3],
        ['Claire', 3],
        [4, None],
        [5, ['Eve']]
    ]
    # Single column key
    readorder = annotate.pk_readorder(
        columns=['ID', 'Name'],
        rows=rows,
        primary_key='Name'
    )
    assert len(readorder) == 5
    for i in range(5):
        rowidx, _, pos = readorder[i]
        assert rowidx == i
        assert pos == i
    assert readorder[0][1].is_string()
    assert readorder[1][1].is_number()
    assert readorder[2][1].is_number()
    assert readorder[3][1].is_null()
    assert readorder[4][1].is_string()
    # Multi column key
    readorder = annotate.pk_readorder(
        columns=['ID', 'Name'],
        rows=rows,
        primary_key=['Name', 'ID']
    )
    assert len(readorder) == 5
    for i in range(5):
        rowidx, _, pos = readorder[i]
        assert rowidx == i
        assert pos == i
    assert readorder[0][1][0].is_string()
    assert readorder[0][1][1].is_number()
    assert readorder[1][1][0].is_number()
    assert readorder[1][1][1].is_number()
    assert readorder[2][1][0].is_number()
    assert readorder[2][1][1].is_string()
    assert readorder[3][1][0].is_null()
    assert readorder[3][1][1].is_number()
    assert readorder[4][1][0].is_string()
    assert readorder[4][1][1].is_number()
    # Error cases
    with pytest.raises(ValueError):
        annotate.pk_readorder(
            columns=['SSN', 'Name'],
            rows=rows,
            primary_key=['Name', 'ID']
        )
    with pytest.raises(ValueError):
        annotate.pk_readorder(
            columns=['Name', 'SSN', 'Name', 'Name'],
            rows=rows,
            primary_key=['Name']
        )
    # No error if duplicate column is not part of the primary key.
    annotate.pk_readorder(
        columns=['ID', 'Name', 'SSN', 'SSN'],
        rows=rows,
        primary_key=['Name']
    )


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
