# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for sorting row keys."""

from histore.annotate.key import NewRow, NullKey, NumberKey, StringKey


def test_sort_scalar_values():
    """Test sorting a list of key values."""
    keys = sorted([
        StringKey('B'),
        NumberKey(2),
        NullKey(1),
        StringKey('A'),
        NumberKey(1.3),
        NewRow(1),
        StringKey('D'),
        NullKey(2),
        NewRow(2)
    ])
    # Numbers come first
    assert keys[0].is_number()
    assert keys[0].value == 1.3
    assert str(keys[0]) == '1.3'
    assert keys[1].is_number()
    assert keys[1].value == 2
    assert str(keys[1]) == '2'
    # Strings come next
    assert keys[2].is_string()
    assert keys[2].value == 'A'
    assert str(keys[2]) == 'A'
    assert keys[3].is_string()
    assert keys[3].value == 'B'
    assert str(keys[3]) == 'B'
    assert keys[4].is_string()
    assert keys[4].value == 'D'
    assert str(keys[4]) == 'D'
    # None values are next
    assert keys[5].is_null()
    assert keys[5].identifier == 1
    assert str(keys[5]) == '<Null (1)>'
    assert keys[6].is_null()
    assert keys[6].identifier == 2
    assert str(keys[6]) == '<Null (2)>'
    # New rows are last
    assert keys[7].is_new()
    assert keys[7].identifier == 1
    assert str(keys[7]) == '<NewRow (1)>'
    assert keys[8].is_new()
    assert keys[8].identifier == 2
    assert str(keys[8]) == '<NewRow (2)>'


def test_sort_tuples():
    """Test sorting a list of key tuples."""
    keys = sorted([
        (StringKey('B'), StringKey('B')),
        (StringKey('B'), NumberKey(1)),
        (NumberKey(1), StringKey('B')),
        (StringKey('A'), StringKey('B')),
        (StringKey('B'), NewRow())
    ])
    assert [k.value for k in keys[0]] == [1, 'B']
    assert [k.value for k in keys[1]] == ['A', 'B']
    assert [k.value for k in keys[2]] == ['B', 1]
    assert [k.value for k in keys[3]] == ['B', 'B']
    assert keys[4][1].is_new()
