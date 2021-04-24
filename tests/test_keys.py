# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""unit test for document row keys."""

import pytest

from histore.key import to_key, NewRow, StringKey


def test_invalid_key():
    """Test invalid key value."""
    with pytest.raises(ValueError):
        to_key({'a', 1})
    with pytest.raises(ValueError):
        to_key(('a', 1))


@pytest.mark.parametrize(
    'value,is_equal',
    [(None, False), (NewRow(), False), (1, False), ('a', True)]
)
def test_key_equality(value, is_equal):
    """Test equality of key values."""
    key = to_key(value)
    assert (key == StringKey('a')) == is_equal


def test_key_generation():
    """Test generating and comparing key classes."""
    keys = [to_key(v) for v in [None, NewRow(), 1, 1.2, 'a', 'b', 2]]
    values = [str(k) for k in sorted(keys)]
    assert values == ['1', '1.2', '2', 'a', 'b', '<Null (None)>', '<NewRow (None)>']
    keys = [to_key(v) for v in [1, 1.2, None, NewRow(), 'a', 'b', 2]]
    values = [str(k) for k in sorted(keys)]
    assert values == ['1', '1.2', '2', 'a', 'b', '<Null (None)>', '<NewRow (None)>']


@pytest.mark.parametrize(
    'value,typeid',
    [(None, 1), (NewRow(), 0), (1, 2), ('a', 3)]
)
def test_key_types(value, typeid):
    """Test comparing numbers and strings."""
    key = to_key(value)
    matched = False
    for i, f in enumerate([key.is_new, key.is_null, key.is_number, key.is_string]):
        if f():
            assert i == typeid
            matched = True
    assert matched
