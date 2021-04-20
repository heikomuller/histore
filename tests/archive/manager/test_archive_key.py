# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the primary key column identifier list generator."""

import pytest

from histore.archive.manager.base import get_key_columns
from histore.document.schema import Column


def test_empty_key():
    """Ensure that None is returned when no primary key is specified."""
    assert get_key_columns(columns=['A', 'B'], primary_key=None) is None
    assert get_key_columns(columns=['A', 'B'], primary_key=[]) is None


def test_error_for_duplicate_column():
    """Ensure an error is raised if the primary key references a non-unique
    column.
    """
    columns = ['A', 'B', Column(colid=0, name='A')]
    assert get_key_columns(columns=columns, primary_key=['B']) is not None
    with pytest.raises(ValueError):
        get_key_columns(columns=columns, primary_key=['A'])


def test_error_for_unknown_column():
    """Ensure an error is raised if the primary key references a unknown
    column.
    """
    columns = ['A', 'B']
    with pytest.raises(ValueError):
        get_key_columns(columns=columns, primary_key=['A', 'C'])


def test_get_key_columns():
    """Test getting the list of primary key column identifier."""
    columns = ['A', 'B', Column(colid=0, name='C')]
    assert get_key_columns(columns=columns, primary_key=['A']) == [1]
    assert get_key_columns(columns=columns, primary_key=['A', 'B']) == [1, 2]
    assert get_key_columns(columns=columns, primary_key=['C', 'A']) == [0, 1]
