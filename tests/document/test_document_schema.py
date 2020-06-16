# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for document schemas."""

import pytest

from histore.document.schema import Column

import histore.document.schema as schema


def test_column_index():
    """Test column index function."""
    SCHEMA = ['Name', 'Age', 'Salary']
    cols = schema.column_index(schema=SCHEMA, columns=['Name', 'Salary'])
    assert cols == [0, 2]
    cols = schema.column_index(schema=SCHEMA, columns=['Name', 2])
    assert cols == [0, 2]
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        schema.column_index(schema=SCHEMA, columns=['Name', 3])
    with pytest.raises(ValueError):
        schema.column_index(schema=SCHEMA, columns=['Name', 'Income'])
    with pytest.raises(ValueError):
        schema.column_index(schema=[], columns=['Name'])


def test_document_columns():
    """Test creating instances of document schema columns."""
    col = Column(colid=1, name='my_col')
    assert col == 'my_col'
    assert isinstance(col, str)
    assert col.colid == 1
