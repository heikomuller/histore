# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for special error cases for archive schemas."""

import pytest

from histore.archive.schema import ArchiveSchema, column_index
from histore.document.schema import Column


def test_column_index_with_duplicates():
    """Test mapping an archive schema to a list of columns with duplicate
    entries.
    """
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        version=0
    )
    columns = list(schema.columns.values())
    with pytest.raises(ValueError):
        column_index(columns, [Column(0, 'Name'), Column(1, 'Age'), Column(0, 'Name')])


def test_duplicate_column_error():
    """Test error when intializing an archive schema with a list of columns that
    contains a duplicate.
    """
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        version=0
    )
    columns = list(schema.columns.values()) + list(schema.columns.values())
    with pytest.raises(ValueError):
        ArchiveSchema(columns=columns)


def test_invalid_columns_argument():
    """Test error when providing an invalid argument type as list of schema
    columns.
    """
    with pytest.raises(ValueError):
        ArchiveSchema(columns=1)
