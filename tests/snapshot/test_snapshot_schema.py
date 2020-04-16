# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for snapshot schemas."""

from histore.snapshot.schema import Column


def test_snapshot_columns():
    """Test creating instances of snapshot schema columns."""
    col = Column(colid=1, name='my_col')
    assert col == 'my_col'
    assert isinstance(col, str)
    assert col.colid == 1
