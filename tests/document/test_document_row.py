# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for the different document reader."""

from histore.document.row import DocumentRow
from histore.key import NumberKey


def test_document_row_to_string():
    """Test string representation for document rows."""
    row = DocumentRow(pos=0, key=NumberKey(1), values={0: 'Alice', 1: 23})
    assert str(row) == "<DocumentRow(key=1, pos=0, values={0: 'Alice', 1: 23})"
