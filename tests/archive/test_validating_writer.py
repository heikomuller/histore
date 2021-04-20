# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the validating archive writer."""

import pytest

from histore.archive.store.mem.base import VolatileArchiveStore
from histore.archive.writer import ValidatingArchiveWriter
from histore.document.row import DocumentRow
from histore.key import NumberKey, StringKey


DOCUMENT = [
    DocumentRow(key=(NumberKey(0), StringKey('a')), pos=2, values={0: 1}),
    DocumentRow(key=(NumberKey(0), StringKey('b')), pos=1, values={0: 2}),
]


def test_write_invalid_archive():
    """Test writing an invalid sequence of rows to an archive."""
    archive = VolatileArchiveStore()
    writer = ValidatingArchiveWriter(archive.get_writer())
    writer.write_document_row(DOCUMENT[1], version=0)
    writer.write_document_row(DOCUMENT[1], version=0)
    with pytest.raises(ValueError):
        writer.write_document_row(DOCUMENT[0], version=0)


def test_write_valid_archive():
    """Test writing a valid sequence of rows to an archive."""
    archive = VolatileArchiveStore()
    writer = ValidatingArchiveWriter(archive.get_writer())
    for row in DOCUMENT:
        writer.write_document_row(row, version=0)
    # The row reflects the number of inserted document rows.
    assert writer.row_counter == len(DOCUMENT)
