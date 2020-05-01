# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for partial document alignments."""

import pandas as pd

from histore.archive.reader import RowIndexReader
from histore.archive.row import ArchiveRow
from histore.archive.store.mem.reader import BufferedReader
from histore.archive.timestamp import Timestamp
from histore.archive.value import SingleVersionValue
from histore.document.base import PartialDocument, RIDocument
from histore.document.schema import Column


def test_align_partial_document():
    """Test row position alignment for partial documents."""
    ts = Timestamp(version=1)
    # Create a list of 5 archive rows.
    rows = list()
    for rid in range(5):
        pos = SingleVersionValue(value=rid, timestamp=ts)
        row = ArchiveRow(rowid=rid, pos=pos, cells=dict(), timestamp=ts)
        rows.append(row)
    # Create a partial document with rows 2 and 3 and one new row 10.
    data = [['A'], ['B'], ['C']]
    df = pd.DataFrame(data=data, index=[10, 3, 2], columns=['Col'])
    schema = [Column(colid=1, name='Col')]
    doc = RIDocument(df=df, schema=schema)
    assert doc.rows == [(2, 2), (3, 1), (10, 0)]
    # Adjust the row positions based on the original row order.
    row_index = RowIndexReader(reader=BufferedReader(rows), version=1)
    doc = PartialDocument(doc=doc, row_index=row_index)
    assert doc.rows == [(2, 2), (3, 3), (10, 5)]
