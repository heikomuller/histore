# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for partial document alignments."""

import pandas as pd

from histore.key.base import NumberKey
from histore.document.mem.dataframe import DataFrameDocument
from histore.document.schema import Column
from histore.tests.base import ListReader


def test_align_partial_document():
    """Test row position alignment for partial documents."""
    # Create a partial document with rows 2 and 3 and one new row 10.
    data = [['A'], ['B'], ['C']]
    df = pd.DataFrame(data=data, index=[10, 3, 2], columns=['Col'])
    doc = DataFrameDocument(df=df)
    posreader = ListReader(values=[(NumberKey(value=i), i) for i in range(5)])
    doc = doc.partial(reader=posreader)
    assert [r[0] for r in doc.readorder] == [2, 1, 0]
    assert [r[1].value for r in doc.readorder] == [2, 3, 10]
    assert [r[2] for r in doc.readorder] == [2, 3, 5]
    reader = doc.reader(schema=[Column(colid=0, name='Col')])
    values = list()
    while reader.has_next():
        row = reader.next()
        values.append(row.values[0])
        assert isinstance(row.key, NumberKey)
    assert values == ['C', 'B', 'A']
