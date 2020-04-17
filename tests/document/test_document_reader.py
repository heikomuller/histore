# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the document readers."""

import pandas as pd
import pytest

from histore.document.base import Document, PKDocument, RIDocument
from histore.document.schema import Column


def test_data_frame_with_primary_key():
    """Test reading a data frame where row identifier are generated from
    primary key columns.
    """
    schema = [Column(colid=0, name='Name'), Column(colid=1, name='Index')]
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 1], ['Claire', 1], ['Alice', 2]],
        columns=['Name', 'Index']
    )
    reader = PKDocument(df=df, schema=schema, primary_key='Name').reader()
    keys = list()
    while reader.has_next():
        row = reader.next()
        keys.append(row.identifier)
    assert keys == ['Alice', 'Alice', 'Bob', 'Claire']
    doc = PKDocument(df=df, schema=schema, primary_key=['Index', 'Name'])
    reader = doc.reader()
    keys = list()
    while reader.has_next():
        row = reader.next()
        keys.append(row.identifier)
    assert keys == [(1, 'Alice'), (1, 'Bob'), (1, 'Claire'), (2, 'Alice')]


def test_data_frame_with_row_index():
    """Test reading a data frame document with row index identifier."""
    # All row identifier are -1
    schema = [Column(colid=1, name='Name'), Column(colid=2, name='Index')]
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=['A', 'B', 'C'])
    reader = RIDocument(df=df, schema=schema, row_counter=0).reader()
    rowcount = 0
    while reader.has_next():
        row = reader.next()
        assert row.identifier == rowcount
        assert row.pos == rowcount
        assert row.values[1] == ((rowcount + 1) * 2) - 1
        rowcount += 1
    assert rowcount == 3
    # Positive row identifier
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=[1, 2, 3])
    reader = RIDocument(df=df, schema=schema, row_counter=4).reader()
    rowcount = 0
    while reader.has_next():
        row = reader.next()
        assert row.identifier == rowcount + 1
        assert row.pos == rowcount
        assert row.values[1] == ((rowcount + 1) * 2) - 1
        rowcount += 1
    assert rowcount == 3
    # Mixed row identifier
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=[1, 2, 'A'])
    reader = RIDocument(df=df, schema=schema, row_counter=3).reader()
    rowids = list()
    while reader.has_next():
        row = reader.next()
        rowids.append(row.identifier)
    assert rowids == [1, 2, 3]
    # No error for non-unique row identifier
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=[1, 2, 1])
    RIDocument(df=df, schema=schema, row_counter=3)


def test_invalid_schema_error():
    """Ensure an error is raised if the number of data frame columns does not
    match the number of elements in the given document schema.
    """
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]])
    with pytest.raises(ValueError):
        Document(df=df, schema=[], rows=[])
