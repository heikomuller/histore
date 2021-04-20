# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the pandas data frame row list."""

import pandas as pd
import pytest

from histore.document.df import DataFrameDocument, rowindex_readorder


@pytest.fixture
def dataset():
    """Get data frame for test purposes."""
    return pd.DataFrame(
        data=[['alice', 26], ['bob', 34], ['claire', 19]],
        index=[0, 2, 1],
        columns=['Name', 'Age']
    )


def test_dataframe_document(dataset):
    """Test creating data frame documents with default read order."""
    doc = DataFrameDocument(df=dataset)
    assert doc.columns == ['Name', 'Age']
    rows = list()
    for rowidx, values in doc.iterrows():
        rows.append((rowidx, values))
    assert rows == [(0, ['alice', 26]), (1, ['claire', 19]), (2, ['bob', 34])]


def test_dataframe_iterate(dataset):
    """Test iterating through data frame rows."""
    doc = DataFrameDocument(df=dataset)
    reader = doc.open()
    rows = list()
    while reader.has_next():
        _, rowidx, values = reader.next()
        rows.append((rowidx, values))
    assert rows == [(0, ['alice', 26]), (1, ['claire', 19]), (2, ['bob', 34])]


def test_dataframe_positions():
    """Ensure that row positions are correctly maintained when iterating over
    an un-keyed data frame document.
    """
    df = pd.DataFrame(
        data=[['Dave', 33], ['Claire', 27], ['Bob', 44], ['Alice', 32]],
        index=[3, 2, 1, 0],
        columns=['Name', 'Age'],
        dtype=object
    )
    positions = rowindex_readorder(df=df)
    assert positions == [3, 2, 1, 0]
    # Read document.
    doc = DataFrameDocument(df=df)
    reader = doc.open()
    positions = list()
    while reader.has_next():
        pos, _, _ = reader.next()
        positions.append(pos)
    reader.close()
    assert positions == [3, 2, 1, 0]


def test_dataframe_read(dataset):
    """Test reading a data frame from a DataFrame document."""
    doc = DataFrameDocument(df=dataset)
    df = doc.read_df()
    pd.testing.assert_frame_equal(df, dataset)
    doc.close()
    assert doc.read_df() is None


def test_data_frame_with_new_rows():
    """Test data frame with rows that have None or -1 as their index value."""
    df = pd.DataFrame(
        data=[['alice', 26], ['bob', 34], ['claire', 19]],
        index=[None, 2, -1],
        columns=['Name', 'Age']
    )
    with DataFrameDocument(df=df).open() as reader:
        names = [values[0] for _, _, values in reader]
    assert names == ['bob', 'alice', 'claire']


def test_read_in_custom_order(dataset):
    """Test reading the data frame document in custom order."""
    doc = DataFrameDocument(df=dataset, readorder=[2, 1, 0])
    assert doc.columns == ['Name', 'Age']
    rows = list()
    for rowidx, values in doc.iterrows():
        rows.append((rowidx, values))
    assert rows == [(1, ['claire', 19]), (2, ['bob', 34]), (0, ['alice', 26])]
