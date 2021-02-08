# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the pandas data frame row list."""


import pandas as pd

from histore.document.mem.dataframe import DataFrameDocument, Rows
from histore.document.schema import Column


def test_dataframe_document():
    """Test creating data frame documents."""
    columns = [Column(colid=0, name='Name'), Column(colid=1, name='Age')]
    df = pd.DataFrame(
        data=[['Alice', 23], ['Bob', 31], ['Claire', 28]],
        columns=columns,
        index=[1, 2, 0]
    )
    # Index-key document
    doc = DataFrameDocument(df=df)
    data = list()
    for row in doc.reader(schema=columns):
        data.append([row.values[0], row.values[1]])
    assert data == [['Claire', 28], ['Alice', 23], ['Bob', 31]]
    # Primary key document
    doc = DataFrameDocument(df=df, primary_key='Age')
    data = list()
    for row in doc.reader(schema=columns):
        data.append([row.values[0], row.values[1]])
    assert data == [['Alice', 23], ['Claire', 28], ['Bob', 31]]
    # Primary key with multiple columns
    df = pd.DataFrame(
        data=[['Alice', 23], ['Claire', 28], ['Bob', '30+'], ['Bob', 31]],
        columns=columns
    )
    doc = DataFrameDocument(df=df, primary_key=['Name', 'Age'])
    data = list()
    for row in doc.reader(schema=columns):
        data.append([row.values[0], row.values[1]])
    assert data == [['Alice', 23], ['Bob', 31], ['Bob', '30+'], ['Claire', 28]]


def test_dataframe_rows():
    """Test accessing and iterating over rows in a data frame."""
    df = pd.DataFrame(
        data=[['Alice', 23], ['Bob', 31], ['Claire', 28]],
        columns=['Name', 'Age'],
        index=['A', 'B', 'C']
    )
    rows = Rows(df=df)
    index = 0
    for row in rows:
        r = rows[index]
        assert row[0] == r['Name']
        assert row['Name'] == r[0]
        assert row['Age'] == r['Age']
        index += 1
    assert index == 3
    # Repeat previous tests with the same rows object.
    index = 0
    for row in rows:
        r = rows[index]
        assert row[0] == r['Name']
        assert row['Name'] == r[0]
        assert row['Age'] == r['Age']
        index += 1
    assert index == 3
