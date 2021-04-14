# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for base functionality and edge cases of the archive base module."""

import os
import pandas as pd
import pytest

from histore.archive.base import Archive, to_input
from histore.document.csv.base import CSVFile
from histore.document.csv.read import SimpleCSVDocument
from histore.document.mem.dataframe import DataFrameDocument


DIR = os.path.dirname(os.path.realpath(__file__))
FILENAME = os.path.join(DIR, '../.files/agencies.csv')


def test_input_from_dataframe():
    """Test creating an input document from a data frame."""
    df = pd.DataFrame(data=[[1, 2], [3, 4]], columns=['A', 'B'])
    doc = to_input(doc=df)
    assert isinstance(doc, DataFrameDocument)


@pytest.mark.parametrize('file', [FILENAME, CSVFile(FILENAME)])
def test_input_from_file(file):
    """Test getting input document from a CSV file."""
    doc = to_input(doc=file)
    assert isinstance(doc, SimpleCSVDocument)
    doc = to_input(doc=doc)
    assert isinstance(doc, SimpleCSVDocument)


def test_special_checkout_cases():
    """Test various special (error) cases for commit and checkout."""
    archive = Archive()
    # Version 0
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 1], ['Claire', 1], ['Alice', 2]],
        columns=['Name', 'Index']
    )
    s = archive.commit(DataFrameDocument(df))
    assert s.version == 0
    # Last version is checked out by default.
    df = archive.checkout(version=0)
    assert list(df.index) == [0, 1, 2, 3]
    df = archive.checkout()
    assert list(df.index) == [0, 1, 2, 3]
    # Partial merge with given origin.
    df = pd.DataFrame(data=[['Alice', 1]], columns=df.columns)
    s = archive.commit(df, origin=0)
    assert s.version == 1
    # Checkout an unknown version.
    with pytest.raises(ValueError):
        archive.checkout(version=10)


def test_partial_commit_to_empty_archive():
    """Test error when committing a partial snapshot to an empty archive."""
    df = pd.DataFrame(data=[[1]])
    with pytest.raises(ValueError):
        Archive().commit(df, partial=True)
