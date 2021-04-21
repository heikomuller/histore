# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the dataset snapshot stream."""

import pandas as pd
import pytest

from histore.archive.base import Archive
from histore.archive.store.fs.base import ArchiveFileStore
from histore.document.df import DataFrameDocument


DF1 = pd.DataFrame(
    data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]],
    columns=['Name', 'Age'],
    dtype=object
)
DF2 = pd.DataFrame(
    data=[['Alice', 32], ['Bob', 44], ['Claire', 27], ['Dave', 23]],
    index=[0, 1, 2, 3],
    columns=['Name', 'Age'],
    dtype=object
)
DF3 = pd.DataFrame(
    data=[['Dave', 33], ['Claire', 27], ['Bob', 44], ['Alice', 32]],
    index=[3, 2, 1, 0],
    columns=['Name', 'Age'],
    dtype=object
)


def test_iterate_over_stream():
    """Test getting a sorted document form an stream reader."""
    archive = Archive()
    archive.commit(doc=DataFrameDocument(df=DF1))
    rows = list()
    with archive.open(version=0).open() as reader:
        for row in reader:
            rows.append(row)
    assert rows == [
        (0, 0, ['Alice', 32]),
        (1, 1, ['Bob', 45]),
        (2, 2, ['Claire', 27]),
        (3, 3, ['Alice', 23])
    ]


def test_stream_keyed_archive(tmpdir):
    """Test committing snaposhots into an unkeyed archive."""
    # -- Setup - Create archive in main memory --------------------------------
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=True, primary_key=[0, 1]))
    for df in [DF1, DF2, DF3]:
        doc = DataFrameDocument(df=df)
        archive.commit(doc)
    # -- Stream snapshots -----------------------------------------------------
    stream = archive.open(version=0)
    assert stream.columns == ['Name', 'Age']
    with stream.open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 23]), (1, ['Alice', 32]), (2, ['Bob', 45]), (3, ['Claire', 27])]
    with archive.open(version=1).open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(1, ['Alice', 32]), (4, ['Bob', 44]), (3, ['Claire', 27]), (5, ['Dave', 23])]
    with archive.open().open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(1, ['Alice', 32]), (4, ['Bob', 44]), (3, ['Claire', 27]), (6, ['Dave', 33])]
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        archive.open(version=5)


def test_stream_to_data_frame():
    """Test reading a data frame from an unkeyed archive."""
    # -- Setup - Create archive in main memory --------------------------------
    archive = Archive()
    for df in [DF1, DF2, DF3]:
        doc = DataFrameDocument(df=df)
        archive.commit(doc)
    # -- Read dataframes for first two snapshots ------------------------------
    #
    # The snapshots are only identical if the data frames where sorted by the
    # data frame index. Thus, the third snapshot will return a data frame in
    # different order.
    pd.testing.assert_frame_equal(archive.open(version=0).to_df(), DF1)
    pd.testing.assert_frame_equal(archive.open(version=1).to_df(), DF2)


def test_stream_sorted():
    """Test getting a sorted document form an stream reader."""
    archive = Archive()
    archive.commit(doc=DataFrameDocument(df=DF1))
    names = list(archive.open(version=0).sorted(keys=[0]).to_df()['Name'])
    assert names == ['Alice', 'Alice', 'Bob', 'Claire']


def test_stream_unkeyed_archive():
    """Test committing snaposhots into an unkeyed archive."""
    # -- Setup - Create archive in main memory --------------------------------
    archive = Archive()
    for df in [DF1, DF2, DF3]:
        doc = DataFrameDocument(df=df)
        archive.commit(doc)
    # -- Stream snapshots -----------------------------------------------------
    stream = archive.open(version=0)
    assert stream.columns == ['Name', 'Age']
    with stream.open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 32]), (1, ['Bob', 45]), (2, ['Claire', 27]), (3, ['Alice', 23])]
    with archive.open(version=1).open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 32]), (1, ['Bob', 44]), (2, ['Claire', 27]), (3, ['Dave', 23])]
    with archive.open().open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 32]), (1, ['Bob', 44]), (2, ['Claire', 27]), (3, ['Dave', 33])]
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        archive.open(version=5)
