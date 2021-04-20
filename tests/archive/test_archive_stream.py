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


def test_stream_keyed_archive(tmpdir):
    """Test committing snaposhots into an unkeyed archive."""
    # -- Setup - Create archive in main memory --------------------------------
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=True, primary_key=[0, 1]))
    for df in [DF1, DF2, DF3]:
        doc = DataFrameDocument(df=df)
        archive.commit(doc)
    # -- Stream first snapshot ------------------------------------------------
    stream = archive.stream(version=0)
    assert stream.columns == ['Name', 'Age']
    with stream.open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 23]), (1, ['Alice', 32]), (2, ['Bob', 45]), (3, ['Claire', 27])]
    with archive.stream(version=1).open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(1, ['Alice', 32]), (4, ['Bob', 44]), (3, ['Claire', 27]), (5, ['Dave', 23])]
    with archive.stream().open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(1, ['Alice', 32]), (4, ['Bob', 44]), (3, ['Claire', 27]), (6, ['Dave', 33])]
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        archive.stream(version=5)


def test_stream_unkeyed_archive():
    """Test committing snaposhots into an unkeyed archive."""
    # -- Setup - Create archive in main memory --------------------------------
    archive = Archive()
    for df in [DF1, DF2, DF3]:
        doc = DataFrameDocument(df=df)
        archive.commit(doc)
    # -- Stream first snapshot ------------------------------------------------
    stream = archive.stream(version=0)
    assert stream.columns == ['Name', 'Age']
    with stream.open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 32]), (1, ['Bob', 45]), (2, ['Claire', 27]), (3, ['Alice', 23])]
    with archive.stream(version=1).open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 32]), (1, ['Bob', 44]), (2, ['Claire', 27]), (3, ['Dave', 23])]
    with archive.stream().open() as s:
        values = ([(rowidx, values) for _, rowidx, values in s])
        assert values == [(0, ['Alice', 32]), (1, ['Bob', 44]), (2, ['Claire', 27]), (3, ['Dave', 33])]
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        archive.stream(version=5)
