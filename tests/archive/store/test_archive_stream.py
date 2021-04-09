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


SNAPSHOT_1 = [['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]]
SNAPSHOT_2 = [['Alice', 32], ['Bob', 44], ['Claire', 27], ['Dave', 23]]
SNAPSHOT_3 = [['Dave', 33], ['Eve', 25], ['Claire', 27], ['Bob', 44], ['Alice', 32]]
SNAPSHOT_4 = [['Alice', 32], ['Eve', 25], ['Claire', 27], ['Bob', 44]]


def test_load_archive_errors():
    """Error cases for loading archive from stream."""
    # -- Load into non-empty archive ------------------------------------------
    archive = Archive()
    # First snapshot
    df = pd.DataFrame(
        data=SNAPSHOT_1,
        columns=['Name', 'Age']
    )
    archive.commit(df)
    with pytest.raises(RuntimeError):
        archive.load_from_stream(df)
    # -- Load into archive with promary key -----------------------------------
    archive = Archive(primary_key=['Name'])
    with pytest.raises(RuntimeError):
        archive.load_from_stream(df)


def test_stream_archive():
    """Test streaming an archive snapshot."""
    # -- Setup - Create archive in main memory --------------------------------
    archive = Archive()
    # First snapshot
    df = pd.DataFrame(
        data=SNAPSHOT_1,
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Second snapshot
    df = pd.DataFrame(
        data=SNAPSHOT_2,
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Third snapshot
    df = pd.DataFrame(
        data=SNAPSHOT_3,
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Fourth snapshot
    df = pd.DataFrame(
        data=SNAPSHOT_4,
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # -- Stream first snapshot ------------------------------------------------
    with archive.stream(version=0).open() as s:
        assert [values for _, values in s] == SNAPSHOT_1
    with archive.stream(version=1).open() as s:
        assert [values for _, values in s] == SNAPSHOT_2
    with archive.stream(version=2).open() as s:
        assert [values for _, values in s] == SNAPSHOT_3
    with archive.stream().open() as s:
        assert [values for _, values in s] == SNAPSHOT_4
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        archive.stream(version=5)
