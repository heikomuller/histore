# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for merging snapshots that are keyed by the row index."""

import pandas as pd

from histore.archive.base import Archive
from histore.archive.timestamp import Timestamp, TimeInterval


def test_full_snapshot_merge():
    """Test merging full snapshots into an archive without a primary key."""
    archive = Archive()
    # Version 0
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 1], ['Claire', 1], ['Alice', 2]],
        columns=['Name', 'Index']
    )
    s = archive.commit(df)
    assert s.version == 0
    df = archive.checkout(s.version)
    assert list(df.index) == [0, 1, 2, 3]
    assert [c.colid for c in df.columns] == [0, 1]
    assert list(df['Index']) == [1, 1, 1, 2]
    # Version 1
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 2], ['Claire', 1], ['Dave', 2]],
        index=[0, 1, 2, 3],
        columns=['Name', 'Index']
    )
    s = archive.commit(df)
    assert s.version == 1
    df = archive.checkout(s.version)
    assert list(df.index) == [0, 1, 2, 3]
    assert [c.colid for c in df.columns] == [0, 1]
    assert list(df['Index']) == [1, 2, 1, 2]
    # Version 2
    df = pd.DataFrame(
        data=[['Alice', 1], ['Dave', 2], ['Bob', 3], ['Claire', 4]],
        index=[0, 3, 1, 2],
        columns=['Name', 'Index']
    )
    s = archive.commit(df)
    assert s.version == 2
    df = archive.checkout(s.version)
    assert list(df.index) == [0, 3, 1, 2]
    assert [c.colid for c in df.columns] == [0, 1]
    assert list(df['Index']) == [1, 2, 3, 4]
    # Version 3
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 3], ['Eve', 5], ['Claire', 4]],
        index=[0, 1, -1, 2],
        columns=['Name', 'Index']
    )
    s = archive.commit(df)
    assert s.version == 3
    df = archive.checkout(s.version)
    assert list(df.index) == [0, 1, 4, 2]
    assert [c.colid for c in df.columns] == [0, 1]
    assert list(df['Index']) == [1, 3, 5, 4]


def test_mixed_snapshot_merge():
    """Test merging full and partial snapshots into an archive without a
    primary key.
    """
    archive = Archive()
    # Version 0
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 1], ['Claire', 1], ['Alice', 2]],
        columns=['Name', 'Index']
    )
    archive.commit(df)
    # Version 1
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 44], ['Claire', 27], ['Dave', 23]],
        index=[0, 1, 2, 3],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Version 2
    df = pd.DataFrame(
        data=[['Dave', 33], ['Claire', 27], ['Bob', 44], ['Alice', 32]],
        index=[3, 2, 1, 0],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Version 3
    df = pd.DataFrame(
        data=[['Alice', 32], ['Eve', 25], ['Claire', 27], ['Bob', 44]],
        index=[0, None, 2, 1],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Version 4
    df = pd.DataFrame(
        data=[['Alice', 31]],
        index=[0],
        columns=['Name', 'Age']
    )
    archive.commit(df, partial=True)
    # Version 5
    df = pd.DataFrame(
        data=[
            ['Alice', 31],
            ['Eve', 25],
            ['Claire', 27],
            ['Bob', 44],
            ['Dave', 34]
        ],
        index=[0, 4, 2, 1, 3],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Validate
    rows, row_order = dict(), list()
    reader = archive.reader()
    while reader.has_next():
        row = reader.next()
        rows[row.rowid] = row
        row_order.append(row.rowid)
    assert row_order == [0, 1, 2, 3, 4]
    assert len(rows) == 5
    for rid in range(5):
        assert rid in rows
    ts = Timestamp(intervals=[TimeInterval(0, 5)])
    for rid in range(3):
        assert rows[rid].timestamp.is_equal(ts)
    ts = Timestamp(intervals=[TimeInterval(0, 2), TimeInterval(5)])
    assert rows[3].timestamp.is_equal(ts)
    ts = Timestamp(intervals=[TimeInterval(3, 5)])
    assert rows[4].timestamp.is_equal(ts)
