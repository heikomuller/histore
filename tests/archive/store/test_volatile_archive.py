# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the volatile archive."""

import os
import pandas as pd
import pytest

from histore.archive.base import VolatileArchive
from histore.archive.timestamp import Timestamp, TimeInterval


DIR = os.path.dirname(os.path.realpath(__file__))
WATERSHED_1 = os.path.join(DIR, '../../.files/y43c-5n92.tsv.gz')


def test_archive_commit_and_rollback(tmpdir):
    """Test merging snapshots into a volatile archive and rolling back."""
    archive = VolatileArchive(primary_key='Name')
    assert archive.is_empty()
    # First snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    assert not archive.is_empty()
    # Second snapshot
    df = pd.DataFrame(
        data=[['Alice', 33], ['Bob', 44], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Third snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 44], ['Claire', 27], ['Eve', 27]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Fourth snapshot
    df = pd.DataFrame(
        data=[['Eve', 27], ['Claire', 28], ['Bob', 44], ['Alice', 32]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Read created archive
    rows = dict()
    reader = archive.reader()
    while reader.has_next():
        row = reader.next()
        rows[row.rowid] = row
    assert len(rows) == 5
    keys = ['Alice', 'Bob', 'Claire', 'Dave', 'Eve']
    for rowid in range(5):
        assert rows[rowid].key.value == keys[rowid]
    ts = Timestamp(intervals=TimeInterval(0, 3))
    for i in range(3):
        assert rows[i].timestamp.is_equal(ts)
    assert rows[3].timestamp.is_equal(Timestamp(intervals=TimeInterval(0, 1)))
    assert rows[4].timestamp.is_equal(Timestamp(intervals=TimeInterval(2, 3)))
    assert sorted([v.value for v in rows[0].cells[1].values]) == [32, 33]
    # Rollback to version 1.
    archive.rollback(version=1)
    rows = dict()
    reader = archive.reader()
    while reader.has_next():
        row = reader.next()
        rows[row.rowid] = row
    assert len(rows) == 4
    keys = ['Alice', 'Bob', 'Claire', 'Dave']
    # Invalid rollback.
    with pytest.raises(ValueError):
        archive.rollback(-1)
