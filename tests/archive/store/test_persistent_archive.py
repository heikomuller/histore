# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the persistent archive."""

import os
import pandas as pd
import pytest

from histore.archive.base import PersistentArchive
from histore.archive.timestamp import Timestamp, TimeInterval
from histore.document.csv.base import CSVFile


DIR = os.path.dirname(os.path.realpath(__file__))
WATERSHED_1 = os.path.join(DIR, '../../.files/y43c-5n92.tsv.gz')


def test_persistent_archive(tmpdir):
    """Test merging snapshots into a persistent archive."""
    archive = PersistentArchive(
        basedir=os.path.join(str(tmpdir), 'archive'),
        replace=True,
        primary_key='Name'
    )
    # First snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
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


def test_merge_file_without_pk(tmpdir):
    """Test merging snapshots of the NYC Watershed data into an archive without
    using a primary key (issue #19).
    """
    archive = PersistentArchive(
        basedir=str(tmpdir),
        replace=True
    )
    s = archive.commit(WATERSHED_1)
    diff = archive.diff(s.version - 1, s.version)
    assert len(diff.schema().insert()) == 10
    assert len(diff.rows().insert()) == 1793


@pytest.mark.parametrize(
    'doc',
    [
        pd.read_csv(WATERSHED_1, compression='gzip', delimiter='\t'),
        CSVFile(WATERSHED_1),
        WATERSHED_1
    ]
)
@pytest.mark.parametrize('replace', [True, False])
def test_watershed_archive(doc, replace, tmpdir):
    """Test merging snapshots of the NYC Watershed data into an archive."""
    archive = PersistentArchive(
        basedir=str(tmpdir),
        primary_key=['Site', 'Date'],
        replace=replace
    )
    s = archive.commit(doc)
    diff = archive.diff(s.version - 1, s.version)
    assert len(diff.schema().insert()) == 10
    assert len(diff.rows().insert()) == 1793
    # -- Recreate the archive instance.
    archive = PersistentArchive(
        basedir=str(tmpdir),
        primary_key=['Site', 'Date'],
        replace=False
    )
    s = archive.commit(doc)
    diff = archive.diff(s.version - 1, s.version)
    assert len(diff.schema().insert()) == 0
    assert len(diff.rows().insert()) == 0
