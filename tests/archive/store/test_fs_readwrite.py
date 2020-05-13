# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive reader and archive writer of the file system
archive store.
"""

import os
import pandas as pd

from histore.archive.base import Archive
from histore.archive.store.fs.reader import ArchiveFileReader
from histore.archive.store.fs.writer import ArchiveFileWriter
from histore.archive.timestamp import Timestamp, TimeInterval


def test_archive_read_write(tmpdir):
    """Test writing an exsiting archive that uses row indices as keys to file
    and then reading it.
    """
    # Create archive in main memory
    archive = Archive()
    # First snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Second snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 44], ['Claire', 27], ['Dave', 23]],
        index=[0, 1, 2, 3],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Third snapshot
    df = pd.DataFrame(
        data=[['Dave', 33], ['Claire', 27], ['Bob', 44], ['Alice', 32]],
        index=[3, 2, 1, 0],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Fourth snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Eve', 25], ['Claire', 27], ['Bob', 44]],
        index=[0, None, 2, 1],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    # Output the archive to an uncompressed file and read it.
    filename = os.path.join(str(tmpdir), 'archive.json')
    writer = ArchiveFileWriter(filename=filename)
    reader = archive.reader()
    while reader.has_next():
        row = reader.next()
        writer.write_archive_row(row)
    writer.close()
    reader = ArchiveFileReader(filename=filename)
    validate_rowindex_archive(reader)
    # Output the archive to a gzip compressed file and read it.
    filename = os.path.join(str(tmpdir), 'archive.json.gz')
    writer = ArchiveFileWriter(filename=filename, compression='gzip')
    reader = archive.reader()
    while reader.has_next():
        row = reader.next()
        writer.write_archive_row(row)
    writer.close()
    reader = ArchiveFileReader(filename=filename, compression='gzip')
    validate_rowindex_archive(reader)


def validate_rowindex_archive(reader):
    rows = dict()
    while reader.has_next():
        row = reader.next()
        assert row.rowid == row.key.value
        rows[row.rowid] = row
    assert len(rows) == 5
    ts = Timestamp(intervals=TimeInterval(0, 3))
    for i in range(3):
        assert rows[i].timestamp.is_equal(ts)
    assert rows[3].timestamp.is_equal(Timestamp(intervals=TimeInterval(0, 2)))
    assert rows[4].timestamp.is_equal(Timestamp(version=3))
    assert rows[0].cells[0].value == 'Alice'
    assert rows[1].cells[0].value == 'Bob'
    assert rows[2].cells[0].value == 'Claire'
    names = [v.value for v in rows[3].cells[0].values]
    assert 'Alice' in names
    assert 'Dave' in names
    assert rows[4].cells[0].value == 'Eve'
