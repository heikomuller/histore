# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for special cases in the archive file store."""

from pathlib import Path

import os
import pytest

from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.fs.reader import ArchiveFileReader


def test_archive_commit(archives, tmpdir):
    """Test merging snapshots into a volatile archive."""
    archive = ArchiveFileStore(basedir=tmpdir, replace=True, primary_key=[0])
    assert archive.is_empty()
    assert archive.primary_key() == [0]
    schema, rows, version = archives[1]
    writer = archive.get_writer()
    for row in rows:
        writer.write_archive_row(row)
    snapshots = archive.get_snapshots().append(version)
    archive.commit(schema=schema, writer=writer, snapshots=snapshots)
    assert not archive.is_empty()
    assert archive.primary_key() == [0]
    row_counter = 0
    with archive.get_reader() as reader:
        for row in reader:
            reader.next()
            row_counter += 1
    assert reader.next() is None
    assert row_counter == 1
    assert archive.get_schema().at_version(version=version) == ['A', 'B']
    # Re-create the archive without replacing the files.
    ArchiveFileStore(basedir=tmpdir, replace=False, primary_key=[0])
    assert not archive.is_empty()
    assert archive.primary_key() == [0]
    assert archive.get_schema().at_version(version=version) == ['A', 'B']


def test_archive_rollback(archives, tmpdir):
    """Test rollback for volatile archive store."""
    archive = ArchiveFileStore(basedir=tmpdir, replace=True, primary_key=[0])
    # First snapshot.
    schema1, rows1, version1 = archives[1]
    writer1 = archive.get_writer()
    for row in rows1:
        writer1.write_archive_row(row)
    snapshots = archive.get_snapshots().append(version1)
    archive.commit(schema=schema1, writer=writer1, snapshots=snapshots)
    # Second snapshot.
    schema, rows, version = archives[2]
    writer = archive.get_writer()
    for row in rows:
        writer.write_archive_row(row)
    snapshots = archive.get_snapshots().append(version)
    archive.commit(schema=schema, writer=writer, snapshots=snapshots)
    row_counter = 0
    with archive.get_reader() as reader:
        for row in reader:
            row_counter += 1
    assert reader.next() is None
    assert row_counter == 2
    # Rollback to first snapshot.
    writer1 = archive.get_writer()
    for row in rows1:
        writer1.write_archive_row(row)
    archive.rollback(schema=schema1, writer=writer1, version=version1)
    assert not archive.is_empty()
    assert archive.primary_key() == [0]
    row_counter = 0
    with archive.get_reader() as reader:
        for row in reader:
            row_counter += 1
    assert reader.next() is None
    assert row_counter == 1
    assert archive.get_schema().at_version(version=version1) == ['A', 'B']
    # Re-create the archive without replacing the files.
    ArchiveFileStore(basedir=tmpdir, replace=False, primary_key=[0])
    assert not archive.is_empty()
    assert archive.primary_key() == [0]
    assert archive.get_schema().at_version(version=version1) == ['A', 'B']
    row_counter = 0
    with archive.get_reader() as reader:
        for row in reader:
            row_counter += 1
    assert reader.next() is None
    assert row_counter == 1


def test_init_store(archives, tmpdir):
    """Test initializing the file store."""
    Path(os.path.join(tmpdir, 'rows.dat')).touch()
    Path(os.path.join(tmpdir, 'b.json')).touch()
    ArchiveFileStore(basedir=tmpdir, replace=True)
