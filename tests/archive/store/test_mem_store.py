# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the volatile archive store."""

from histore.archive.store.mem.base import VolatileArchiveStore


def test_archive_commit(archives):
    """Test merging snapshots into a volatile archive."""
    archive = VolatileArchiveStore(primary_key=[0])
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
    with archive.get_reader() as reader:
        row_counter = sum([1 for row in reader])
    assert row_counter == 1
    assert archive.get_schema().at_version(version=version) == ['A', 'B']


def test_archive_rollback(archives):
    """Test rollback for volatile archive store."""
    archive = VolatileArchiveStore(primary_key=[0])
    # First snapshot.
    schema1, rows, version1 = archives[1]
    writer1 = archive.get_writer()
    for row in rows:
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
    with archive.get_reader() as reader:
        row_counter = sum([1 for row in reader])
    assert row_counter == 2
    # Rollback to first snapshot.
    archive.rollback(schema=schema1, writer=writer1, version=version1)
    assert not archive.is_empty()
    assert archive.primary_key() == [0]
    with archive.get_reader() as reader:
        row_counter = sum([1 for row in reader])
    assert row_counter == 1
    assert archive.get_schema().at_version(version=version1) == ['A', 'B']
