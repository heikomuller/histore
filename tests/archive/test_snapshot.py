# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""unit tests for snapshot descriptors and snapshot descriptor listings."""

import pytest

from histore.archive.snapshot import Snapshot, SnapshotListing
from histore.document.base import InputDescriptor

import histore.util as util


def test_append_snapshots():
    """Test appending new snapshot descriptors to a snapshot listing."""
    snapshots = SnapshotListing()
    assert snapshots.is_empty()
    assert snapshots.last_snapshot() is None
    snapshots = snapshots.append(snapshots.next_version())
    assert not snapshots.is_empty()
    assert snapshots.last_snapshot() is not None
    s = snapshots.last_snapshot()
    assert s.version == 0
    assert str(s).startswith('<Snapshot')
    snapshots = snapshots.append(
        version=snapshots.next_version(),
        descriptor=InputDescriptor(
            description='some text',
            action={'command': 'X'}
        )
    )
    s = snapshots.last_snapshot()
    assert s.version == 1
    assert s.description == 'some text'
    assert s.action == {'command': 'X'}
    assert len(snapshots) == 2
    assert snapshots.has_version(0)
    assert snapshots.has_version(1)
    assert not snapshots.has_version(2)


def test_create_snapshot_descriptor():
    """Test creating instances of the snapshot descriptor class."""
    s = Snapshot(version=0, valid_time=util.to_datetime('2020-05-01'))
    assert s.version == 0
    assert s.valid_time == util.to_datetime('2020-05-01')
    assert s.transaction_time is not None
    assert s.transaction_time == s.created_at
    assert s.description == ''
    assert s.action is None
    s = Snapshot(
        version=0,
        valid_time=util.to_datetime('2020-05-01'),
        transaction_time=util.to_datetime('2020-04-01'),
        description='some text'
    )
    assert s.valid_time == util.to_datetime('2020-05-01')
    assert s.transaction_time == util.to_datetime('2020-04-01')
    assert s.description == 'some text'


def test_snapshot_listing():
    """Test creating a listing of snapshot descriptors."""
    s1 = Snapshot(version=0, valid_time=util.to_datetime('2020-05-01'))
    s2 = Snapshot(version=1, valid_time=util.to_datetime('2020-05-02'))
    s3 = Snapshot(version=2, valid_time=util.to_datetime('2020-05-03'))
    listing = SnapshotListing(snapshots=[s1, s2, s3])
    # Get snapshots by identifier.
    for version in range(2):
        assert listing[version].version == version
        assert listing.get(version).version == version
    versions = list()
    for s in listing:
        versions.append(s.version)
    assert versions == [0, 1, 2]
    s = listing.at_time(util.to_datetime('2020-04-01'))
    assert s is None
    s = listing.at_time(util.to_datetime('2020-05-01T08:00:00'))
    assert s.version == 0
    s = listing.at_time(util.to_datetime('2020-05-10'))
    assert s.version == 2
    # Error when accessing snapshot with unknown identifier.
    with pytest.raises(KeyError):
        listing[4]
    # Error when adding snapshot with invalid version number.
    with pytest.raises(ValueError):
        listing.append(version=100)
    # Empty listing returns None for any time
    assert SnapshotListing().at_time(util.to_datetime('2020-04-01')) is None
    # Error case for snapshots with invalid 'vaid_time' order
    with pytest.raises(ValueError):
        SnapshotListing(snapshots=[s1, s3, s2])
    s1 = Snapshot(version=0, valid_time=util.to_datetime('2020-05-01'))
    s2 = Snapshot(version=1, valid_time=util.to_datetime('2020-05-03'))
    s3 = Snapshot(version=2, valid_time=util.to_datetime('2020-05-02'))
    # Error case for snapshots with invalid 'vaid_time'
    with pytest.raises(ValueError):
        SnapshotListing(snapshots=[s1, s2, s3])


def test_snapshot_rollback():
    """Test rollback for a snapshot listing."""
    snapshots = SnapshotListing()
    snapshots = snapshots.append(snapshots.next_version())
    snapshots = snapshots.append(snapshots.next_version())
    snapshots = snapshots.append(snapshots.next_version())
    assert len(snapshots) == 3
    snapshots = snapshots.rollback(1)
    assert len(snapshots) == 2
    snapshots = snapshots.rollback(0)
    assert len(snapshots) == 1
