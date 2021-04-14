# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive descriptor."""

import pytest

from histore.archive.manager.mem import VolatileArchiveManager
from histore.document.snapshot import InputDescriptor

import histore.util as util


def test_volatile_archive_manager(dataset):
    """Test functionality of the volatile archive manager."""
    manager = VolatileArchiveManager()
    assert len(manager.archives()) == 0
    # Create archive
    now = util.utc_now()
    descriptor = manager.create(
        name='First archive',
        description='My first archive',
        primary_key='SSN',
        doc=dataset,
        snapshot=InputDescriptor(
            valid_time=now,
            description='First snapshot',
            action={'x': 1}
        )
    )
    assert descriptor.identifier() is not None
    assert descriptor.name() == 'First archive'
    assert descriptor.description() == 'My first archive'
    assert descriptor.primary_key() == [0]
    assert len(manager.archives()) == 1
    # Create archive with existing name.
    with pytest.raises(ValueError):
        manager.create(name='First archive')
    # List archive(s)
    descriptor = manager.list()[0]
    assert descriptor.identifier() is not None
    assert descriptor.name() == 'First archive'
    assert descriptor.description() == 'My first archive'
    assert descriptor.primary_key() == [0]
    archive = manager.get(descriptor.identifier())
    assert archive is not None
    assert archive.snapshots()[0].valid_time == now
    assert archive.snapshots()[0].description == 'First snapshot'
    assert archive.snapshots()[0].action == {'x': 1}
    # Rename the archive.
    manager.rename(descriptor.identifier(), 'Some archive')
    assert manager.get_by_name('My first archive') is None
    assert manager.get_by_name('Some archive') is not None
    # No error when archive name is identical to new name
    manager.rename(descriptor.identifier(), 'Some archive')
    # Error when renaming an unknown archive.
    with pytest.raises(ValueError):
        manager.rename('unknown', 'My archive')
    # Error when renaming to an existing archive.
    manager.create(name='First archive')
    with pytest.raises(ValueError):
        manager.rename(descriptor.identifier(), 'First archive')
    # Delete the archive
    manager.delete(descriptor.identifier())
    assert len(manager.archives()) == 1
    manager.delete(descriptor.identifier())
    # Error when accessing non-existing archive
    with pytest.raises(ValueError):
        manager.get(descriptor.identifier())
