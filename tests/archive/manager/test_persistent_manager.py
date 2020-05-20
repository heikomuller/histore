# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive descriptor."""

import os
import pytest

from histore.archive.manager.fs import PersistentArchiveManager

import histore.config as config


def test_persistent_archive_manager(tmpdir):
    """Test functionality of the persistent archive manager."""
    manager = PersistentArchiveManager(basedir=str(tmpdir))
    assert len(manager.archives()) == 0
    descriptor = manager.create(
        name='First archive',
        description='My first archive',
        primary_key='SSN'
    )
    assert descriptor.identifier() is not None
    assert descriptor.name() == 'First archive'
    assert descriptor.description() == 'My first archive'
    assert descriptor.primary_key() == ['SSN']
    assert len(manager.archives()) == 1
    descriptor = manager.list()[0]
    assert descriptor.identifier() is not None
    assert descriptor.name() == 'First archive'
    assert descriptor.description() == 'My first archive'
    assert descriptor.primary_key() == ['SSN']
    archive = manager.get(descriptor.identifier())
    assert archive is not None
    with pytest.raises(ValueError):
        manager.get('unknown')
    # Reload the archive manager.
    os.environ[config.ENV_HISTORE_BASEDIR] = str(tmpdir)
    manager = PersistentArchiveManager()
    assert len(manager.archives()) == 1
    archive = manager.get(descriptor.identifier())
    assert archive is not None
    # Delete the archive
    manager.delete(descriptor.identifier())
    assert len(manager.archives()) == 0
    manager.delete(descriptor.identifier())
    # Error cases
    with pytest.raises(ValueError):
        manager.get(descriptor.identifier())
    # Cleanup the environment
    del os.environ[config.ENV_HISTORE_BASEDIR]
