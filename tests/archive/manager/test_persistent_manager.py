# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive descriptor."""

import os
import pandas as pd
import pytest

from datetime import datetime

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
    manager = PersistentArchiveManager(exists=True)
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
    # Error case when using the exist flag.
    with pytest.raises(ValueError):
        PersistentArchiveManager(
            basedir=os.path.join(str(tmpdir), 'ABC'),
            exists=True
        )


def test_default_json_encoder(tmpdir):
    """Test persistent archives with default Json encoder."""
    # Use the default encoder and decoder.
    manager = PersistentArchiveManager(basedir=str(tmpdir))
    descriptor = manager.create(name='Archive')
    archive = manager.get(descriptor.identifier())
    dt = datetime.now()
    archive.commit(pd.DataFrame(data=[[dt]]))
    df = archive.checkout()
    assert df.shape == (1, 1)
    assert df.iloc[0][0] == dt
    assert isinstance(df.iloc[0][0], datetime)


def test_custom_json_encoder(tmpdir):
    """Test persistent archives with custom Json encoder."""
    # Use the default encoder and decoder.
    manager = PersistentArchiveManager(basedir=str(tmpdir))
    descriptor = manager.create(
        name='Archive',
        encoder='histore.tests.encode.TestEncoder',
        decoder='histore.tests.encode.test_decoder'
    )
    manager = PersistentArchiveManager(basedir=str(tmpdir))
    archive = manager.get(descriptor.identifier())
    dt = datetime.now()
    archive.commit(pd.DataFrame(data=[[dt, 'A']]))
    df = archive.checkout()
    assert df.shape == (1, 2)
    assert df.iloc[0][0] == dt.isoformat()
    assert isinstance(df.iloc[0][0], str)
