# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive descriptor."""

import os
import pandas as pd
import pytest

from datetime import datetime

from histore.archive.manager.db.base import DBArchiveManager
from histore.archive.manager.db.database import DB, TEST_URL
from histore.archive.manager.fs import FileSystemArchiveManager
from histore.archive.manager.persist import PersistentArchiveManager
from histore.archive.serialize.base import DEFAULT
from histore.document.base import InputDescriptor
from histore.document.schema import Column

import histore.config as config
import histore.util as util


@pytest.mark.parametrize(
    'ManagerCls,kwargs',
    [
        (FileSystemArchiveManager, dict()),
        (DBArchiveManager, dict({'db': DB(connect_url=TEST_URL)}))
    ]
)
def test_create_archive(ManagerCls, kwargs, dataset, tmpdir):
    """Test functionality of the persistent archive manager."""
    # -- Setup ----------------------------------------------------------------
    kwargs['basedir'] = str(tmpdir)
    kwargs['create'] = True
    # -- Create empty manager instance ----------------------------------------
    manager = ManagerCls(**kwargs)
    assert len(manager.archives()) == 0
    # -- Add first archive ----------------------------------------------------
    now = util.utc_now()
    descriptor = manager.create(
        name='First archive',
        description='My first archive',
        primary_key='SSN',
        encoder='histore.tests.encode.TestEncoder',
        decoder='histore.tests.encode.test_decoder',
        serializer=DEFAULT,
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
    descriptor = manager.list()[0]
    assert descriptor.identifier() is not None
    assert descriptor.name() == 'First archive'
    assert descriptor.description() == 'My first archive'
    assert descriptor.primary_key() == [0]
    archive = manager.get(descriptor.identifier())
    assert archive.snapshots()[0].valid_time == now
    assert archive.snapshots()[0].description == 'First snapshot'
    assert archive.snapshots()[0].action == {'x': 1}
    assert archive is not None
    with pytest.raises(ValueError):
        manager.get('unknown')
    # -- Reload the archive manager -------------------------------------------
    kwargs['create'] = False
    manager = ManagerCls(**kwargs)
    assert len(manager.archives()) == 1
    archive = manager.get(descriptor.identifier())
    assert archive.store.primary_key() == [0]
    assert archive is not None
    # -- Error when creating archive with same name.
    with pytest.raises(ValueError):
        manager.create(
            name='First archive',
            description='My first archive',
            primary_key='SSN',
            encoder='histore.tests.encode.TestEncoder',
            decoder='histore.tests.encode.test_decoder',
            serializer=DEFAULT,
            doc=dataset,
            snapshot=InputDescriptor(
                valid_time=now,
                description='First snapshot',
                action={'x': 1}
            )
        )
    # Delete the archive
    manager.delete(descriptor.identifier())
    assert len(manager.archives()) == 0
    manager.delete(descriptor.identifier())
    # Error cases
    with pytest.raises(ValueError):
        manager.get(descriptor.identifier())


@pytest.mark.parametrize(
    'ManagerCls,kwargs',
    [
        (FileSystemArchiveManager, dict()),
        (DBArchiveManager, dict({'db': DB(connect_url=TEST_URL)}))
    ]
)
def test_create_archive_error(ManagerCls, kwargs, tmpdir):
    """Test error when creating and archive with primary key but no initial
    snapshot document.
    """
    # -- Setup ----------------------------------------------------------------
    kwargs['basedir'] = str(tmpdir)
    kwargs['create'] = True
    manager = ManagerCls(**kwargs)
    # -- Error for invalid parameters -----------------------------------------
    with pytest.raises(ValueError):
        manager.create(
            name='First archive',
            description='My first archive',
            encoder='histore.tests.encode.TestEncoder',
            decoder='histore.tests.encode.test_decoder',
            serializer=DEFAULT,
            primary_key='SSN'
        )


@pytest.mark.parametrize(
    'ManagerCls,kwargs',
    [
        (FileSystemArchiveManager, dict()),
        (DBArchiveManager, dict({'db': DB(connect_url=TEST_URL)}))
    ]
)
def test_create_manager(ManagerCls, kwargs, tmpdir):
    """Test creating an archive manager in a non-existing base directory."""
    # -- Create empty manager instance ----------------------------------------
    # -- Setup ----------------------------------------------------------------
    basedir = os.path.join(tmpdir, 'archives')
    kwargs['basedir'] = basedir
    kwargs['create'] = True
    ManagerCls(**kwargs)
    assert os.path.isdir(basedir)


@pytest.mark.parametrize(
    'ManagerCls,kwargs',
    [
        (FileSystemArchiveManager, dict()),
        (DBArchiveManager, dict({'db': DB(connect_url=TEST_URL)}))
    ]
)
def test_encoder_default(ManagerCls, kwargs, tmpdir):
    """Test persistent archives with default Json encoder."""
    # -- Setup ----------------------------------------------------------------
    kwargs['basedir'] = str(tmpdir)
    kwargs['create'] = True
    # -- Use the default encoder and decoder ----------------------------------
    manager = ManagerCls(**kwargs)
    descriptor = manager.create(name='Archive')
    archive = manager.get(descriptor.identifier())
    dt = datetime.now()
    archive.commit(pd.DataFrame(data=[[dt]]))
    df = archive.checkout()
    assert df.shape == (1, 1)
    assert df.iloc[0][0] == dt
    assert isinstance(df.iloc[0][0], datetime)
    # DataFrame schema
    for col in df.columns:
        assert isinstance(col, Column)
        assert col.colid >= 0
        assert col.colidx >= 0


@pytest.mark.parametrize(
    'ManagerCls,kwargs',
    [
        (FileSystemArchiveManager, dict()),
        (DBArchiveManager, dict({'db': DB(connect_url=TEST_URL)}))
    ]
)
def test_encoder_custom(ManagerCls, kwargs, tmpdir):
    """Test persistent archives with custom Json encoder."""
    # -- Setup ----------------------------------------------------------------
    kwargs['basedir'] = str(tmpdir)
    kwargs['create'] = True
    # Use the default encoder and decoder.
    manager = ManagerCls(**kwargs)
    descriptor = manager.create(
        name='Archive',
        encoder='histore.tests.encode.TestEncoder',
        decoder='histore.tests.encode.test_decoder'
    )
    kwargs['create'] = False
    manager = ManagerCls(**kwargs)
    archive = manager.get(descriptor.identifier())
    dt = datetime.now()
    archive.commit(pd.DataFrame(data=[[dt, 'A']]))
    df = archive.checkout()
    assert df.shape == (1, 2)
    assert df.iloc[0][0] == dt.isoformat()
    assert isinstance(df.iloc[0][0], str)


def test_manager_factory(tmpdir):
    """Test creating persistent archive manager using the manager factory."""
    # -- Setup ----------------------------------------------------------------
    os.environ[config.ENV_HISTORE_BASEDIR] = str(tmpdir)
    # -- Get file system manager if no connect URL is given -------------------
    manager = PersistentArchiveManager()
    assert isinstance(manager, FileSystemArchiveManager)
    # -- Get database manager if connect URL is given -------------------------
    manager = PersistentArchiveManager(dbconnect=TEST_URL)
    assert isinstance(manager, DBArchiveManager)
    # -- Cleanup --------------------------------------------------------------
    del os.environ[config.ENV_HISTORE_BASEDIR]


@pytest.mark.parametrize(
    'ManagerCls,kwargs',
    [
        (FileSystemArchiveManager, dict()),
        (DBArchiveManager, dict({'db': DB(connect_url=TEST_URL)}))
    ]
)
def test_rename_archive(ManagerCls, kwargs, dataset, tmpdir):
    """Test functionality of the persistent archive manager."""
    # -- Setup ----------------------------------------------------------------
    kwargs['basedir'] = str(tmpdir)
    kwargs['create'] = True
    # -- Create empty manager instance ----------------------------------------
    manager = ManagerCls(**kwargs)
    assert len(manager.archives()) == 0
    # -- Create two archives archive ------------------------------------------
    arch_1 = manager.create(
        name='First archive',
        description='My first archive',
        primary_key='SSN',
        doc=dataset
    )
    arch_2 = manager.create(name='Second archive', description='Another one')
    # -- Reload the archive manager -------------------------------------------
    kwargs['create'] = False
    manager = ManagerCls(**kwargs)
    manager.rename(identifier=arch_2.identifier(), name='My archive')
    assert manager.get_by_name('First archive') is not None
    assert manager.get_by_name('My archive') is not None
    assert manager.get_by_name('Second archive') is None
    manager.rename(identifier=arch_2.identifier(), name='My archive')
    # -- Error when renaming a non-exitent archive ----------------------------
    with pytest.raises(ValueError):
        manager.rename(identifier='unknown', name='My archive')
    # -- Error when renaming to an existing archive name ----------------------
    with pytest.raises(ValueError):
        manager.rename(identifier=arch_2.identifier(), name=arch_1.name())
