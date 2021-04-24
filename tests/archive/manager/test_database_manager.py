# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the database archive manager."""

import os

from histore.archive.manager.db.base import DBArchiveManager

import histore.config as config


def test_init_database_manager(tmpdir):
    """Test creating database archive manager with database on local file system."""
    dbfile = os.path.join(tmpdir, 'db.sql')
    os.environ[config.ENV_HISTORE_DBCONNECT] = 'sqlite:///{}'.format(dbfile)
    DBArchiveManager(basedir=tmpdir, create=True)
    assert os.path.isfile(dbfile)
    del os.environ[config.ENV_HISTORE_DBCONNECT]
