# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Factory pattern for persistent archive managers."""

from typing import Optional

from histore.archive.manager.base import ArchiveManager
from histore.archive.manager.db.base import DBArchiveManager
from histore.archive.manager.db.database import DB
from histore.archive.manager.fs import FileSystemArchiveManager


class PersistentArchiveManager(ArchiveManager):
    """Create an instance of a persistent archive manager. There currenty are
    two implementations for persistent archive manager: (i) the file-system
    archive manager, and (ii) the archive manager that maintains archive
    descriptors in a relational databaase.
    """
    def __new__(
        cls, basedir: Optional[str] = None, dbconnect: Optional[str] = None,
        create: Optional[bool] = False
    ):
        """Create an instance of a persistent archive manager. If the database
        connector string is given an instance of the DBArchiveManager is
        returned. Otherwise, an instance of the FileSystemArchiveManager is
        returned

        Parameters
        ----------
        basedir: string, default=None
            Path to dorectory on disk where archives are maintained.
        db: histore.archive.manager.db.database.DB, default=None
            Database connection object.
        create: bool, default=False
            Create a fresh database and delete all files in the base directory
            if True.
        """
        if dbconnect is not None:
            return DBArchiveManager(
                basedir=basedir,
                db=DB(connect_url=dbconnect),
                create=create
            )
        else:
            return FileSystemArchiveManager(basedir=basedir, create=create)
