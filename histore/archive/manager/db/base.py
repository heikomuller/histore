# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Manager for archives that maintains all archive metadata in a relational
database.
"""

import os
import shutil

from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Optional, Union

from histore.archive.base import PersistentArchive
from histore.archive.manager.base import ArchiveManager
from histore.archive.manager.db.database import DB
from histore.archive.manager.db.model import Archive, ArchiveKey
from histore.archive.manager.descriptor import ArchiveDescriptor

import histore.config as config
import histore.util as util


class DBArchiveManager(ArchiveManager):
    """The database archive manager maintains a set of archives on disk. The
    list of archive descriptors is maintained in a relational database.
    """
    def __init__(
        self, basedir: Optional[str] = None, db: Optional[DB] = None,
        create: Optional[bool] = False
    ):
        """Initialize the base directory under which all archives are stored in
        individual sub-folders and the database connection object for archive
        descriptors.

        If the base directory is not given the value will be read from the
        environment variable HISTORE_BASEDIR. If the database is not given
        the connection string will be read from the environment variable
        HISTORE_DBCONNECT.

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
        # Initialize the base directory.
        if basedir is None:
            basedir = config.BASEDIR()
        self.basedir = basedir
        # Initialize the database connector.
        if db is None:
            db = DB(connect_url=config.DBCONNECT())
        self.db = db
        if create:
            # Create a fresh database instance.
            db.init()
            # Clear all files in the base directory if it exists.
            if os.path.isdir(self.basedir):
                util.cleardir(self.basedir)
        # Create the base directory (if it does not exist).
        util.createdir(basedir)

    def archives(self) -> Dict[str, ArchiveDescriptor]:
        """Get dictionary of archive descriptors. The returned dictionary maps
        archive identifier to their descriptor.

        Returns
        -------
        dict(string: histore.archive.manager.descriptor.ArchiveDescriptor)
        """
        archives = dict()
        with self.db.session() as session:
            for arch in session.query(Archive).all():
                archives[arch.archive_id] = arch.descriptor()
        return archives

    def create(
        self, name: str = None, description: Optional[str] = None,
        primary_key: Optional[Union[List[str], str]] = None,
        encoder: Optional[str] = None, decoder: Optional[str] = None
    ) -> ArchiveDescriptor:
        """Create a new archive object. Raises a ValueError if an archive with
        the given name exists.

        Parameters
        ----------
        name: string, default=None
            Descriptive name that is associated with the archive.
        description: string, default=None
            Optional long description that is associated with the archive.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for rows in the
            archive.
        encoder: string, default=None
            Full package path for the Json encoder class that is used by the
            persistent archive.
        decoder: string, default=None
            Full package path for the Json decoder function that is used by the

        Returns
        -------
        histore.archive.manager.descriptor.ArchiveDescriptor

        Raises
        ------
        ValueError
        """
        try:
            with self.db.session() as session:
                archive = Archive(
                    name=name,
                    description=description,
                    encoder=encoder,
                    decoder=decoder
                )
                if primary_key is not None:
                    if isinstance(primary_key, str):
                        primary_key = [primary_key]
                    for pos, colname in enumerate(primary_key):
                        key = ArchiveKey(name=colname, pos=pos)
                        archive.keyspec.append(key)
                session.add(archive)
                session.commit()
                return archive.descriptor()
        except SQLAlchemyError as ex:
            raise ValueError(ex)

    def delete(self, identifier: str):
        """Delete the archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        """
        with self.db.session() as session:
            # Query database to ensure that the archive exists.
            archive = session\
                .query(Archive)\
                .filter(Archive.archive_id == identifier)\
                .one_or_none()
            if archive is None:
                return
            # Remove the archive base directory and the entry in the database.
            archdir = os.path.join(self.basedir, identifier)
            if os.path.isdir(archdir):
                shutil.rmtree(archdir)
            session.delete(archive)

    def get(self, identifier: str) -> PersistentArchive:
        """Get the archive that is associated with the given identifier. Raises
        a ValueError if the identifier is unknown.

        Parameters
        ----------
        identifier: string
            Unique archive identifier

        Returns
        -------
        histore.archive.base.Archive

        Raises
        ------
        ValueError
        """
        with self.db.session() as session:
            # Query database to ensure that the archive exists.
            archive = session\
                .query(Archive)\
                .filter(Archive.archive_id == identifier)\
                .one_or_none()
            if archive is None:
                raise ValueError('unknown archive {}'.format(identifier))
            # Get the archive descriptor and close the database connection.
            desc = archive.descriptor()
        archdir = os.path.join(self.basedir, identifier)
        primary_key = desc.primary_key()
        # Load JSONEncoder class if encoder is contained in the descriptor.
        if desc.encoder() is not None:
            encoder = util.import_obj(desc.encoder())
        else:
            encoder = None
        # Load the corresponding Json decoder function if a decoder is
        # contained in the descriptor.
        if desc.decoder() is not None:
            decoder = util.import_obj(desc.decoder())
        else:
            decoder = None
        return PersistentArchive(
            basedir=archdir,
            primary_key=primary_key,
            encoder=encoder,
            decoder=decoder
        )

    def rename(self, identifier: str, name: str):
        """Rename the specified archive. Raises a ValueError if the identifier
        is unknown or if an archive with the given name exist.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        name: string
            New archive name.

        Raises
        ------
        ValueError
        """
        try:
            with self.db.session() as session:
                # Query database to ensure that the archive exists.
                archive = session\
                    .query(Archive)\
                    .filter(Archive.archive_id == identifier)\
                    .one_or_none()
                if archive is None:
                    raise ValueError('unknown archive {}'.format(identifier))
                # Update the archive name in the database.
                archive.name = name
        except SQLAlchemyError as ex:
            raise ValueError(ex)
