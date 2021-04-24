# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Manager for archives that maintains all archive metadata in a relational
database.
"""

from sqlalchemy.exc import SQLAlchemyError
from typing import Callable, Dict, Optional, Union

import os
import shutil

from histore.archive.base import InputDocument, PrimaryKey, get_key_columns, to_document
from histore.archive.base import Archive as PersistentArchive
from histore.archive.manager.base import ArchiveManager
from histore.archive.manager.db.database import DB
from histore.archive.manager.db.model import Archive, ArchiveKey
from histore.archive.manager.descriptor import ArchiveDescriptor
from histore.archive.manager.descriptor import decoder_from_string, encoder_from_string, serializer_from_dict
from histore.archive.store.fs.base import ArchiveFileStore
from histore.document.base import InputDescriptor

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
        if create:
            # Clear all files in the base directory if it exists.
            if os.path.isdir(self.basedir):
                util.cleardir(self.basedir)
        # Initialize the database connector.
        if db is None:
            db = DB(connect_url=config.DBCONNECT())
        self.db = db
        if create:
            # Create a fresh database instance.
            db.init()
        # Create the base directory (if it does not exist).
        util.createdir(basedir)

    def _archive_dir(self, identifier: str) -> str:
        """Get directory for archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier.

        Returns
        -------
        string
        """
        return os.path.join(self.basedir, identifier)

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
        self, name: Optional[str] = None, description: Optional[str] = None,
        encoder: Optional[str] = None, decoder: Optional[str] = None,
        serializer: Union[Dict, Callable] = None, doc: Optional[InputDocument] = None,
        primary_key: Optional[PrimaryKey] = None, snapshot: Optional[InputDescriptor] = None,
        sorted: Optional[bool] = False, buffersize: Optional[float] = None,
        validate: Optional[bool] = False
    ) -> ArchiveDescriptor:
        """Create a new archive object under a given unique name.

        For archives that are keyed by a primary key, the input document for
        the first dataset snapshot  has to be provided. This snapshot will be
        loaded into the archive.

        Raises a ValueError if an archive with the given name exists.

        Parameters
        ----------
        name: string, default=None
            Descriptive name that is associated with the archive.
        description: string, default=None
            Optional long description that is associated with the archive.
        encoder: string, default=None
            Full package path for the Json encoder class that is used by the
            persistent archive.
        decoder: string, default=None
            Full package path for the Json decoder function that is used by the
            persistent archive.
        serializer: dict or callable, default=None
            Dictionary or callable that returns a dictionary that contains the
            specification for the serializer. The serializer specification is
            a dictionary with the following elements:
            - ``clspath``: Full package target path for the serializer class
            that is instantiated.
            - ``kwargs`` : Additional arguments that are passed to the
            constructor of the created serializer instance.
            Only ``clspath`` is required.
        doc: histore.archive.base.InputDocument, default=None
            Input document representing the initial dataset snapshot that is
            being loaded into the archive.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for snapshot rows.
        snapshot: histore.document.base.InputDescriptor, default=None
            Optional metadata for the created snapshot.
        sorted: bool, default=False
            Flag indicating if the document is sorted by the optional primary
            key attributes. Ignored if the archive is not keyed.
        buffersize: float, default=None
            Maximum size (in bytes) for the memory buffer when sorting the
            input document.
        validate: bool, default=False
            Validate that the resulting archive is in proper order before
            committing the action.

        Returns
        -------
        histore.archive.manager.descriptor.ArchiveDescriptor

        Raises
        ------
        ValueError
        """
        # Get serializer dictionary if a function was given.
        serializer = serializer() if serializer is not None and callable(serializer) else serializer
        # Create an unique identifier for the new archive.
        identifier = util.get_unique_identifier()
        try:
            with self.db.session() as session:
                archive = Archive(
                    archive_id=identifier,
                    name=name,
                    description=description,
                    encoder=encoder,
                    decoder=decoder,
                    serializer=serializer
                )
                # Load initial snapshot if given.
                if doc is not None:
                    doc = to_document(doc)
                    # Get the expected identifier for the primary key columns.
                    primary_key = get_key_columns(columns=doc.columns, primary_key=primary_key)
                    store = ArchiveFileStore(
                        basedir=self._archive_dir(identifier),
                        replace=True,
                        serializer=serializer_from_dict(serializer),
                        encoder=encoder_from_string(encoder),
                        decoder=decoder_from_string(decoder),
                        primary_key=primary_key
                    )
                    arch = PersistentArchive(store=store)
                    arch.commit(
                        doc=doc,
                        descriptor=snapshot,
                        sorted=sorted,
                        buffersize=buffersize,
                        validate=validate
                    )
                elif primary_key is not None:
                    raise ValueError('missing snapshot document')
                if primary_key is not None:
                    for pos, colid in enumerate(primary_key):
                        key = ArchiveKey(colid=colid, pos=pos)
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
            archdir = self._archive_dir(identifier)
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
        store = ArchiveFileStore(
            basedir=self._archive_dir(identifier),
            replace=False,
            serializer=desc.serializer(),
            encoder=desc.encoder(),
            decoder=desc.decoder(),
            primary_key=desc.primary_key()
        )
        return PersistentArchive(store=store)

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
