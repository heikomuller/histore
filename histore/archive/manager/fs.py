# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Factory pattern for archives that are maintained on disk."""

from typing import Callable, Dict, Optional, Union

import json
import os
import shutil

from histore.archive.base import Archive, InputDocument, PersistentArchive, PrimaryKey
from histore.archive.manager.base import ArchiveManager
from histore.archive.manager.descriptor import ArchiveDescriptor
from histore.archive.store.fs.base import ArchiveFileStore
from histore.document.base import InputDescriptor

import histore.config as config
import histore.util as util


class FileSystemArchiveManager(ArchiveManager):
    """The persistent archive manager maintains a set of archives on disk. The
    list of archive descriptors is also maintained on disk as a Json file.
    """
    def __init__(
        self, basedir: Optional[str] = None, create: Optional[bool] = False
    ):
        """Initialize the base directory under which all archives are stored in
        individual sub-folders. If the base directory is not given the value
        will be read from the environment variable HISTORE_BASEDIR or the
        default value $HOME/.histore.

        Read the Json file containing the archive descriptors (if it exist).

        Parameters
        ----------
        basedir: string
            Path to dorectory on disk where archives are maintained.
        create: bool, default=False
            Create a fresh instance of the archive manager if True. This will
            delete all files in the base directory.
        """
        if basedir is None:
            basedir = config.BASEDIR()
        self.basedir = basedir
        # Initialize path to file that maintains archive descriptors.
        if create:
            # Clear all files in the base directory if it exists.
            if os.path.isdir(self.basedir):
                util.cleardir(self.basedir)
        util.createdir(self.basedir)
        # Initialize the internal cache of archive descriptors
        self.descriptorfile = os.path.join(basedir, 'archives.json')
        self._archives = dict()
        if os.path.isfile(self.descriptorfile):
            with open(self.descriptorfile, 'r') as f:
                doc = json.load(f)
            for obj in doc:
                descriptor = ArchiveDescriptor(obj)
                self._archives[descriptor.identifier()] = descriptor

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
        return self._archives

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
        the first dataset snapshot has to be provided. This snapshot will be
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
        # Ensure that the archive name is unique.
        if self.get_by_name(name) is not None:
            raise ValueError("archive '{}' already exists".format(name))
        # Create an unique identifier for the new archive.
        identifier = util.get_unique_identifier()
        # Create persistent archive. This will load the initial snapshot if a
        # document is given.
        archive = PersistentArchive(
            basedir=self._archive_dir(identifier),
            create=True,
            serializer=serializer,
            encoder=encoder,
            decoder=decoder,
            descriptor=snapshot,
            doc=doc,
            primary_key=primary_key,
            sorted=sorted,
            buffersize=buffersize,
            validate=validate
        )
        # Create the descriptor for the new archive.
        descriptor = ArchiveDescriptor.create(
            identifier=identifier,
            name=name,
            description=description,
            primary_key=archive.store.primary_key(),
            encoder=encoder,
            decoder=decoder,
            serializer=serializer
        )
        # Write list of archive descriptors.
        self._archives[identifier] = descriptor
        self.write()
        return descriptor

    def delete(self, identifier: str):
        """Delete the archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        """
        if self.contains(identifier):
            archdir = self._archive_dir(identifier)
            shutil.rmtree(archdir)
            del self._archives[identifier]
            self.write()

    def get(self, identifier: str) -> Archive:
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
        desc = self._archives.get(identifier)
        if desc is None:
            raise ValueError('unknown archive {}'.format(identifier))
        store = ArchiveFileStore(
            basedir=self._archive_dir(identifier),
            replace=False,
            serializer=desc.serializer(),
            encoder=desc.encoder(),
            decoder=desc.decoder(),
            primary_key=desc.primary_key()
        )
        return Archive(store=store)

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
        archive = self._archives.get(identifier)
        if archive is None:
            raise ValueError("unknown archive '{}''".format(identifier))
        if archive.name() == name:
            # Do nothing if the archive name matches the new name.
            return
        # Raise an error if another archive with the new name exists.
        # Ensure that the archive name is unique.
        if self.get_by_name(name) is not None:
            raise ValueError("archive '{}' already exists".format(name))
        archive.rename(name)
        # Write list of archive descriptors.
        self.write()

    def write(self):
        """Write the current descriptor set to file."""
        with open(self.descriptorfile, 'w') as f:
            json.dump([d.doc for d in self._archives.values()], f)
