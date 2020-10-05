# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Factory pattern for archives that are maintained on disk."""

import json
import os
import shutil

from typing import Dict, List, Optional, Union

from histore.archive.base import PersistentArchive
from histore.archive.manager.base import ArchiveManager
from histore.archive.manager.descriptor import ArchiveDescriptor

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
        # Ensure that the archive name is unique.
        if self.get_by_name(name) is not None:
            raise ValueError("archive '{}' already exists".format(name))
        # Create the descriptor for the new archive.
        descriptor = ArchiveDescriptor.create(
            name=name,
            description=description,
            primary_key=primary_key,
            encoder=encoder,
            decoder=decoder
        )
        identifier = descriptor.identifier()
        primary_key = descriptor.primary_key()
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
            archdir = os.path.join(self.basedir, identifier)
            if os.path.isdir(archdir):
                shutil.rmtree(archdir)
            del self._archives[identifier]
            self.write()

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
        desc = self._archives.get(identifier)
        if desc is None:
            raise ValueError('unknown archive {}'.format(identifier))
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
