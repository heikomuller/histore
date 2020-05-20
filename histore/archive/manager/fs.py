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

from histore.archive.base import PersistentArchive
from histore.archive.manager.base import ArchiveManager
from histore.archive.manager.descriptor import ArchiveDescriptor

import histore.config as config
import histore.util as util


class PersistentArchiveManager(ArchiveManager):
    """The persistent archive manager maintains a set of archives on disk. The
    list of archive descriptors is also maintained on disk as a Json file.
    """
    def __init__(self, basedir=None):
        """Initialize the base directory under which all archives are stores in
        individual sub-folders. If the base directory is not given the value
        will be read from the environment variable HISTORE_BASEDIR or the
        default value $HOME/.histore.

        Read the Json file containing the archive descriptors (if it exist).

        Parameters
        ----------
        basedir: string
            Path to dorectory on disk where archives are maintained.
        """
        if basedir is None:
            basedir = config.BASEDIR()
        self.basedir = util.createdir(basedir)
        # Initialize path to file that maintains archive descriptors.
        self.descriptorfile = os.path.join(self.basedir, 'archives.json')
        # Initialize the internal cache of archive descriptors
        self._archives = dict()
        if os.path.isfile(self.descriptorfile):
            with open(self.descriptorfile, 'r') as f:
                doc = json.load(f)
            for obj in doc:
                descriptor = ArchiveDescriptor(obj)
                self._archives[descriptor.identifier()] = descriptor

    def archives(self):
        """Get dictionary of archive descriptors. The returned dictionary maps
        archive identifier to their descriptor.

        Returns
        -------
        dict
        """
        return self._archives

    def create(self, name=None, description=None, primary_key=None):
        """Create a new archive object.

        Parameters
        ----------
        name: string, default=None
            Descriptive name that is associated with the archive.
        description: string, default=None
            Optional long description that is associated with the archive.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for rows in the
            archive.

        Returns
        -------
        histore.archive.manager.descriptor.ArchiveDescriptor
        """
        # Create the descriptor for the new archive.
        descriptor = ArchiveDescriptor.create(
            name=name,
            description=description,
            primary_key=primary_key
        )
        identifier = descriptor.identifier()
        primary_key = descriptor.primary_key()
        # Write list of archive descriptors.
        self._archives[identifier] = descriptor
        with open(self.descriptorfile, 'w') as f:
            json.dump([d.doc for d in self._archives.values()], f)
        return descriptor

    def delete(self, identifier):
        """Delete the archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        """
        if self.contains(identifier):
            shutil.rmtree(os.path.join(self.basedir, identifier))
            del self._archives[identifier]
            with open(self.descriptorfile, 'w') as f:
                json.dump([d.doc for d in self._archives.values()], f)

    def get(self, identifier):
        """Get the archive that is associated with the given identifier. Raises
        a ValueError if the identifier is unknown.

        Parameters
        ----------
        identifier: string
            Unique archive identifier

        Returns
        -------
        histore.archive.vase.Archive

        Raises
        ------
        ValueError
        """
        if identifier not in self._archives:
            raise ValueError('unknown archive {}'.format(identifier))
        archdir = os.path.join(self.basedir, identifier)
        primary_key = self._archives[identifier].primary_key()
        return PersistentArchive(basedir=archdir, primary_key=primary_key)
