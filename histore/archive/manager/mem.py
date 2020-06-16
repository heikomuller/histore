# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Manager implementation for archives that are maintained in main memory."""

from histore.archive.base import Archive
from histore.archive.manager.base import ArchiveManager
from histore.archive.manager.descriptor import ArchiveDescriptor


class VolatileArchiveManager(ArchiveManager):
    """The volatile archive manager maintains a set of archives in main memory.
    """
    def __init__(self):
        """Initialize the dictionaries that maintain archive descriptors and
        archive objects.
        """
        self._archives = dict()
        self._descriptors = dict()

    def archives(self):
        """Get dictionary of archive descriptors. The returned dictionary maps
        archive identifier to their descriptor.

        Returns
        -------
        dict
        """
        return self._descriptors

    def create(self, name=None, description=None, primary_key=None):
        """Create a new volatiole archive object.

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
        archive = Archive(primary_key=descriptor.primary_key())
        self._archives[identifier] = archive
        self._descriptors[identifier] = descriptor
        return descriptor

    def delete(self, identifier):
        """Delete the archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        """
        if self.contains(identifier):
            del self._archives[identifier]
            del self._descriptors[identifier]

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
        return self._archives[identifier]
