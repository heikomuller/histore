# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Manager implementation for archives that are maintained in main memory."""

from typing import Dict, List, Optional, Union

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

    def archives(self) -> Dict[str, ArchiveDescriptor]:
        """Get dictionary of archive descriptors. The returned dictionary maps
        archive identifier to their descriptor.

        Returns
        -------
        dict(string: histore.archive.manager.descriptor.ArchiveDescriptor)
        """
        return self._descriptors

    def create(
        self, name: Optional[str] = None, description: Optional[str] = None,
        primary_key: Optional[Union[List[str], str]] = None,
        encoder: Optional[str] = None, decoder: Optional[str] = None
    ) -> ArchiveDescriptor:
        """Create a new volatile archive object. Raises a ValueError if an
        archive with the given name exists.

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
            Ignored. Full package path for the Json encoder class that is used
            by the persistent archive. Included for API completeness.
        decoder: string, default=None
            Ignored. Full package path for the Json decoder function that is
            used by the persistent archive. Included for API completeness.

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
            primary_key=primary_key
        )
        identifier = descriptor.identifier()
        archive = Archive(primary_key=descriptor.primary_key())
        self._archives[identifier] = archive
        self._descriptors[identifier] = descriptor
        return descriptor

    def delete(self, identifier: str):
        """Delete the archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        """
        if self.contains(identifier):
            del self._archives[identifier]
            del self._descriptors[identifier]

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
        if identifier not in self._archives:
            raise ValueError("unknown archive '{}''".format(identifier))
        return self._archives[identifier]

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
        archive = self._descriptors.get(identifier)
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
