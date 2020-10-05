# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class for managers that maintain a set of archives."""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional, Union

from histore.archive.base import Archive
from histore.archive.manager.descriptor import ArchiveDescriptor


class ArchiveManager(metaclass=ABCMeta):  # pragma: no cover
    """The manager for archives implements a factory pattern for creating and
    accessing different types of archives. The type of archive (e.g. persistent
    or volatile is implementaiton dependent. Matdata about each archive is
    maintained in an archive descriptor.
    """
    @abstractmethod
    def archives(self) -> Dict[str, ArchiveDescriptor]:
        """Get dictionary of archive descriptors. The returned dictionary maps
        archive identifier to their descriptor.

        Returns
        -------
        dict(string: histore.archive.manager.descriptor.ArchiveDescriptor)
        """
        raise NotImplementedError()

    def contains(self, identifier: str) -> bool:
        """Returns True if an archive with the given identifier exists.

        Parameters
        ----------
        identifier: string
            Unique archive identifier

        Returns
        -------
        bool
        """
        return identifier in self.archives()

    @abstractmethod
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
            persistent archive.

        Returns
        -------
        histore.archive.manager.descriptor.ArchiveDescriptor

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, identifier: str):
        """Delete the archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    def get_by_name(self, name: str) -> ArchiveDescriptor:
        """Get descriptor for the archive with the given name. If no archive
        with that name exists None is returned.

        Parameters
        ----------
        name: string
            Archive name

        Returns
        -------
        histore.archive.manager.descriptor.ArchiveDescriptor
        """
        for archive in self.archives().values():
            if archive.name() == name:
                return archive
        return None

    def list(self) -> List[ArchiveDescriptor]:
        """Get the list of descriptors for the maintained archives.

        Returns
        -------
        list(histore.archive.manager.descriptor.ArchiveDescriptor)
        """
        return list(self.archives().values())

    @abstractmethod
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
        raise NotImplementedError()
