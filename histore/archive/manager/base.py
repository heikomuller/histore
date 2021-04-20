# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class for managers that maintain a set of archives."""

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Union

from histore.archive.base import Archive, InputDocument
from histore.archive.manager.descriptor import ArchiveDescriptor
from histore.archive.schema import ArchiveSchema
from histore.document.base import InputDescriptor
from histore.document.schema import Schema


# Primary key of a dataset.
PrimaryKey = Union[str, List[str]]


class ArchiveManager(metaclass=ABCMeta):
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
        raise NotImplementedError()  # pragma: no cover

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
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete(self, identifier: str):
        """Delete the archive with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique archive identifier
        """
        raise NotImplementedError()  # pragma: no cover

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
        raise NotImplementedError()  # pragma: no cover

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
        raise NotImplementedError()  # pragma: no cover


# -- Helper Functions ---------------------------------------------------------

def get_key_columns(columns: Schema, primary_key: PrimaryKey) -> List[int]:
    """Get identifier for primary key columns.

    Uses the archive schema merge method to get the identifiable columns for the
    given schema. Then returns the identifier for the columns that match the
    primary key columns.

    Note that the primary key specification cannot reference columns that have
    non-unique names. If a primary key column occurs more than once in the
    resulting schema an error is raised. An error is also raised if the primary
    key references a column that does not exist.

    If the list of primary key columns is None or empty the result is None.

    Parameters
    ----------
    columns: list of string
        Schema for the input document.
    primary_key: list of string
        Names of primary key columns. May be None.

    Returns
    -------
    list of int
    """
    # Return None if the list of primary key columns is None or empty.
    if not primary_key:
        return None
    # Ensure that the primary key is a list.
    primary_key = primary_key if isinstance(primary_key, list) else [primary_key]
    # Get the identifiable columns for the document schema. Create a mapping
    # from the column name to the identifier(s).
    schema, _ = ArchiveSchema().merge(columns=columns, version=0)
    columns = defaultdict(list)
    for col in schema.at_version(0):
        columns[col].append(col.colid)
    # Generate the list of primary key column identifier.
    pk = list()
    for name in primary_key:
        col = columns.get(name)
        if not col:
            raise ValueError("unknown column '{}'".format(col))
        elif len(col) > 1:
            raise ValueError("not a unique key column '{}'".format(col))
        pk.append(col[0])
    return pk
