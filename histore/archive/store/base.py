# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""The archive store intruduces a layer of abstraction between the archive
object and the way in which archives are managed and maintained.
"""

from abc import ABCMeta, abstractmethod
from typing import List

from histore.archive.reader import ArchiveReader
from histore.archive.schema import ArchiveSchema
from histore.archive.snapshot import SnapshotListing
from histore.archive.writer import ArchiveWriter


class ArchiveStore(metaclass=ABCMeta):
    """Abstract class that defines the interface to read and write dataset
    archive information.
    """
    @abstractmethod
    def commit(
        self, schema: ArchiveSchema, writer: ArchiveWriter,
        snapshots: SnapshotListing
    ):
        """Commit an updated dataset archive.

        The modified components of the archive are given as the three
        arguments of this method.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the new archive version.
        writer: histore.archive.writer.ArchiveWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the new archive version.
        snapshots: histore.archive.snapshot.SnapshotListing
            Snapshot listing for the modified archive.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def is_empty(self) -> bool:
        """True if the archive does not contain any snapshots yet.

        Returns
        -------
        bool
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_reader(self) -> ArchiveReader:
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.reader.ArchiveReader
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_schema(self) -> ArchiveSchema:
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_snapshots(self) -> SnapshotListing:
        """Get listing of all snapshots in the archive.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_writer(self) -> ArchiveWriter:
        """Get a writer for a new version of the archive.

        Returns
        -------
        histore.archive.writer.ArchiveWriter
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def primary_key(self) -> List[int]:
        """Get the list of identifier for the primary key column(s).

        Returns None if the archive is not keyed by a primary key.

        Returns
        -------
        list of int
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def rollback(self, schema: ArchiveSchema, writer: ArchiveWriter, version: int):
        """Store the archive after it has been rolled back to a previous
        version.

        The archive schema and archive writer contain the modified schema and
        rows after the rollback.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the previous archive version.
        writer: histore.archive.writer.ArchiveWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the previous archive version.
        version: int
            Unique version identifier for the rollback snapshot.
        """
        raise NotImplementedError()  # pragma: no cover
