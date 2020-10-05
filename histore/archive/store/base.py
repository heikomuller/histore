# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""The archive store intruduces a layer of abstraction between the archive
object and the way in which archives are managed and maintained.
"""

from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Optional

from histore.archive.reader import ArchiveReader
from histore.archive.schema import ArchiveSchema
from histore.archive.snapshot import Snapshot, SnapshotListing
from histore.archive.writer import ArchiveWriter


class ArchiveStore(metaclass=ABCMeta):
    """Abstract class that defines the interface to read and write dataset
    archive information.
    """
    @abstractmethod
    def commit(
        self, schema: ArchiveSchema, writer: ArchiveWriter, version: int,
        valid_time: Optional[datetime] = None,
        description: Optional[str] = None
    ) -> Snapshot:  # pragma: no cover
        """Commit a new version of the dataset archive. The modified components
        of the archive are given as the three arguments of this method.

        Returns the handle for the newly created snapshot.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the new archive version.
        writer: histore.archive.writer.ArchiveWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the new archive version.
        version: int
            Unique version identifier for the new snapshot.
        valid_time: datetime.datetime, default=None
            Timestamp when the snapshot was first valid. A snapshot is valid
            until the valid time of the next snapshot in the archive.
        description: string, default=None
            Optional user-provided description for the snapshot.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        raise NotImplementedError()

    @abstractmethod
    def is_empty(self) -> bool:  # pragma: no cover
        """True if the archive does not contain any snapshots yet.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def get_reader(self) -> ArchiveReader:  # pragma: no cover
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.reader.ArchiveReader
        """
        raise NotImplementedError()

    @abstractmethod
    def get_schema(self) -> ArchiveSchema:  # pragma: no cover
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        raise NotImplementedError()

    @abstractmethod
    def get_snapshots(self) -> SnapshotListing:  # pragma: no cover
        """Get listing of all snapshots in the archive.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing
        """
        raise NotImplementedError()

    @abstractmethod
    def get_writer(self) -> ArchiveWriter:  # pragma: no cover
        """Get a writer for a new version of the archive.

        Returns
        -------
        histore.archive.writer.ArchiveWriter
        """
        raise NotImplementedError()
