# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of a basic archive store that maintains all data in main
memory. Archive information is not persisted.
"""

from typing import List, Optional

from histore.archive.schema import ArchiveSchema
from histore.archive.snapshot import SnapshotListing
from histore.archive.store.base import ArchiveStore
from histore.archive.store.mem.reader import BufferedReader
from histore.archive.store.mem.writer import ArchiveBuffer


class VolatileArchiveStore(ArchiveStore):
    """Archive store that keeps all archive related information in main memory.
    The store is volatile as no information is persisted on disk.
    """
    def __init__(self, primary_key: Optional[List[int]] = None):
        """Initialize the archive archive components.

        Parameters
        ----------
        primary_key: list of int, default=None
            List of identifier for primary key columns.
        """
        self.rows = list()
        self.schema = ArchiveSchema()
        self.snapshots = SnapshotListing()
        self.row_counter = 0
        self._primary_key = primary_key

    def commit(
        self, schema: ArchiveSchema, writer: ArchiveBuffer,
        snapshots: SnapshotListing
    ):
        """Commit an updated dataset archive.

        The modified components of the archive are given as the three
        arguments of this method.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the new archive version.
        writer: histore.archive.store.mem.writer.ArchiveBuffer
            Buffer containing the modified archive rows.
        snapshots: histore.archive.snapshot.SnapshotListing
            Snapshot listing for the modified archive.
        """
        self.rows = writer.rows
        self.schema = schema
        self.snapshots = snapshots
        self.row_counter = writer.row_counter

    def is_empty(self):
        """True if the archive does not contain any snapshots yet.

        Returns
        -------
        bool
        """
        return self.snapshots.is_empty()

    def get_reader(self) -> BufferedReader:
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.store.mem.BufferedReader
        """
        return BufferedReader(rows=self.rows)

    def get_schema(self) -> ArchiveSchema:
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        return self.schema

    def get_snapshots(self) -> SnapshotListing:
        """Get listing of all snapshots in the archive.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing
        """
        return self.snapshots

    def get_writer(self) -> ArchiveBuffer:
        """Get a a new archive buffer to maintain rows for a new archive
        version.

        Returns
        -------
        histore.archive.store.mem.ArchiveBuffer
        """
        return ArchiveBuffer(row_counter=self.row_counter)

    def primary_key(self) -> List[int]:
        """Get the list of identifier for the primary key column(s).

        Returns None if the archive is not keyed by a primary key.

        Returns
        -------
        list of int
        """
        return self._primary_key

    def rollback(self, schema: ArchiveSchema, writer: ArchiveBuffer, version: int):
        """Store the archive after it has been rolled back to a previous
        version.

        The archive schema and archive writer contain the modified schema and
        rows after the rollback.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the previous archive version.
        writer: histore.archive.store.mem.writer.ArchiveBuffer
            Instance of the archive writer class returned by this store that
            was used to output the rows of the previous archive version.
        version: int
            Unique version identifier for the rollback snapshot.
        """
        snapshots = self.snapshots.rollback(version=version)
        self.rows = writer.rows
        self.schema = schema
        self.snapshots = snapshots
