# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of a basic archive store that maintains all data in main
memory. Archive information is not persisted.
"""

from datetime import datetime
from typing import Dict, Optional

from histore.archive.schema import ArchiveSchema
from histore.archive.snapshot import SnapshotListing
from histore.archive.store.base import ArchiveStore
from histore.archive.store.mem.reader import BufferedReader
from histore.archive.store.mem.writer import ArchiveBuffer


class VolatileArchiveStore(ArchiveStore):
    """Archive store that keeps all archive related information in main memory.
    The store is volatile as no information is persisted on disk.
    """
    def __init__(self):
        """Initialize the archive archive components."""
        self.rows = list()
        self.schema = ArchiveSchema()
        self.snapshots = SnapshotListing()
        self.row_counter = 0

    def commit(
        self, schema: ArchiveSchema, writer: ArchiveBuffer, version: int,
        valid_time: Optional[datetime] = None, description: Optional[str] = None,
        action: Optional[Dict] = None
    ):
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
        action: dict, default=None
            Optional metadata defining the action that created the snapshot.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        # Get an updated shapshot listing.
        snapshots = self.snapshots.append(
            version=version,
            valid_time=valid_time,
            description=description,
            action=action
        )
        self.rows = writer.rows
        self.schema = schema
        self.snapshots = snapshots
        self.row_counter = writer.row_counter
        # Return handle for the new snapshot.
        return snapshots.last_snapshot()

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
