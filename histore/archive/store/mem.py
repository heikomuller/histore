# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of the archive store and related classes. Maintains all data
in main memory. Archive information is not persisted.
"""

from histore.archive.reader import ArchiveReader
from histore.archive.schema import ArchiveSchema
from histore.archive.snapshot import SnapshotListing
from histore.archive.store.base import ArchiveStore
from histore.archive.writer import ArchiveWriter


class VolatileArchiveStore(ArchiveStore):
    """Archive store that keeps all archive related information in main memory.
    The store is volatile as no information is persisted on disk.
    """
    def __init__(self):
        """Initialize the archive archive components."""
        self.rows = list()
        self.schema = ArchiveSchema()
        self.snapshots = SnapshotListing()

    def commit(self, schema, writer, snapshots):
        """Commit a new version of the dataset archive. The modified components
        of the archive are given as the three arguments of this method.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the new archive version.
        writer: histore.archive.writer.ArchiveWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the new archive version.
        snapshots: histore.archive.snapshot.SnapshotListing
            Modified list of snapshots in the new archive. The new archive
            version is the last entry in the list.
        """
        self.rows = writer.rows
        self.schema = schema
        self.snapshots = snapshots

    def get_reader(self):
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.store.mem.BufferedReader
        """
        return BufferedReader(rows=self.rows)

    def get_schema(self):
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        return self.schema

    def get_snapshots(self):
        """Get listing of all snapshots in the archive.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing
        """
        return self.snapshots

    def get_writer(self):
        """Get a a new archive buffer to maintain rows for a new archive
        version.

        Returns
        -------
        histore.archive.store.mem.ArchiveBuffer
        """
        return ArchiveBuffer()


# -- In-memory archive reader and writer --------------------------------------

class ArchiveBuffer(ArchiveWriter):
    """The archive buffer maintains a list of archive rows in memory."""
    def __init__(self):
        """Initialize an empty row buffer."""
        self.rows = list()

    def write(self, row):
        """Add the given row to the row buffer.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        self.rows.append(row)


class BufferedReader(ArchiveReader):
    """Reader for a list of archive rows that are kept in main memory."""
    def __init__(self, rows):
        """Initialize the list of rows that the reader will return.

        Parameters
        ----------
        rows: list(histore.archive.row.ArchiveRow)
            List of rows in the input stream.
        """
        self.rows = rows
        # Maintain a read index that points to the next row in the input
        # stream.
        self.read_index = 0

    def has_next(self):
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otherwise, the next() method will return
        None.

        Returns
        -------
        bool
        """
        return self.read_index < len(self.rows)

    def next(self):
        """Read the next row in the dataset archive. Returns None if the end of
        the archive rows has been reached.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        # Return None if the reader has no more rows.
        if not self.has_next():
            return None
        # Get the next row that the read index points to. Advance the read
        # index before returning that row..
        row = self.rows[self.read_index]
        self.read_index += 1
        return row
