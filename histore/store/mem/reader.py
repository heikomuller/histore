# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of the archive reader for the in-memory archive."""

from histore.archive.reader import ArchiveReader


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
