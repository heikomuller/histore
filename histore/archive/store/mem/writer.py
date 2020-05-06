# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of the archive writer for the in-memory archive."""


from histore.archive.writer import ArchiveWriter


class ArchiveBuffer(ArchiveWriter):
    """The archive buffer maintains a list of archive rows in memory."""
    def __init__(self, row_counter):
        """Initialize an empty row buffer and the counter to generate unique
        row identifier.

        Parameters
        ----------
        row_counter: int
            Counter that is used to generate unique internal row identifier.
            The current value of the counter is the value for the next unique
            identifier.
        """
        super(ArchiveBuffer, self).__init__(row_counter)
        self.rows = list()

    def write_archive_row(self, row):
        """Add the given row to the row buffer.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        self.rows.append(row)
