# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of the archive writer for the in-memory archive."""


from histore.archive.row import ArchiveRow
from histore.archive.timestamp import Timestamp
from histore.archive.value import SingleVersionValue
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
            the current value of the counter is the value for the next unique
            identifier.
        """
        self.row_counter = row_counter
        self.rows = list()

    def write_archive_row(self, row):
        """Add the given row to the row buffer.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        self.rows.append(row)

    def write_document_row(self, row, version):
        """Add a given document row to a new archive version with the given
        identifier.

        Parameters
        ----------
        row: histore.document.row.DocumentRow
            Row from an inout document (snapshot) that is being added to the
            archive snapshot for the given version.
        version: int
            Unique identifier for the snapshot version that the document row is
            added to.
        """
        # Create a new archive row with unique row identifier for the given
        # document row.
        ts = Timestamp(version=version)
        cells = dict()
        for colid, value in row.values.items():
            cells[colid] = SingleVersionValue(value=value, timestamp=ts)
        arch_row = ArchiveRow(
            rowid=self.row_counter,
            key=row.key if row.key is not None else self.row_counter,
            index=SingleVersionValue(value=row.pos, timestamp=ts),
            cells=cells,
            timestamp=ts
        )
        # Increment the row counter and add the new row to the archive.
        self.row_counter += 1
        self.write_archive_row(arch_row)
