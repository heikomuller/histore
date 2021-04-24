# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class that defines the interface for the archive writer."""

from abc import ABCMeta, abstractmethod

from histore.key import NumberKey
from histore.archive.row import ArchiveRow
from histore.archive.timestamp import SingleVersion
from histore.archive.value import SingleVersionValue
from histore.document.row import DocumentRow


class ArchiveWriter(metaclass=ABCMeta):
    """The abstract archive writer class defines the methods for adding rows
    to an output archive. The writer maintains an internal counter to assign
    unique identifier to document rows.
    """
    def __init__(self, row_counter: int):
        """Initialize the counter that is used to generate unique row
        identifier.

        Parameters
        ----------
        row_counter: int
            Counter that is used to generate unique internal row identifier.
            The current value of the counter is the value for the next unique
            identifier.
        """
        self.row_counter = row_counter

    @abstractmethod
    def write_archive_row(self, row: ArchiveRow):
        """Add the given row to a new archive version.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        raise NotImplementedError()  # pragma: no cover

    def write_document_row(self, row: DocumentRow, version: int):
        """Add a given document row to a new archive version with the given
        identifier. Uses the internal row counter to assign a new unique row
        identifier.

        Parameters
        ----------
        row: histore.document.row.DocumentRow
            Row from an input document (snapshot) that is being added to the
            archive snapshot for the given version.
        version: int
            Unique identifier for the snapshot version that the document row is
            added to.
        """
        # Create a new archive row with unique row identifier for the given
        # document row.
        ts = SingleVersion(version=version)
        cells = dict()
        for colid, value in row.values.items():
            cells[colid] = SingleVersionValue(value=value, timestamp=ts, has_timestamp=False)
        key = row.key
        if not isinstance(key, tuple) and key.is_new():
            key = NumberKey(self.row_counter)
        arch_row = ArchiveRow(
            rowid=self.row_counter,
            key=key,
            pos=SingleVersionValue(value=row.pos, timestamp=ts, has_timestamp=False),
            cells=cells,
            timestamp=ts
        )
        # Increment the row counter and add the new row to the archive.
        self.row_counter += 1
        self.write_archive_row(arch_row)


class ValidatingArchiveWriter(ArchiveWriter):
    """Wrapper for another archive writer. Ensures that archive rows are
    ordered in descending order of their key values. Raises a ValueError
    if an attempt is made to write rows that are not in order.
    """
    def __init__(self, writer: ArchiveWriter):
        """Initialize the wrapped archive writer that is used to write archive
        rows.

        Parameters
        ----------
        writer: histore.archive.writer.ArchiveWriter
            Writer for archive rows.
        """
        self.writer = writer
        self._prev = None

    @property
    def row_counter(self) -> int:
        """Counter for archive rows.

        Returns
        -------
        int
        """
        return self.writer.row_counter

    @row_counter.setter
    def row_counter(self, val: int):
        """Update the row counter for the associated writer.

        Parameters
        ----------
        val: int
            Updated row counter value.
        """
        self.writer.row_counter = val

    def write_archive_row(self, row: ArchiveRow):
        """Add the given row to a new archive version.

        Raises a ValueError if the row key is lower than the key of the
        previous row that was written.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        if self._prev is not None and row.key < self._prev:
            msg = "row {} with key '{}' is out of order for previous key '{}'"
            raise ValueError(msg.format(row.rowid, row.key, self._prev))
        self._prev = row.key
        self.writer.write_archive_row(row)
