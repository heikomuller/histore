# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class that defines the interface for the archive writer."""

from abc import ABCMeta, abstractmethod

from histore.key.base import NumberKey
from histore.archive.row import ArchiveRow
from histore.archive.timestamp import Timestamp
from histore.archive.value import SingleVersionValue


class ArchiveWriter(metaclass=ABCMeta):
    """The abstract archive writer class defines the methods for adding rows
    to an output archive. The writer maintains an internal counter to assign
    unique identifier to document rows.
    """
    def __init__(self, row_counter):
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
    def write_archive_row(self, row):  # pragma: no cover
        """Add the given row to a new archive version.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        raise NotImplementedError()

    def write_document_row(self, row, version):
        """Add a given document row to a new archive version with the given
        identifier. Uses the internal row counter to assign a new unique row
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
        key = row.key
        if not isinstance(key, tuple) and key.is_new():
            key = NumberKey(self.row_counter)
        arch_row = ArchiveRow(
            rowid=self.row_counter,
            key=key,
            pos=SingleVersionValue(value=row.pos, timestamp=ts),
            cells=cells,
            timestamp=ts
        )
        # Increment the row counter and add the new row to the archive.
        self.row_counter += 1
        self.write_archive_row(arch_row)
