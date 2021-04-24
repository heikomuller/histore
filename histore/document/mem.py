# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of the document interface for documents that have been read
completely into main memory.
"""

from __future__ import annotations
from typing import List, Optional, Tuple

import pandas as pd

from histore.document.base import DataRow, Document, DocumentIterator, RowIndex, document_to_df
from histore.document.schema import DocumentSchema

import histore.util as util


class InMemoryDocumentIterator(DocumentIterator):
    """Document iterator for in-memory documents."""
    def __init__(self, rows: List[Tuple[int, RowIndex, DataRow]]):
        """Initialize the list of document rows.

        Parameters
        ----------
        rows: list
            List of document rows. Each row in the list is a tuple of row
            position, row index, and cell values.
        """
        self.rows = rows
        self._readindex = 0

    def close(self):
        """Set list of rows to None."""
        self.rows = None

    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the document.

        Returns the row position, row index and the list of cell values for each
        of the document columns. Raises a StopIteration error if an attempt is
        made to read past the end of the document.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        try:
            rowpos, rowidx, values = self.rows[self._readindex]
        except (IndexError, TypeError):
            # If an attempt is made to read past the current row array length
            # either an IndexError is raised or a TypeError if the row array
            # has been set to None by the close method.
            raise StopIteration()
        self._readindex += 1
        return rowpos, rowidx, values


class InMemoryDocument(Document):
    """The in-memory document maintains a array of document rows (each as a
    tuple of row position, row index, and cell values). The row position is the
    original position of the row in a sorted document.
    """
    def __init__(self, columns: DocumentSchema, rows: List[Tuple[int, RowIndex, DataRow]]):
        """Initialize document schema and rows.

        Parameters
        ----------
        columns: list
            List of column names. The number of values in each row is expected
            to be the same as the number of columns and the order of values in
            each row is expected to correspond to their respective column in
            this list.
        rows: list
            List of document rows. Each row in the list is a tuple of row
            position, row index, and cell values.
        """
        super(InMemoryDocument, self).__init__(columns=columns)
        self.rows = rows

    def close(self):
        """Set list of rows to None when closing the document."""
        self.rows = None

    def open(self) -> InMemoryDocumentIterator:
        """Open the document to get a iterator for the rows in the document.

        Returns
        -------
        histore.document.base.DocumentIterator
        """
        return InMemoryDocumentIterator(rows=self.rows)

    def to_df(self) -> pd.DataFrame:
        """Create data frame from the document rows.

        Returns
        -------
        pd.DataFrame
        """
        return document_to_df(self)

    def sorted(self, keys: List[int], buffersize: Optional[float] = None) -> InMemoryDocument:
        """Sort the document rows based on the values in the key columns.

        Key columns are specified by their index position. Returns a new
        document.

        Parameters
        ----------
        keys: list of int
            Index position of sort columns.
        buffersize: float, default=None
            Maximum size (in bytes) of file blocks that are kept in main-memory.
            Ignored. Included for API completeness.

        Returns
        -------
        histore.document.mem.InMemoryDocument
        """
        return InMemoryDocument(
            columns=self.columns,
            rows=sorted(self.rows, key=lambda row: util.keyvalue(row[2], keys))
        )


class Schema(InMemoryDocument):
    """Empty document that only defines the document schema."""
    def __init__(self, columns: DocumentSchema):
        """Initialize document schema.

        Parameters
        ----------
        columns: list
            List of column names.
        """
        super(Schema, self).__init__(columns=columns, rows=[])
