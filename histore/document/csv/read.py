# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Functions and classes to read CSV files as HISTORE documents."""

from typing import List, Optional

import csv
import os

from histore.document.base import Document
from histore.document.csv.base import CSVFile, CSVReader
from histore.document.mem.base import InMemoryDocument
from histore.document.mem.dataframe import DataFrameDocument
from histore.document.reader import DocumentReader
from histore.document.row import DocumentRow
from histore.document.schema import Column, Schema, to_schema
from histore.key.base import NumberKey, to_key


# -- Documents ----------------------------------------------------------------

class SimpleCSVDocument(Document):
    """CSV file containing a document that is keyed by the row position and
    not a primary key (refered to as an un-keyed document). Returns rows in
    document order with the row identifier being the line number (starting at
    zero and ignoring an optional column header).
    """
    def __init__(self, file: CSVFile):
        """Initialize the reference to the CSV file reader.

        Parameters
        ----------
        file: histore.document.csv.base.CSVFile
            Reference to the CSV file.
        """
        super(SimpleCSVDocument, self).__init__(columns=file.columns)
        self.file = file

    def close(self):
        """The un-keyed CSV document reader has no additional resources that
        need to be released.
        """
        pass  # pragma: no cover

    def partial(self, reader):
        """If the document in the CSV file is a partial document we read it
        as a data frame. This requires that partial documents in CSV files fit
        into main memory.

        Parameters
        ----------
        reader: histore.archive.reader.RowPositionReader
            Reader for row (key, position) tuples from the original
            snapshot version.

        Returns
        -------
        histore.document.base.Document
        """
        return DataFrameDocument(self.file.read_df()).partial(reader)

    def reader(self, schema: Optional[List[Column]] = None) -> DocumentReader:
        """Get reader for data frame rows ordered by their row identifier. In
        a partial document the row positions that are returned by the reader
        are aligned with the positions of the corresponding rows in the
        document of origin.

        Parameters
        ----------
        schema: list(histore.document.schema.Column), default=None
            List of columns in the document schema. Each column corresponds to
            a column in the column list of this document (corresponding to
            their position in the list). The schema columns provide the unique
            column identifier that are required by the document reader to
            generate document rows. An error is raised if the number of
            elements in the schema does not match the number of columns in the
            data frame. If no schema is provided the document schema itself is
            used as the default.

        Returns
        -------
        histore.document.reader.DocumentReader

        Raises
        ------
        ValueError
        """
        return SimpleCSVDocumentReader(
            reader=self.file.open(),
            schema=schema if schema is not None else to_schema(self.columns)
        )


class SimpleCSVDocumentReader(DocumentReader):
    """Document reader for un-keyed CSV documents."""
    def __init__(self, reader: CSVReader, schema: List[Column]):
        """Initialize the CSV reader and the list of column names.

        Parameters
        ----------
        reader: histore.document.csv.base.CSVReader
            Reader for the document file. If the file has a header it is
            assumed that the corresponding row has already been read.
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the document rows (based on list position). The schema
            columns provide the unique column identifier that are required to
            generate the returned document rows.
        """
        self.reader = reader
        self.schema = schema
        self._next_row = None
        self.next()

    def close(self):
        """Release all resources that are held by this reader."""
        if self.reader:
            self.reader.close()
            self.reader = None

    def has_next(self) -> bool:
        """Test if the reader has more rows to read. If this reader has more
        rows the internal row buffer is not empty.

        Returns
        -------
        bool
        """
        return self._next_row is not None

    def next(self) -> DocumentRow:
        """Read the next row in the document. Returns None if the end of the
        document has been reached.

        Returns
        -------
        histore.document.row.DocumentRow
        """
        # Return None if the reader is not set (i.e., because the end of the
        # file has been reached).
        if self.reader is None:
            return None
        result = self._next_row
        try:
            rowid, row = next(self.reader)
            # If a next row was read ensure that the number of values
            # is the same as the number of columns in the schema.
            if len(row) != len(self.schema):
                self.reader.close()
                raise ValueError('invalid row %d' % (self.reader.rowid))
            # Create document row object for the read values.
            values = dict()
            for i, col in enumerate(self.schema):
                values[col.colid] = row[i]
            self._next_row = DocumentRow(
                key=NumberKey(rowid),
                pos=rowid,
                values=values
            )
        except StopIteration:
            # Close the file if the end is reached. Set the reader to None in
            # order to return None when next is called again.
            self._next_row = None
            self.reader.close()
            self.reader = None
        # Return the buffered result
        return result


# -- Document class for sorted CSV files --------------------------------------

class SortedCSVDocument(Document):
    """CSV file containing a document that is sorted by one or more of the
    document columns.
    """
    def __init__(self, filename: str, columns: Schema, primary_key: List[int]):
        """Initialize the object properties.

        Parameters
        ----------
        filename: string
            Path to the CSV file that contains the document.
        columns: list of string
            List of column names in the dataset schema.
        primary_key: list of string
            List of index positions for sort columns.
        """
        self.filename = filename
        self.columns = columns
        self.primary_key = primary_key

    def close(self):
        """Remove the sorted (temporary) file when merging is done."""
        os.remove(self.filename)

    def partial(self, reader):
        """If the document in the CSV file is a partial document we read it
        into a data frame and use the respective data frame document method.
        This requires that partial documents in CSV files fit into main memory.

        Parameters
        ----------
        reader: histore.archive.reader.RowPositionReader
            Reader for row (key, position) tuples from the original
            snapshot version.

        Returns
        -------
        histore.document.base.Document
        """
        rows = list()
        readorder = list()
        with open(self.filename, 'r', newline='') as f:
            csvreader = csv.reader(f)
            for row in csvreader:
                rows.append(row)
                key = rowkey(row, self.primary_key)
                pos = len(readorder)
                readorder.append((pos, key, pos))
        return InMemoryDocument(
            columns=self.columns,
            rows=rows,
            readorder=readorder
        ).partial(reader)

    def reader(self, schema: Optional[List[Column]] = None) -> DocumentReader:
        """Get reader for the CSV document.

        Parameters
        ----------
        schema: list(histore.document.schema.Column), default=None
            List of columns in the document schema. Each column corresponds to
            a column in the column list of this document (corresponding to
            their position in the list). The schema columns provide the unique
            column identifier that are required by the document reader to
            generate document rows. If no schema is provided the document schema
            itself is used as the default.

        Returns
        -------
        histore.document.reader.DocumentReader
        """
        return SortedCSVDocumentReader(
            filename=self.filename,
            schema=schema if schema is not None else to_schema(self.columns),
            primary_key=self.primary_key
        )


class SortedCSVDocumentReader(DocumentReader):
    """Document reader for a sorted CSV document."""
    def __init__(self, filename, schema, primary_key):
        """Initialize the CSV reader and the list of column names.

        Parameters
        ----------
        reader: csv.reader
            Reader for the document file. If the file has a header it is
            assumed that the corresponding row has already been read.
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the document rows (based on list position). The schema
            columns provide the unique column identifier that are required to
            generate the returned document rows.
        primary_key: list of int
            List of index positions for sort columns.
        """
        self.fh = open(filename, 'r', newline='')
        self.reader = csv.reader(self.fh)
        self.schema = schema
        self.primary_key = primary_key
        self._next_row = None
        self._read_index = 0
        self.next()

    def close(self):
        """Release all resources that are held by this reader."""
        self.fh.close()

    def has_next(self):  # pragma: no cover
        """Test if the reader has more rows to read. If this reader has more
        rows the internal row buffer is not empty.

        Returns
        -------
        bool
        """
        return self._next_row is not None

    def next(self):
        """Read the next row in the document. Returns None if the end of the
        document has been reached.

        Returns
        -------
        histore.document.row.DocumentRow
        """
        result = self._next_row
        try:
            row = next(self.reader)
            # If a next row was read ensure that the number of values
            # is the same as the number of columns in the schema.
            if len(row) != len(self.schema):
                self.fh.close()
                raise ValueError('invalid row %d' % (self.reader.line_num))
            # Create document row object for the read values.
            rowpos = self._read_index
            values = dict()
            for i, col in enumerate(self.schema):
                values[col.colid] = row[i]
            key = rowkey(row, self.primary_key)
            self._next_row = DocumentRow(
                key=key,
                pos=rowpos,
                values=values
            )
            self._read_index += 1
        except StopIteration:
            # Close the file if the file is reached.
            self._next_row = None
            self.fh.close()
        # Return the buffered result
        return result


# -- Helper methods for document classes --------------------------------------

def rowkey(row, primary_key):
    """Get the key value for a row.

    Parameters
    ----------
    row: list
        List of cells in a row.
    primary_key: list
        List of index positions for primary key columns.

    Returns
    -------
    scalar or tuple
    """
    if len(primary_key) == 1:
        return to_key(row[primary_key[0]])
    else:
        return tuple([to_key(row[c]) for c in primary_key])
