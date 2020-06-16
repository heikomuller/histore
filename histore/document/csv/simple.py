# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Functions and classes to read CSV files as HISTORE documents."""

import csv
import gzip
import pandas as pd

from histore.document.base import Document
from histore.document.mem.dataframe import DataFrameDocument
from histore.document.reader import DocumentReader
from histore.document.row import DocumentRow
from histore.key.base import NumberKey


class SimpleCSVDocument(Document):
    """CSV file containing a document that is keyed by the row position and
    not a primary key (refered to as an un-keyed document). Returns rows in
    document order with the row identifier being the line number (starting at
    zero and ignoring an optional column header).
    """
    def __init__(
        self, filename, columns, delimiter=',', quotechar='"',
        compression=None, has_header=True
    ):
        """Initialize the object properties. The abstract document class
        maintains a list of column names in the document schema. Columns may
        either be represented by strings or by instances of the Column class.

        Parameters
        ----------
        filename: string
            Path to the CSV file that contains the document.
        columns: list
            List of column names. The number of values in each document row is
            expected to be the same as the number of columns and the order of
            values in each row is expected to correspond to their respective
            column in this list.
        delimiter: string, default=','
            A one-character string used to separate fields.
        quotechar: string, default='"'
            A one-character string used to quote fields containing special
            characters, such as the delimiter or quotechar, or which contain
            new-line characters.
        compression: string, default=None
            String identifier for the file compression format. Currently only
            'gzip' compression is supported.
        has_header: bool, default=True
            Flag indicating whether the first row of the given file contains
            column names. The header row will be ignored by the reader in favor
            of the given column names.
        """
        self.filename = filename
        self.columns = columns
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.compression = compression
        self.has_header = has_header

    def close(self):
        """The un-keyed CSV document reader has no additional resources that
        need to be released.
        """
        pass  # pragma: no cover

    def open(self):
        """Open the associated file for reading.

        Returns
        -------
        FileObject
        """
        if self.compression == 'gzip':
            return gzip.open(self.filename, 'rt')
        else:
            return open(self.filename, 'r')

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
        df = pd.read_csv(
            self.filename,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            header=0 if self.has_header else None,
            names=self.columns,
            compression=self.compression
        )
        return DataFrameDocument(df).partial(reader)

    def reader(self, schema):
        """Get reader for data frame rows ordered by their row identifier. In
        a partial document the row positions that are returned by the reader
        are aligned with the positions of the corresponding rows in the
        document of origin.

        Parameters
        ----------
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the column list of this document (corresponding to
            their position in the list). The schema columns provide the unique
            column identifier that are required by the document reader to
            generate document rows. An error is raised if the number of
            elements in the schema does not match the number of columns in the
            data frame.

        Returns
        -------
        histore.document.reader.DocumentReader

        Raises
        ------
        ValueError
        """
        # Open the input file and skip the header.
        f = self.open()
        try:
            reader = csv.reader(
                f,
                delimiter=self.delimiter,
                quotechar=self.quotechar
            )
            if self.has_header:
                # Skip the column header (if present).
                next(reader)
        except IOError as ex:  # pragma: no cover
            # Ensure that the file is closed in case of an error.
            f.close()
            raise ex
        return SimpleCSVDocumentReader(reader=reader, schema=schema, fh=f)


class SimpleCSVDocumentReader(DocumentReader):
    """Document reader for un-keyed CSV documents."""
    def __init__(self, reader, schema, fh):
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
        fh: FileObject
            Handle for the file that is being read. Required to be able to
            properly close the file when done with reading.
        """
        self.reader = reader
        self.schema = schema
        self.fh = fh
        self._next_row = None
        self._row_id = 0
        self.next()

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
            rowid = self._row_id
            values = dict()
            for i, col in enumerate(self.schema):
                values[col.colid] = row[i]
            self._next_row = DocumentRow(
                key=NumberKey(rowid),
                pos=rowid,
                values=values
            )
            self._row_id += 1
        except StopIteration:
            # Close the file if the file is reached.
            self._next_row = None
            self.fh.close()
        # Return the buffered result
        return result
