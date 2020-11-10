# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Functions and classes to read CSV files as HISTORE documents."""

from typing import List, Optional

from histore.document.base import Document, PrimaryKey
from histore.document.csv.base import CSVFile, CSVReader
from histore.document.csv.sort import SortedCSVDocument
from histore.document.mem.base import InMemoryDocument
from histore.document.mem.dataframe import DataFrameDocument
from histore.document.reader import DocumentReader
from histore.document.row import DocumentRow
from histore.document.schema import Column
from histore.key.base import NumberKey

import histore.document.csv.sort as sort
import histore.document.schema as schema
import histore.key.annotate as anno


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

    def reader(self, schema: List[Column]):
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
        return SimpleCSVDocumentReader(reader=self.file.open(), schema=schema)


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
            # Close the file if the file is reached.
            self._next_row = None
            self.reader.close()
        # Return the buffered result
        return result


def open_document(
    file: CSVFile, primary_key: Optional[PrimaryKey] = None,
    max_size: Optional[float] = None
) -> Document:
    """Read a document from a csv file. If the primary key attributes are
    given the returned document is sorted by the primary key values.
    Otherwise, the original order of rows is kept and each row is assigned
    a unique index that is equal to the row position. Sorts the document by
    the primary key values (if given).

    Parameters
    ----------
    file: histore.document.csv.base.CSVFile
        Reference to the CSV file.
    primary_key: string or list, default=None
        Column(s) that are used to generate identifier for snapshot rows.
        The columns may be identified by their name or index position. If a
        string in the primary key list refers to a non-unique column name
        in the file, a ValueError is raised.
    max_size: float, default=None
        Maximum size (in MB) of the main-memory buffer for blocks of the
        CSV file that are sorted in main-memory.

    Returns
    -------
    histore.document.base.Document

    Raises
    ------
    ValueError
    """
    # with self.open() as reader:
    # Create document instance depending on whether a primary key was given
    # or not.
    if primary_key is not None:
        # If a primary key is given we first need to get the index position
        # for the key attributes in the document schema and then sort the
        # input file.
        columns = file.columns
        pk = schema.column_index(schema=columns, columns=primary_key)
        with file.open() as reader:
            buffer, filenames = sort.split(
                reader=reader,
                sortkey=pk,
                buffer_size=max_size
            )
        if not filenames:
            # If the file fits into main-memory return a sorted in-memory
            # document.
            return InMemoryDocument(
                columns=columns,
                rows=buffer,
                readorder=anno.pk_readorder(rows=buffer, primary_key=pk)
            )
        else:
            # Merge the CSV file blocks and return a sorted CSV document
            # object that wrapps the sorted CSV file.
            return SortedCSVDocument(
                filename=sort.mergesort(
                    buffer=buffer,
                    filenames=filenames,
                    sortkey=pk
                ),
                columns=columns,
                primary_key=pk
            )
    else:
        # In this case we do not need to sort the document. The document
        # will always be read in original order. Return a file reader for
        # the csv file.
        return SimpleCSVDocument(file)
