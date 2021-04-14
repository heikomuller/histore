# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Documents that are serialized as Json arrays. The file format is a list of
rows with the first row expected to be the column headers. The following rows
are pairs of row identifier and a list of cell values.
"""

from typing import Callable, List, Optional

import pandas as pd

from histore.document.base import DataIterator, Document
from histore.document.json.reader import JsonReader
from histore.document.mem.dataframe import DataFrameDocument
from histore.document.reader import DocumentReader
from histore.document.row import DocumentRow
from histore.document.schema import Column, to_schema
from histore.key.base import NumberKey


# -- Documents ----------------------------------------------------------------

class JsonDocument(Document):
    """File containing a document that is serialized as a list of Json rows.
    The first row is assumed to contain the list of column names. The following
    rows are pairs of row identifier and a list of cell values.
    """
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the Json reader.

        Parameters
        ----------
        filename: string
            Path to the output file.
        compression: string, default=None
            String representing the compression mode for the output file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        """
        # Open the reader for the input file.
        self._reader = JsonReader(
            filename=filename,
            compression=compression,
            decoder=decoder
        )
        # Get the list of columns from the first row. If the file is empty the
        # list of rows is empty.
        columns = self._reader.next() if self._reader.has_next() else []
        super(JsonDocument, self).__init__(columns=columns)

    def close(self):
        """Close the Json file reader."""
        self._reader.close()

    def partial(self, reader):
        """If the document in the file is a partial document we read it
        as a data frame.

        Parameters
        ----------
        reader: histore.archive.reader.RowPositionReader
            Reader for row (key, position) tuples from the original
            snapshot version.

        Returns
        -------
        histore.document.base.Document
        """
        return DataFrameDocument(self.read_df()).partial(reader)

    def reader(self, schema: Optional[List[Column]] = None) -> DocumentReader:
        """Get reader for document rows.

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
        return JsonDocumentReader(
            reader=self._reader,
            schema=schema if schema is not None else to_schema(self.columns)
        )

    def read_df(self) -> pd.DataFrame:
        """Read rows in the document as a pandas data frame.

        Returns
        -------
        pd.DataFrame
        """
        data, index = list(), list()
        while self._reader.has_next():
            rowid, row = self._reader.next()
            index.append(rowid)
            data.append(row)
        return pd.DataFrame(data=data, index=index, columns=self.columns, dtype=object)


class JsonDocumentReader(DocumentReader):
    """Document reader for Json documents."""
    def __init__(self, reader: JsonReader, schema: List[Column]):
        """Initialize the Json reader and the list of column names.

        Parameters
        ----------
        reader: histore.document.json.JsonReader
            Reader for the document file.
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

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        self.reader.close()
        return False

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def __next__(self):
        """Return next row from JSON reader. Raise a StopIteration error when
        the end of the file is reached.
        """
        if not self.has_next():
            raise StopIteration()
        return self.next()

    def close(self):
        """Release all resources that are held by this reader."""
        self.reader.close()

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
        result = self._next_row
        if not self.reader.has_next():
            self._next_row = None
            return result
        rowid, row = self.reader.next()
        # If a next row was read ensure that the number of values
        # is the same as the number of columns in the schema.
        if len(row) != len(self.schema):
            self.reader.close()
            raise ValueError('invalid row {}'.format(rowid))
        # Create document row object for the read values.
        values = dict()
        for i, col in enumerate(self.schema):
            values[col.colid] = row[i]
        self._next_row = DocumentRow(
            key=NumberKey(rowid),
            pos=rowid,
            values=values
        )
        return result
