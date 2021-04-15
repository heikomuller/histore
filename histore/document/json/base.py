# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Documents that are serialized as Json arrays. The file format is a list of
lists (rows). The first row is expected to be the list of column names. All
following rows are triples of row position, row identifier and a list of cell
values for each of the columns.
"""

from typing import Callable, Optional, Tuple

import json

from histore.document.base import DataRow, DocumentIterator, RowIndex
from histore.document.external import ExternalDocument
from histore.document.json.reader import JsonReader


# -- Documents ----------------------------------------------------------------

class JsonIterator(DocumentIterator):
    """Document iterator for documents that have been serialized as Json
    documents. The iterator expects a file that contains a list if lists (rows).
    The first row is expected to contain the document schema. Data rows are
    expected to be triples of row position, row index, and cell values.
    """
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the input file, the row deserializer, and the Json
        decoder.

        Parameters
        ----------
        filename: string
            Path to the data file.
        compression: string, default=None
            String representing the compression mode for the output file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        """
        self.reader = JsonReader(
            filename=filename,
            compression=compression,
            decoder=decoder
        )
        # Skip the column name row.
        if self.reader.has_next():
            next(self.reader)

    def close(self):
        """Close the associated Json reader."""
        self.reader.close()

    def has_next(self) -> bool:
        """Test if the iterator has more rows to read.

        Returns
        -------
        bool
        """
        return self.reader.has_next()

    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the document.

        Returns the row position, row index and the list of cell values for each
        of the document columns. Raises a StopIteration error if an attempt is
        made to read past the end of the document.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        rowpos, rowidx, values = next(self.reader)
        return rowpos, rowidx, values


class JsonDocument(ExternalDocument):
    """File containing a document that is serialized as a list of Json rows.
    The first row is assumed to contain the list of column names. The following
    rows are triples of row position, row identifier and a list of cell values.
    """
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        decoder: Optional[Callable] = None, encoder: Optional[json.JSONEncoder] = None,
        delete_on_close: Optional[bool] = False
    ):
        """Initialize inofrmation about the file that contains the Json
        serialization of the document.

        Parameters
        ----------
        filename: string
            Path to the data file.
        compression: string, default=None
            String representing the compression mode for the output file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        encoder: json.JSONEncoder, default=None
            Encoder used when writing rows as JSON objects to file during
            external merge sort.
        delete_on_close: bool, default=False
            If True, delete the file when the document is closed.
        """
        self.compression = compression
        self.decoder = decoder
        # Open the reader for the input file to get the list of column names
        # from the first row. If the file is empty the list of rows is empty.
        reader = JsonReader(
            filename=filename,
            compression=compression,
            decoder=decoder
        )
        columns = next(reader) if reader.has_next() else []
        reader.close()
        # Initialize the super class.
        super(JsonDocument, self).__init__(
            columns=columns,
            filename=filename,
            delete_on_close=delete_on_close
        )

    def open(self) -> JsonIterator:
        """Open the document to get a iterator for the rows in the document.

        Returns
        -------
        histore.document.json.base.JsonIterator
        """
        return JsonIterator(
            filename=self.filename,
            compression=self.compression,
            decoder=self.decoder
        )
