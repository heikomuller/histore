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

from typing import Callable, List, Optional

import json

from histore.document.base import Document
from histore.document.external import ExternalDocument
from histore.document.json.reader import JsonReader, JsonIterator
from histore.document.sort import SortEngine


class JsonDocument(ExternalDocument):
    """File containing a document that is serialized as a list of Json rows.
    The first row is assumed to contain the list of column names. The following
    rows are triples of row position, row identifier and a list of cell values.
    """
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        encoder: Optional[json.JSONEncoder] = None, decoder: Optional[Callable] = None,
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
        encoder: json.JSONEncoder, default=None
            Encoder used when writing rows as JSON objects to file during
            external merge sort.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        delete_on_close: bool, default=False
            If True, delete the file when the document is closed.
        """
        self.compression = compression
        self.encoder = encoder
        self.decoder = decoder
        # Open the reader for the input file to get the list of column names
        # from the first row. If the file is empty the list of rows is empty.
        reader = JsonReader(
            filename=filename,
            compression=compression,
            decoder=decoder
        )
        try:
            columns = next(reader)
        except StopIteration:
            columns = []
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

    def sorted(self, keys: List[int], buffersize: Optional[float] = None) -> Document:
        """Sort the document rows based on the values in the key columns.

        Key columns are specified by their index position. Returns a new
        document.

        Parameters
        ----------
        keys: list of int
            Index position of sort columns.
        buffersize: float, default=None
            Maximum size (in bytes) of file blocks that are kept in main-memory.

        Returns
        -------
        histore.document.base.Document
        """
        sort = SortEngine(buffersize=buffersize, encoder=self.encoder, decoder=self.decoder)
        return sort.sorted(doc=self, keys=keys)
