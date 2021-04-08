# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Iterator for document rows. Provides a reader that allows to iterate over
the rows in a data frame sorted by the row key.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Iterable, List

from histore.document.row import DocumentRow
from histore.document.schema import Column, Schema
from histore.document.stream import InputStream, StreamConsumer


class DocumentReader(metaclass=ABCMeta):
    """Reader for rows in a document. Reads rows in order defined by the row
    key that is used for merging the document.
    """
    def __iter__(self):
        """Make the reader instance iterable by returning a generator that
        yields all rows.

        Returns
        -------
        Generator
        """
        return row_stream(self)

    @abstractmethod
    def has_next(self) -> bool:
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otherwise, the next() method will return
        None.

        Returns
        -------
        bool
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def next(self) -> DocumentRow:
        """Read the next row in the document. Returns None if the end of the
        document has been reached.

        Returns
        -------
        histore.document.row.DocumentRow
        """
        raise NotImplementedError()  # pragma: no cover

    def stream(self) -> DocumentStream:
        """Get an input stream object for the document.

        The input stream is used, for example, to load an initial dataset
        snapshot into an empty archive.

        Returns
        -------
        histore.document.reader.DocumentStream
        """
        raise NotImplementedError()  # pragma: no cover


class DocumentStream(InputStream):
    """Input stream implementation for a document reader."""
    def __init__(self, columns: Schema, doc: DocumentReader):
        """Initialize the list of columns in the document schema and the reader
        for document rows.

        Parameters
        ----------
        columns: list of string, default=None
            List of column names in the schema of the data stream rows.
        doc: histore.document.reader.DocumentReader
            Reader for document rows.
        """
        super(DocumentStream, self).__init__(columns=columns)
        self.doc = doc

    def stream_to_archive(self, schema: List[Column], version: int, consumer: StreamConsumer):
        """Write rows from a document to a stream consumer.

        Parameters
        ----------
        schema: list of histore.document.schema.Column
            List of columns (with unique identifier). The order of entries in
            this list corresponds to the order of columns in the document schema.
        version: int
            Unique identifier for the new snapshot version.
        consumer: histore.document.stream.StreamCounsumer
            Consumer for rows in the stream.
        """
        for row in self.doc:
            consumer.consume_document_row(row=row, version=version)


# -- Helper Methods -----------------------------------------------------------

def row_stream(reader: DocumentReader) -> Iterable:
    """Geterator that yields all rows in a document reader.

    Parameters
    ----------
    reader: histore.document.reader.DocumentReader
        Document reader over which we are iterating.

    Returns
    -------
    histore.document.row.DocumentRow
    """
    while reader.has_next():
        yield reader.next()
