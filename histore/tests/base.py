# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper classes for unit tests."""

from typing import List

import pandas as pd

from histore.document.base import DocumentConsumer
from histore.document.json.writer import JsonWriter
from histore.document.row import DocumentRow
from histore.document.schema import Column
from histore.document.stream import InputStream
from histore.key.base import NumberKey


class DataFrameStream(InputStream):
    """Simple input stream implementation using a pandas data frame."""
    def __init__(self, df: pd.DataFrame):
        """Initialize the data frame.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.
        """
        super(DataFrameStream, self).__init__(columns=list(df.columns))
        self.df = df

    def stream_to_archive(self, schema: List[Column], version: int, consumer: DocumentConsumer):
        """Write rows from a document to a stream consumer.

        Parameters
        ----------
        schema: list of histore.document.schema.Column
            List of columns (with unique identifier). The order of entries in
            this list corresponds to the order of columns in the document schema.
        version: int
            Unique identifier for the new snapshot version.
        consumer: histore.document.base.DocumentConsumer
            Consumer for rows in the stream.
        """
        pos = 0
        for _, values in self.df.iterrows():
            values = {c.colid: v for c, v in zip(schema, values)}
            row = DocumentRow(key=NumberKey(pos), pos=pos, values=values)
            consumer.consume_document_row(row=row, version=version)
            pos += 1

    def write_as_json(self, filename: str):
        """Write the data to a Json file.

        Parameters
        ----------
        filename: string
            Path to the output file.
        """
        writer = JsonWriter(filename=filename)
        try:
            writer.write(list(self.df.columns))
            for rowid, values in self.df.iterrows():
                writer.write(row=[rowid, list(values)])
        finally:
            writer.close()


class ListReader(object):
    """Implementation of the reader interface. Returns values from a given list
    until the list is empty.
    """
    def __init__(self, values):
        """Initialize the list of returned values.

        Parameters
        ----------
        values: list
            List of returned values.
        """
        # Reverse the list of values to be able to remove values in original
        # order using pop()
        self.values = values[::-1]

    def has_next(self):
        """Returns True if the internal list is not empty (i.e., there are
        values left to be returned by the next() method).

        Returns
        -------
        bool
        """
        return self.values

    def next(self):
        """Return the next value in the list.

        Returns
        -------
        object
        """
        if self.has_next():
            return self.values.pop()
