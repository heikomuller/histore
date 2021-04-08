# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Interfaces for mixin classes that allow creating a first dataset archive
version from a data stream.
"""

from abc import ABCMeta, abstractmethod
from typing import List

from histore.document.row import DocumentRow
from histore.document.schema import Column, Schema


class DocumentConsumer(metaclass=ABCMeta):
    """Mixin class for stream consumer that are used to write document rows to
    a archive store.
    """
    @abstractmethod
    def consume_document_row(self, row: DocumentRow, version: int):
        """Add a given document row to a new archive version.

        Parameters
        ----------
        row: histore.document.row.DocumentRow
            Row from an input stream (snapshot) that is being added to the
            archive snapshot for the given version.
        version: int
            Unique identifier for the new snapshot version.
        """
        raise NotImplementedError()  # pragma: no cover


class InputStream(metaclass=ABCMeta):
    """Abstract class for data input streams. An input stream contains informaton
    about the schema of the stream rows and a method that allows to output the
    stream rows to a dataset archive.
    """
    def __init__(self, columns: Schema):
        """Initialize the list of columns in the stream schema.

        Parameters
        ----------
        columns: list of string, default=None
            List of column names in the schema of the data stream rows.
        """
        self.columns = columns

    @abstractmethod
    def stream_to_archive(self, schema: List[Column], version: int, consumer: DocumentConsumer):
        """Write rows in a stream to a consumer.

        Parameters
        ----------
        schema: list of histore.document.schema.Column
            List of columns (with unique identifier). The order of entries in
            this list corresponds to the order of columns in the stream schema.
        version: int
            Unique identifier for the new snapshot version.
        consumer: histore.document.stream.StreamCounsumer
            Consumer for rows in the stream.
        """
        raise NotImplementedError()  # pragma: no cover
