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
from typing import Optional

import json

from histore.document.base import DataRow, OutputStream
from histore.document.schema import Schema
from histore.document.snapshot import InputDescriptor


class InputStream(OutputStream, metaclass=ABCMeta):
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
    def write_as_json(
        self, filename: str, compression: Optional[str] = None,
        encoder: Optional[json.JSONEncoder] = None
    ):
        """Write the rows in the data stream to a file in Json serialized
        format.

        Parameters
        ----------
        filename: string
            Path to the output file.
        compression: string, default=None
            String representing the compression mode for the output file.
        encoder: json.JSONEncoder, default=None
            Encoder used when writing archive rows as JSON objects to file.
        """
        raise NotImplementedError()  # pragma: no cover


class StreamOperator(metaclass=ABCMeta):
    """Abstract class for operators on rows in a data stream. The stream
    operator can be used to directly process rows in a dataset archive to
    create a new dataset snapshot.
    """
    def __init__(self, columns: Schema, snapshot: Optional[InputDescriptor] = None):
        """Initialize the schema for rows that this operator will produce.

        Parameters
        ----------
        columns: list of string
            Columns in the output schema of the operator.
        snapshot: histore.document.snapshot.InputDescriptor, default=None
            Optional metadata for the created snapshot.
        """
        self.columns = columns
        self.snapshot = snapshot

    @abstractmethod
    def eval(self, rowid: int, row: DataRow) -> DataRow:
        """Evaluate the operator on the given row.

        Returns the processed row. If the result is None this signals that the
        given row should not be part of the collected result.

        Parameters
        -----------
        rowid: int
            Unique identifier for the processed row.
        row: list
            List of values in the row.
        """
        raise NotImplementedError()  # pragma: no cover
