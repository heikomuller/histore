# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Documents are wrappers around different forms of snapshots that may be
committed to an archive. A document primarily serves two purposes: (1) it
contains the document schema (a list of column names), and (2) it supports
reading the document rows.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd

from histore.document.schema import DocumentSchema


"""Type aliases."""
# Scalar values and dataset rows.
Scalar = Union[int, float, str, datetime]
DataRow = List[Scalar]
RowIndex = Union[Scalar, Tuple[Scalar, ...]]


# -- Input Documents ----------------------------------------------------------

class DocumentIterator(metaclass=ABCMeta):
    """Iterator over rows in a document. Each row is represented as a list of
    cell values for the document columns. Cell values are expected to be scalar
    values. In addition to the cell values, for each row the iterator provides
    access to the row position in the document (starting at 0) and the row
    index. The row index is used as the row key when merging documents into
    archives that are not keyed by a primary key.

    For most external documents the row position and the row index will be the
    same value. Documents that represent ``pandas.DataFrame`` objects, on the
    other hand, will use the value from the data frame index for each row as
    the returned row index.
    """
    def __enter__(self) -> DocumentIterator:
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        """Close the document iterator when the context manager exits."""
        self.close()
        return False

    def __iter__(self) -> DocumentIterator:
        """Return object for row iteration."""
        return self

    def __next__(self) -> Tuple[int, RowIndex, DataRow]:
        """Return next row from the iterator.

        Raises a StopIteration error when the end of the document is reached.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        return self.next()

    @abstractmethod
    def close(self):
        """Release all resources that are held by the iterator."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the document.

        Returns the row position, row index and the list of cell values for each
        of the document columns. Raises a StopIteration error if an attempt is
        made to read past the end of the document.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        raise NotImplementedError()  # pragma: no cover


class Document(metaclass=ABCMeta):
    """The abstract document class maintains the document schema and provides
    access to the document rows via a document iterator.
    """
    def __init__(self, columns: DocumentSchema):
        """Initialize the document schema.

        Parameters
        ----------
        columns: list of string
            List of column names. The number of values in each document row is
            expected to be the same as the number of columns and the order of
            values in each row is expected to correspond to their respective
            column in this list.
        """
        self.columns = columns

    @abstractmethod
    def close(self):
        """Signal that processing of the document is finished.

        Any resources that were created by the document (e.g., temporary files)
        can be released.
        """
        raise NotImplementedError()  # pragma: no cover

    def iterrows(self) -> Iterator[Tuple[RowIndex, DataRow]]:
        """Simulate the iterrows() function of a pandas DataFrame.

        Returns an iterator that yields pairs of row index and row value lists
        for each row in the streamed data frame.

        Returns
        -------
        iterator
        """
        with self.open() as f:
            for _, rowidx, row in f:
                yield rowidx, row

    @abstractmethod
    def open(self) -> DocumentIterator:
        """Open the document to get a iterator for the rows in the document.

        Returns
        -------
        histore.document.base.DocumentIterator
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Create data frame from the document rows.

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
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
        raise NotImplementedError()  # pragma: no cover


class DefaultDocument(Document):
    """Document that provides default implementation for the to_df() and
    sorted() methods of the Document class.
    """
    def __init__(self, columns: DocumentSchema):
        """Initialize the document schema.

        Parameters
        ----------
        columns: list of string
            List of column names.
        """
        super(DefaultDocument, self).__init__(columns=columns)

    def to_df(self) -> pd.DataFrame:
        """Create data frame from the document rows.

        Returns
        -------
        pd.DataFrame
        """
        return document_to_df(self)

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
        from histore.document.sort import SortEngine
        return SortEngine(buffersize=buffersize).sorted(doc=self, keys=keys)


# -- Document descriptors -----------------------------------------------------

@dataclass
class InputDescriptor:
    """Descriptor for archive snapshot input documents."""
    # Optional user-provided description for the snapshot.
    description: Optional[str] = ''
    # Timestamp when the snapshot was first valid.
    valid_time: Optional[datetime] = None
    # Optional metadata defining the action that created the snapshot.
    action: Optional[Dict] = None


# -- Helper Functions ---------------------------------------------------------

def document_to_df(doc: Document) -> pd.DataFrame:
    """Create data frame from the document rows.

    Parameters
    ----------
    doc: histore.document.base.Document
        Input document.

    Returns
    -------
    pd.DataFrame
    """
    data, index = list(), list()
    for rowid, row in doc.iterrows():
        index.append(rowid)
        data.append(row)
    return pd.DataFrame(data=data, index=index, columns=doc.columns, dtype=object)
