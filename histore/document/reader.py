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
from typing import Iterable

from histore.document.row import DocumentRow


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
    def close(self):
        """Release all resources that are held by this reader."""
        raise NotImplementedError()  # pragma: no cover

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
