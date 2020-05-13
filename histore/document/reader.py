# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Iterator for document rows. Provides a reader that allows to iterate over
the rows in a data frame sorted by the row key.
"""

from abc import ABCMeta, abstractmethod


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
    def has_next(self):  # pragma: no cover
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otherwise, the next() method will return
        None.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def next(self):  # pragma: no cover
        """Read the next row in the document. Returns None if the end of the
        document has been reached.

        Returns
        -------
        histore.document.row.DocumentRow
        """
        raise NotImplementedError()


# -- Helper Methods -----------------------------------------------------------

def row_stream(reader):
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
