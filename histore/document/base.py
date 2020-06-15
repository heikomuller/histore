# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Documents are wrappers around different forms of snapshots that may be
committed to an archive.
"""

from abc import ABCMeta, abstractmethod


class Document(metaclass=ABCMeta):
    """The document interface provides access to a document reader. The reader
    is expected to give access to a document that is sorted in ascending order
    of the row key that is used by an archive during merge.
    """
    def __init__(self, columns):
        """Initialize the object properties. The abstract document class
        maintains a list of column names in the document schema. Columns may
        either be represented by strings or by instances of the Column class.

        Parameters
        ----------
        columns: list
            List of column names. The number of values in each document row is
            expected to be the same as the number of columns and the order of
            values in each row is expected to correspond to their respective
            column in this list.
        """
        self.columns = columns

    @abstractmethod
    def close(self):  # pragma: no cover
        """Signal that the archive merge is done reading the document. Any
        resources (e.g., temporary files) that were created for the document
        can be released.
        """
        raise NotImplementedError()

    @abstractmethod
    def partial(self, reader):  # pragma: no cover
        """Return a copy of the document that provides access to the set of
        rows such that the document is considered a partial document. In a
        partial document the rows are aligned with the position of the
        corresponding row in the document origin. The original row order is
        accessible via the given row position reader.

        Parameters
        ----------
        reader: histore.archive.reader.RowPositionReader
            Reader for row (key, position) tuples from the original
            snapshot version.

        Returns
        -------
        histore.document.base.Document
        """
        raise NotImplementedError()

    @abstractmethod
    def reader(self, schema):  # pragma: no cover
        """Get reader for data frame rows ordered by their row identifier. In
        a partial document the row positions that are returned by the reader
        are aligned with the positions of the corresponding rows in the
        document of origin.

        Parameters
        ----------
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the column list of this document (corresponding to
            their position in the list). The schema columns provide the unique
            column identifier that are required by the document reader to
            generate document rows. An error is raised if the number of
            elements in the schema does not match the number of columns in the
            data frame.

        Returns
        -------
        histore.document.reader.DocumentReader

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()
