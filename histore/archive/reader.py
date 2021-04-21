# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

from abc import ABCMeta, abstractmethod
from typing import Callable, List, Tuple

from histore.document.base import DefaultDocument, DocumentIterator, RowIndex, DataRow
from histore.document.schema import Column


class ArchiveReader(metaclass=ABCMeta):
    """Reader for rows in a dataset archive. Reads rows in ascending order of
    their identifier.
    """
    def __enter__(self) -> DocumentIterator:
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        """Close the document iterator when the context manager exits."""
        self.close()
        return False

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
        """Release all reseources that are associated with the reader."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def next(self):
        """Read the next row in the dataset archive.

        Returns None if the end of the archive rows has been reached.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        raise NotImplementedError()  # pragma: no cover


class SnapshotIterator(DocumentIterator):
    """Reader for data streams. Provides the functionality to open the stream
    for reading. Dataset reader should be able to read the same dataset
    multiple times.
    """
    def __init__(self, reader: ArchiveReader, version: int, schema: List[Column], is_keyed: bool):
        """Initialize the archive reader.

        Parameters
        ----------
        reader: histore.archive.reader.ArchiveReader
            Reader for a dataset archive.
        version: int
            Unique version identifier for the read snapshot.
        schema: list of histore.document.schema.Column
            Schema for the read snapshot.
        is_keyed: bool
            Flag indicating whether the archive snapshots are keyed by a primary
            key or not. The type of archive key determines the value for the
            row index when reading the document.
        """
        self.reader = reader
        self.version = version
        self.colids = [c.colid for c in schema]
        self.is_keyed = is_keyed

    def close(self):
        """Close the associated archive reader and set it to None (to avoid
        repeated attempts to close it multiple times).
        """
        if self.reader is not None:
            self.reader.close()
            self.reader = None

    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the document.

        Returns the row position, row index and the list of cell values for each
        of the document columns. Raises a StopIteration error if an attempt is
        made to read past the end of the document.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        """Return next row from the archive reader."""
        row = self.reader.next()
        while row is not None:
            if row.timestamp.contains(self.version):
                pos, vals = row.at_version(self.version, self.colids)
                rowidx = row.rowid if self.is_keyed else row.key.value
                return pos, rowidx, vals
            row = self.reader.next()
        self.close()
        raise StopIteration()


class SnapshotReader(DefaultDocument):
    """Stream reader for rows in a dataset archive snapshot."""
    def __init__(self, reader: Callable, version: int, schema: List[Column], is_keyed: bool):
        """Initialize the function to open the archive reader and information
        about the read snapshot.

        Parameters
        ----------
        reader: callable
            Callable that returns a new archive reader.
        version: int
            Unique version identifier for the read snapshot.
        schema: list of histore.document.schema.Column
            Schema for the read snapshot.
        is_keyed: bool
            Flag indicating whether the archive snapshots are keyed by a primary
            key or not. The type of archive key determines the value for the
            row index when reading the document.
        """
        super(SnapshotReader, self).__init__(columns=schema)
        self.reader = reader
        self.schema = schema
        self.version = version
        self.is_keyed = is_keyed

    def close(self):
        """There are no resources that need to be released."""
        pass

    def open(self) -> SnapshotIterator:
        """Open the data stream to get a iterator for the rows in the dataset
        snapshot.

        Returns
        -------
        histore.archive.reader.SnapshotIterator
        """
        return SnapshotIterator(
            reader=self.reader(),
            version=self.version,
            schema=self.schema,
            is_keyed=self.is_keyed
        )


# -- Helper Methods -----------------------------------------------------------

def row_stream(reader):
    """Geterator that yields all rows in an archive.

    Parameters
    ----------
    reader: histore.arcive.reader.ArchiveReader
        Archive reader over which we are iterating.

    Returns
    -------
    histore.archive.row.ArchiveRow
    """
    while True:
        row = reader.next()
        if row is None:
            break
        yield row
