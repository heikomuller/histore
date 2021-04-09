# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

from abc import ABCMeta, abstractmethod
from typing import Callable, List

from histore.document.base import DataIterator, DataReader
from histore.document.schema import Column


class ArchiveReader(metaclass=ABCMeta):
    """Reader for rows in a dataset archive. Reads rows in ascending order of
    their identifier.
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
        """Release all reseources that are associated with the reader."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def has_next(self):
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otherwise, the next() method will return
        None.

        Returns
        -------
        bool
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def next(self):
        """Read the next row in the dataset archive. Returns None if the end of
        the archive rows has been reached.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        raise NotImplementedError()  # pragma: no cover


class RowPositionReader(object):
    """Reader for row index information for a given snapshot version in a
    dataset archive. The row information is read in ascending order of the
    row identifier in the archive.
    """
    def __init__(self, reader, version):
        """Initialize the archive reader and the version for which the index is
        being read.

        Parameters
        ----------
        reader: histore.archive.reader.ArchiveReader
            Reader for a dataset archive.
        version: int
            Identifier of version for which the row index is being read.
        """
        self.reader = reader
        self.version = version

    def next(self):
        """Get information for the next row in the archive. The result is a
        tuple of row key and position. If the end of the archive has been
        reached the result is None.

        Returns
        -------
        tuple
        """
        while self.reader.has_next():
            row = self.reader.next()
            if row.timestamp.contains(self.version):
                return (row.key, row.pos.at_version(self.version))


class SnapshotIterator(DataIterator):
    """Reader for data streams. Provides the functionality to open the stream
    for reading. Dataset reader should be able to read the same dataset
    multiple times.
    """
    def __init__(self, reader: ArchiveReader, version: int, schema: List[Column]):
        """Initialize the archive reader.

        Parameters
        ----------
        reader: histore.archive.reader.ArchiveReader
            Reader for a dataset archive.
        version: int
            Unique version identifier for the read snapshot.
        schema: list of histore.document.schema.Column
            Schema for the read snapshot.
        """
        self.reader = reader
        self.version = version
        self.colids = [c.colid for c in schema]

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated archive reader when the context manager exits."""
        self.close()
        return False

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def __next__(self):
        """Return next row from the archive reader."""
        while self.reader.has_next():
            row = self.reader.next()
            if row.timestamp.contains(self.version):
                _, vals = row.at_version(self.version, self.colids)
                return row.rowid, vals
        self.close()
        raise StopIteration()

    def close(self):
        """Close the associated archive reader and set it to None (to avoid
        repeated attempts to close it multiple times).
        """
        if self.reader is not None:
            self.reader.close()
            self.reader = None


class SnapshotReader(DataReader):
    """Stream reader for rows in a dataset archive snapshot."""
    def __init__(self, reader: Callable, version: int, schema: List[Column]):
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
        """
        self.reader = reader
        self.version = version
        self.schema = schema

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
            schema=self.schema
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
    while reader.has_next():
        yield reader.next()
