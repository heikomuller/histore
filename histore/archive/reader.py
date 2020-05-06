# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

from abc import ABCMeta, abstractmethod


class ArchiveReader(metaclass=ABCMeta):
    """Reader for rows in a dataset archive. Reads rows in ascending order of
    their identifier.
    """
    @abstractmethod
    def has_next(self):
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otherwise, the next() method will return
        None.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def next(self):
        """Read the next row in the dataset archive. Returns None if the end of
        the archive rows has been reached.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        raise NotImplementedError()


class RowIndexReader(object):
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
        tuple of row identifier and position. If the end of the archive has
        been reached the result is None.

        Returns
        -------
        tuple
        """
        while self.reader.has_next():
            row = self.reader.next()
            if row.timestamp.contains(self.version):
                return (row.key, row.pos.at_version(self.version))
