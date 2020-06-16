# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""The archive store intruduces a layer of abstraction between the archive
object and the way in which archives are managed and maintained.
"""

from abc import ABCMeta, abstractmethod


class ArchiveStore(metaclass=ABCMeta):
    """Abstract class that defines the interface to read and write dataset
    archive information.
    """
    @abstractmethod
    def commit(self, schema, writer, snapshots):  # pragma: no cover
        """Commit a new version of the dataset archive. The modified components
        of the archive are given as the three arguments of this method.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the new archive version.
        writer: histore.archive.writer.ArchiveWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the new archive version.
        snapshots: histore.archive.snapshot.SnapshotListing
            Modified list of snapshots in the new archive. The new archive
            version is the last entry in the list.
        """
        raise NotImplementedError()

    @abstractmethod
    def is_empty(self):  # pragma: no cover
        """True if the archive does not contain any snapshots yet.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def get_reader(self):  # pragma: no cover
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.reader.ArchiveReader
        """
        raise NotImplementedError()

    @abstractmethod
    def get_schema(self):  # pragma: no cover
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        raise NotImplementedError()

    @abstractmethod
    def get_snapshots(self):  # pragma: no cover
        """Get listing of all snapshots in the archive.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing
        """
        raise NotImplementedError()

    @abstractmethod
    def get_writer(self):  # pragma: no cover
        """Get a writer for a new version of the archive.

        Returns
        -------
        histore.archive.writer.ArchiveWriter
        """
        raise NotImplementedError()
