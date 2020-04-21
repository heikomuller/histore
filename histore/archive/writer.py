# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class that defines the interface for the archive writer."""

from abc import ABCMeta, abstractmethod


class ArchiveWriter(metaclass=ABCMeta):
    """The archive writer interface defines a single method for adding
    individual archive rows to the output stream.
    """
    @abstractmethod
    def write_archive_row(self, row):
        """Add the given row to a new archive version.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_document_row(self, row, version):
        """Add a given document row to a new archive version with the given
        identifier.

        Parameters
        ----------
        row: histore.document.row.DocumentRow
            Row from an inout document (snapshot) that is being added to the
            archive snapshot for the given version.
        version: int
            Unique identifier for the snapshot version that the document row is
            added to.
        """
        raise NotImplementedError()
