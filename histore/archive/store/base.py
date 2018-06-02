# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""The archive store intruduces a layer of abstraction between the archive
object and the way in which archives are managed and maintained.
"""

from abc import abstractmethod

from histore.timestamp import TimeInterval, Timestamp


class ArchiveStore(object):
    """Abstract class that defines the interface to read and write the root
    node of an archive.

    The archive store maintains the document schema for the archive and the list
    of snapshot handles.
    """
    @abstractmethod
    def get_root(self):
        """Get the root node for the archive that is maintained by this store.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        raise NotImplementedError()

    @abstractmethod
    def get_schema(self):
        """Get the current archive schema.

        Returns
        -------
        histore.schema.document.DocumentSchema
        """
        raise NotImplementedError()

    @abstractmethod
    def get_snapshots(self):
        """Get the current list of document snapshot handles.

        Returns
        -------
        list(histore.archive.snapshot.Snapshot)
        """
        raise NotImplementedError()

    @abstractmethod
    def read(self):
        """Read the complete archive information. Returns a triple containing
        the archive root, the list of snapshots, and the archive schema.

        Returns
        -------
        histore.archive.node.ArchiveElement
        list(histore.archive.snapshot.Snapshot)
        histore.schema.document.DocumentSchema
        """
        raise NotImplementedError()

    def validate_snapshots(self, root, snapshots):
        """Validate a given list of snapshots. Ensures that the version number
        of a snapshot is equal to its index position in the list.

        Raises ValueError if the given list of snapshots is not valid.

        Parameters
        ----------
        snapshots: list(histore.archive.snapshot.Snapshot)
        """
        for index in range(len(snapshots)):
            s = snapshots[index]
            if s.version != index:
                raise ValueError('invalid snapshot \'' + str(s) + ' \' at ' + str(index) + '\'')
        # Ensure that the root timestamp contains exactly the versions in the
        # snapshot list
        t = Timestamp([TimeInterval(0, len(snapshots) - 1)])
        if not root.timestamp.is_equal(t):
            raise ValueError('root timestamp \'' + str(root.timestamp) + '\' does not match snapshot versions \'' + str(t) + '\'')

    @abstractmethod
    def write(self, root, snapshots, schema=None):
        """Relace the current archive information with an updated version
        (e.g., after adding a new snapshot to the archive). At this point the
        schema is not expected to be changed after the archive is created.
        However, the system is capable to manage changes to the schema if they
        only afect elements that have not been present in any of the previous
        document snapshots.

        Parameters
        ----------
        root: histore.archive.node.ArchiveElement
        snapshots: list(histore.archive.snapshot.Snapshot)
        schema: histore.schema.document.DocumentSchema
        """
        raise NotImplementedError()
