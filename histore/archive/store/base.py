# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""The archive store intruduces a layer of abstraction between the archive
object and the way in which archives are managed and maintained.
"""

from abc import abstractmethod

from histore.schema.document import DocumentSchema


class ArchiveStore(object):
    """Abstract class that defines the interface to read and write the root
    node of an archive.

    The archive store maintains the document schema for the archive and the list
    of snapshot handles.
    """
    def __init__(self, schema=None, snapshots=None):
        """Initialize the document schema and list of snapshots.

        Raises ValueError if the version numbers of snapshots are not ordered
        consecutively starting at 0.

        Parameters
        ----------
        schema: histore.schema.document.DocumentSchema
        snapshots: list(histore.archive.snapshot.Snapshot)
        """
        self.schema = schema if not schema is None else DocumentSchema()
        self.snapshots = list()
        # If snapshot list is given ensure that the version numbers of all
        # snapshots are unique
        if not snapshots is None:
            for s in snapshots:
                if s.version != len(self.snapshots):
                    raise ValueError('invalid snapshot version \'' + str(s.version) + '\'')
                self.snapshots.append(s)

    def add_snapshot(self, snapshot):
        """Add a new snapshot handle to the list of document snapshots.

        Raises ValueError if the version identifier of the given snapshot is not
        equal to the length of the snapshot list.

        Parameters
        ----------
        snapshot: histore.archive.snapshot.Snapshot
        """
        if snapshot.version != len(self.snapshots):
            raise ValueError('invalid snapshot version \'' + str(snapshot.version) + '\'')
        self.snapshots.append(snapshot)
        
    @abstractmethod
    def read(self):
        """Get the root node for the archive that is maintained by this store.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, node, snapshot):
        """Relace the current root node of the archive that is maintained by
        this store with the given root node. Also adds the latest snapshot to
        the list of maintained snapshots.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement
        snapshot: histore.archive.snapshot.Snapshot
        """
        raise NotImplementedError
