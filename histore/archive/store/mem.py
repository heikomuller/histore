# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Simple in-memory store for archives."""

from histore.archive.store.base import ArchiveStore
from histore.schema.document import DocumentSchema


class InMemoryArchiveStore(ArchiveStore):
    """Implements an archive store that keept the archive root node in memory
    """
    def __init__(self, root=None, schema=None, snapshots=None):
        """Initialize the archive root node, document schema and list of
        document snapshot handles. By default the root is None.

        Parameters
        ----------
        root: histore.archive.node.ArchiveElement, optional
        schema: histore.schema.document.DocumentSchema
        snapshots: list(histore.archive.snapshot.Snapshot)
        """
        self.root = root
        self.snapshots = snapshots if not snapshots is None else list()
        self.schema = schema if not schema is None else DocumentSchema()
        # Validate snapshots if given and root is not None
        if self.root is None and len(self.snapshots) > 0:
            raise ValueError('invalid list of snapshots for empty archive')
        elif not self.root is None:
            self.validate_snapshots(self.root, self.snapshots)

    def get_root(self):
        """Get the root node for the archive that is maintained by this store.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        return self.root

    def get_schema(self):
        """Get the current archive schema.

        Returns
        -------
        histore.schema.document.DocumentSchema
        """
        return self.schema

    def get_snapshots(self):
        """Get the current list of document snapshot handles.

        Returns
        -------
        list(histore.archive.snapshot.Snapshot)
        """
        return list(self.snapshots)

    def read(self):
        """Read the complete archive information. Returns a triple containing
        the archive root, the list of snapshots, and the archive schema.

        Returns
        -------
        histore.archive.node.ArchiveElement
        list(histore.archive.snapshot.Snapshot)
        histore.schema.document.DocumentSchema
        """
        return (self.root, self.snapshots, self.schema)

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
        self.validate_snapshots(root, snapshots)
        self.root = root
        self.snapshots = snapshots
        if not schema is None:
            self.schema = schema
