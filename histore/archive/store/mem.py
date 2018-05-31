# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Simple in-memory store for archives."""

from histore.archive.store.base import ArchiveStore


class InMemoryArchiveStore(ArchiveStore):
    """Implements an archive store that keept the archive root node in memory
    """
    def __init__(self, root=None):
        """Initialize the archive root node. By default the root is None.

        Parameters
        ----------
        root: histore.archive.node.ArchiveElement, optional
        """
        self.root = root

    def read(self):
        """Get the root node for the archive that is maintained by this store.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        return self.root

    def write(self, node, snapshot):
        """Relace the current root node of the archive that is maintained by
        this store with the given root node.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement
        snapshot: histore.archive.snapshot.Snapshot
        """
        self.root = node
