# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""The archive store intruduces a layer of abstraction between the archive
object and the way in which archives are managed and maintained.
"""

from abc import abstractmethod


class ArchiveStore(object):
    """Abstract class that defines the interface to read and write the root
    node of an archive.
    """
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
