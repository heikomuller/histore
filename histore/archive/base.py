# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

from histore.archive.node import ArchiveElement, print_node
from histore.archive.merge import NestedMerger
from histore.archive.query.snapshot import SnapshotQuery
from histore.archive.snapshot import Snapshot
from histore.archive.store.mem import InMemoryArchiveStore
from histore.document.base import Document
from histore.path import Path
from histore.schema.document import DocumentSchema


"""Archives are collections of snapshots of an evolving dataset."""


class Archive(object):
    """An archive maintains a list of snapshot handles. All snapshots are
    expected to follow the same document schema.

    Archives are represented as trees. The root of the tree is maintained by
    an archive store. The store provides a layer of abstraction such that the
    archive object does not have to deal with the different ways in which
    archives are managed by different systems.
    """
    def __init__(self, schema=None, snapshots=None, store=None):
        """Initialize the document schema and the optional list of dataset
        snapshots. If no schema is provided an empty document schema is used.

        Raises ValueError if the version numbers of snapshots are not ordered
        consecutively starting at 0.

        Parameters
        ----------
        schema: histore.schema.DocumentSchema, optional
        snapshots: list(histore.archive.snapshot.Snapshot), optional
        store: histore.archive.store.base.ArchiveStore
        """
        self.schema = schema if not schema is None else DocumentSchema()
        # Maintain list of snapshots. Expects that snapshot handles are ordered
        # by the unique version number. Snapshots are numbered consecutively
        # starting at 0.
        self.snapshots = list()
        # If snapshot list is given ensure that the version numbers of all
        # snapshots are unique
        if not snapshots is None:
            for s in snapshots:
                if s.version != len(self.snapshots):
                    raise ValueError('invalid snapshot version \'' + str(s.version) + '\'')
                self.snapshots.append(s)
        self.store = store if not store is None else InMemoryArchiveStore()

    def get(self, version):
        """Retrieve document snapshot from the archive. The version identifies
        the snapshot that is being retireved.

        Raises ValueError if the provided version number does not identify a
        valid snapshot.

        Parameters
        ----------
        version: int

        Returns
        -------
        dict
        """
        # Raise exception if version number is unknown
        if version < 0 or version >= len(self.snapshots):
            raise ValueError('unknown version number \'' + str(version) + '\'')
        # Evaluate snapshot query for requested document snapshot
        return SnapshotQuery(self).get(self.snapshot(version))

    def insert(self, doc, name=None, strict=False):
        """Insert a new document version as snapshot into this archive.

        Strict mode ensures that all nodes in the given document have a unique
        key.

        Parameters
        ----------
        doc: dict
        name: string, optional
        strict: bool, optional

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        # Create a handle for the new snapshot
        snapshot = Snapshot(len(self.snapshots), name=name)
        # Create an archive from the given document with a single root node
        doc_root = ArchiveElement.from_document(
            doc=Document(doc=doc),
            schema=self.schema,
            version=snapshot.version,
            strict=strict
        )
        # Get the current root node
        root = self.store.read()
        if root is None:
            # This is the first snapshot in the archive. We do not need to
            # merge anything.
            self.store.write(doc_root, snapshot)
        else:
            root = NestedMerger().merge(
                archive_node=root,
                doc_node=doc_root,
                version=snapshot.version,
                timestamp=root.timestamp.append(snapshot.version)
            )
            self.store.write(root, snapshot)
        self.snapshots.append(snapshot)
        return snapshot

    def length(self):
        """Number of snapshots in the archive.

        Returns
        -------
        int
        """
        return len(self.snapshots)

    def root(self):
        """Get the root of the archive from the associated store.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        return self.store.read()

    def snapshot(self, version):
        """Get handle for snapshot with given version number.

        Parameters
        ----------
        version: int
            Unique snapshot version identifier

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        return self.snapshots[version]


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def print_archive(archive, indent='\t'):
    """Print nested archive node tree. Primarily for debugging purposes.

    Parameters
    ----------
    archive: histore.archive.base.Archive
    indent: string
    """
    print str(archive.root())
    for node in archive.root().children:
        print_node(node, indent=indent, depth=1)
