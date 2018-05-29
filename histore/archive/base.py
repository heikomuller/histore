# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

from histore.archive.node import ArchiveElement
from histore.archive.merge import NestedMerger
from histore.archive.snapshot import Snapshot
from histore.document.base import Document
from histore.path import Path


"""Archives are collections of snapshots of an evolving dataset."""


class Archive(object):
    """An archive maintains a list of snapshot handles. All snapshots are
    expected to follow the same document schema.
    """
    def __init__(self, schema, snapshots=None):
        """Initialize the document schema and the optional list of dataset
        snapshots.

        Raises ValueError if the version numbers of snapshots are not ordered
        consecutively starting at 0.

        Parameters
        ----------
        schema: histore.schema.DocumentSchema
        snapshots: list(histore.archive.snapshot.Snapshot)
        """
        self.schema = schema
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
        else:
            self.root = None

    def get(self, version):
        """
        """
        raise NotImplementedError

    def insert(self, doc, name=None):
        """Insert a new document version as snapshot into this archive.

        Parameters
        ----------
        doc: dict
        name: string, optional

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        # Create a handle for the new snapshot
        snapshot = Snapshot(len(self.snapshots), name=name)
        if self.root is None:
            # This is the first snapshot in the archive. We do not need to
            # merge anything.
            self.root = ArchiveElement.from_document(
                doc=Document(doc=doc),
                schema=self.schema,
                version=snapshot.version
            )
        else:
            NestedMerger.merge(
                archive_node=self.root,
                doc_nodes=Document(doc=doc).nodes,
                schema=self.schema,
                path=Path(''),
                version=snapshot.version
            )
        self.snapshots.append(snapshot)
        return snapshot

    def length(self):
        """Number of snapshots in the archive.

        Returns
        -------
        int
        """
        return len(self.snapshots)

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
