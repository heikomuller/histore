# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

from histore.archive.node import ArchiveElement
from histore.archive.merge import NestedMerger
from histore.archive.query.engine import PathQueryEngine, SnapshotQueryEngine
from histore.archive.snapshot import Snapshot
from histore.archive.store.mem import InMemoryArchiveStore
from histore.document.base import Document
from histore.document.serialize import DefaultDocumentSerializer
from histore.path import Path


"""Archives are collections of snapshots of an evolving dataset."""


class Archive(object):
    """An archive maintains a list of snapshot handles. All snapshots are
    expected to follow the same document schema.

    Archives are represented as trees. The root of the tree is maintained by
    an archive store. The store provides a layer of abstraction such that the
    archive object does not have to deal with the different ways in which
    archives are managed by different systems.
    """
    def __init__(self, schema=None, store=None):
        """Initialize the archive. Every archive has to have an associated
        archive store. If the archive store paratemter is given the schema
        has to be None. Providing only a schema (or none of the two arguments)
        will create an archive with a in-memory store. If no schema is provided
        an empty document schema is used.

        Raises ValueError if both arguments (schema and store) are not None.

        Parameters
        ----------
        schema: histore.schema.DocumentSchema, optional
        store: histore.archive.store.base.ArchiveStore
        """
        if not schema is None and not store is None:
            raise ValueError('invalid combination of arguments')
        # If the store is not given create an archive with an in-memory store
        if store is None:
            self.store = InMemoryArchiveStore(schema=schema)
        else:
            self.store = store

    def find_all(self, query):
        """Find all nodes in the archivethat match the given path query. Returns
        an empty list if no matching node is found.

        Parameters
        ----------
        query: histore.archive.query.path.PathQuery

        Returns
        -------
        node: list(histore.archive.node.ArchiveElement)
        """
        return PathQueryEngine(query).find_all(self.root())

    def find_one(self, query, strict=False):
        """Evaluate a given path query on this archive. Return one matching node
        or None if no node matches the path query.

        In strict mode, a ValueError() is raised if more that one node matches
        the query.

        Parameters
        ----------
        query: histore.archive.query.path.PathQuery

        Returns
        -------
        node: histore.archive.node.ArchiveElement
        """
        return PathQueryEngine(query).find_one(self.root(), strict=strict)

    def get(self, version, serializer=None):
        """Retrieve a document snapshot from the archive. The version identifies
        the snapshot that is being retireved. Use the provided document
        serializer to convert the internal document representation into the
        format that is used by the application/user.

        Raises ValueError if the provided version number does not identify a
        valid snapshot.

        Parameters
        ----------
        version: int
        serializer: histore.document.serializer.DocumentSerializer

        Returns
        -------
        any
        """
        # Raise exception if version number is unknown
        if version < 0 or version >= len(self.snapshots):
            raise ValueError('unknown version number \'' + str(version) + '\'')
        # Evaluate snapshot query for requested document snapshot
        doc_root = SnapshotQueryEngine(self.snapshot(version)).get(self)
        doc = Document(nodes=doc_root.children)
        # Use the default serializer if the application did not provide a
        # serializer
        if serializer is None:
            return DefaultDocumentSerializer().serialize(doc)
        else:
            return serializer.serialize(doc)

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

    @property
    def schema(self):
        """Shortcut to access the archive schema maintained by the archive
        store.

        Returns
        -------
        histore.schema.document.DocumentSchema
        """
        return self.store.schema

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

    @property
    def snapshots(self):
        """Shortcut to access the list of document snapshot handles maintained
        by the archive store.

        Returns
        -------
        list(histore.archive.snapshot.Snapshot)
        """
        return self.store.snapshots
