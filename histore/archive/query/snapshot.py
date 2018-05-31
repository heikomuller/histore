# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Queries to retrieve a document snapshot from an archive."""

from histore.document.node import InternalNode, LeafNode


class SnapshotQuery(object):
    """Implements logic to retrieve a particular snapshot from an archive. The
    snapshot query maintains a reference to the archive that is being queried.
    """
    def __init__(self, archive):
        """Initialize the reference to the archive that is being queries.

        Parameters
        ----------
        archive: histore.archive.base.Archive
        """
        self.archive = archive

    def eval(self, archive_node, document_node, version):
        """Evaluate a snapshot query. Recursively adds all archive node children
        that contain the given version in their timestamp to the document node.
        Returns the document node.

        Parameters
        ----------
        archive_node: histore.archive.node.ArchiveElement
        document_node: histore.document.node.InternalNode
        version: int

        Returns
        -------
        histore.document.node.InternalNode
        """
        for arch_child in archive_node.children:
            if arch_child.timestamp.contains(version):
                node_index = None
                for pos in arch_child.positions:
                    if pos.timestamp.contains(version):
                        node_index = pos.value
                # Test if this is an internal node or a leaf node by looking
                # at the arch_node children. If there is a ValueNode that
                # contains the version we have reached a leaf. Otherwise,
                # continue recursively.
                leaf_value = None
                for cand in arch_child.children:
                    if cand.timestamp.contains(version):
                        if cand.is_value():
                            leaf_value = cand
                        # We can break once we found one matching child. Even if
                        # it is a value, because a document node cannot have
                        # values and elements as children.
                        break
                if not leaf_value is None:
                    node = LeafNode(
                        label=arch_child.label,
                        value=leaf_value.value,
                        index=node_index
                    )
                else:
                    node = InternalNode(
                        label=arch_child.label,
                        index=node_index
                    )
                    self.eval(
                        archive_node=arch_child,
                        document_node=node,
                        version=version
                    )
                document_node.add(node)
        return document_node

    def get(self, snapshot):
        """Retrieve the document snapshot from the archive for the given
        snapshot handle.

        Parameters
        ----------
        snapshot: histore.archive.snapshot.Snapshot

        Returns
        -------
        histore.document.node.InternalNode
        """
        doc_root = InternalNode(label='root')
        return self.eval(
            archive_node=self.archive.root(),
            document_node=doc_root,
            version=snapshot.version
        )
