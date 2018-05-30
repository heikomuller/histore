# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Contains the nested-merge logic for merging archives and input documents
(i.e., dataset snapshots). Assumes that archive and document  follow the same
dataset schema.
"""

from histore.archive.node import ArchiveElement


class NestedMerger(object):
    """The nested merger contains the logic to merge an archive tree and a new
    dataset snapshot.
    """
    def merge(self, archive_node, doc_node, version, positions=None, timestamp=None):
        """Recirsively merges the children of a node in an archive and a node
        from a document snapshot. Both nodes are expected to be archive
        elements. Returns a modified copy of the archive node.

        Parameters
        ----------
        archive_node: histore.archive.node.ArchiveElement
            Archive element that mathces the parent of the given document nodes
        doc_node: histore.archive.node.ArchiveElement
            Archive element generated from a document snapshot
        version: int
            Version of the document snapshot
        positions: list(histore.archive.node.ValueNode)
            History of index positions for this node among its siblings with the
            same label (for keyed nodes only)
        timestamp: histore.timestamp.Timestamp, optional
            Timestamp of the archive node in the new archive (may differ from
            timestamp in current archive)

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        # Get the timestamp for the archive node in the new archive
        if timestamp is None:
            t = archive_node.timestamp.append(version)
        else:
            t = timestamp.append(version)
        # Create modified copy of the archive node
        result_node = ArchiveElement(
            label=archive_node.label,
            key=archive_node.key,
            positions=positions,
            timestamp=t
        )

        # Populate list of children in modified copy of the archive node by
        # recursively merging matching nodes from the archive and the document.
        children = list()
        arch_idx = 0
        doc_idx = 0
        arch_children = archive_node.children
        doc_children = doc_node.children
        while arch_idx < len(arch_children) and doc_idx < len(doc_children):
            arch_child = arch_children[arch_idx]
            doc_child = doc_children[doc_idx]
            comp = doc_child.compare_to(arch_child)
            if comp < 0:
                # The node has never been in any previous snapshot. Set the
                # local timestamp to the timestamp of the document snapshot.
                doc_idx += 1
            elif comp > 0:
                # The archive node is not present in this merged snapshot. Get
                # a copy of the node. If the node does not have a local
                # timestamp its current timestamp becomes the local timestamp.
                arch_idx += 1
            else:
                # Merge
                arch_idx += 1
                doc_idx += 1
        # Add remaining nodes
        while arch_idx < len(arch_children):
            arch_idx += 1
        while doc_idx < len(doc_children):
            doc_idx += 1
        # Return new archive node
