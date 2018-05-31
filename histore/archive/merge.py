# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Contains the nested-merge logic for merging archives and input documents
(i.e., dataset snapshots). Assumes that archive and document  follow the same
dataset schema.
"""

from histore.archive.node import ArchiveElement, ArchiveNode, ArchiveValue


class NestedMerger(object):
    """The nested merger contains the logic to merge an archive tree and a new
    dataset snapshot.
    """
    def merge(self, archive_node, doc_node, version, timestamp, positions=None):
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
        timestamp: histore.timestamp.Timestamp
            Timestamp of the parent node in the new archive
        positions: list(histore.archive.node.ValueNode)
            History of index positions for this node among its siblings with the
            same label (for keyed nodes only)

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        # Create modified copy of the archive node
        result_node = ArchiveElement(
            label=archive_node.label,
            key=archive_node.key,
            positions=positions,
            timestamp=timestamp
        )
        # Populate list of children in modified copy of the archive node by
        # recursively merging matching nodes from the archive and the document.
        arch_idx = 0
        doc_idx = 0
        arch_children = archive_node.children
        doc_children = doc_node.children
        while arch_idx < len(arch_children) and doc_idx < len(doc_children):
            arch_child = arch_children[arch_idx]
            doc_child = doc_children[doc_idx]
            comp = ArchiveNode.compare(doc_child, arch_child)
            if comp < 0:
                # The node has never been in any previous snapshot.
                result_node.add(doc_child)
                doc_idx += 1
            elif comp > 0:
                # The archive node is not present in this merged snapshot.
                # Add it to the children of the new node. The timestamp should
                # not change.
                result_node.add(arch_child)
                arch_idx += 1
            else:
                # Merge the two nodes recursively if they are element nodes. For
                # value nodes we add the merged node directly to the result
                # node's children.
                if arch_child.is_value() and doc_child.is_value():
                    # Create a new value node with the modified timestamp
                    value_node = ArchiveValue(
                        timestamp=pick_node_timestamp(
                            arch_child.timestamp.append(version),
                            timestamp
                        ),
                        value=arch_child.value
                    )
                    result_node.add(value_node)
                elif arch_child.is_element() and doc_child.is_element():
                    # If the nodes have a key value add the position of the
                    # document node to the list of positions for the archive
                    # element.
                    merged_pos = None
                    if not arch_child.key is None:
                        merged_pos = merge_positions(
                            positions=arch_child.positions,
                            pos=doc_child.positions[0],
                            version=version,
                            timestamp=timestamp
                        )
                    # Merge element nodes recursively.
                    merged_node = self.merge(
                        archive_node=arch_child,
                        doc_node=doc_child,
                        version=version,
                        timestamp=pick_node_timestamp(
                            arch_child.timestamp.append(version),
                            timestamp
                        ),
                        positions=merged_pos
                    )
                    result_node.add(merged_node)
                else:
                    # This should never happen if the compare method is
                    # implemented correctly.
                    raise RuntimeError('nodes of different type have been matched')
                arch_idx += 1
                doc_idx += 1
        # Add remaining nodes
        while arch_idx < len(arch_children):
            result_node.add(arch_children[arch_idx])
            arch_idx += 1
        while doc_idx < len(doc_children):
            result_node.add(doc_children[doc_idx])
            doc_idx += 1
        # Return new archive node
        return result_node


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def merge_positions(positions, pos, version, timestamp):
    """Insert a given node position into a list of node positions. Node
    positions are represented as archive values. Return a new list of node
    positions. It is expected that the given list of positions and the result
    are sorted by the position nodes values in ascending order.

    Parameters
    ----------
    positions: list(histore.archive.node.ArchiveValue)
    pos: histore.archive.node.ArchiveValue
    version: int
    timestamp: histore.timestamp.Timestamp

    Returns
    -------
    list(histore.archive.node.ArchiveValue)
    """
    result = list()
    was_added = False
    for node in positions:
        if node.value < pos.value:
            result.append(node)
        elif node.value > pos.value:
            if not was_added:
                result.append(pos)
                was_added = True
            result.append(node)
        else:
            result.append(
                ArchiveValue(
                    timestamp=pick_node_timestamp(
                        node.timestamp.append(version),
                        timestamp
                    ),
                    value=node.value
                )
            )
            was_added = True
    # If the new position wasn't added yet append it to the result
    if not was_added:
        result.append(pos)
    return result


def pick_node_timestamp(node_timestamp, parent_timestamp):
    """Avoid replication of identical timestamps for parent and child. Assign
    a node the parent timestamp if it is equals to the node timestamp.

    Parameters
    ----------
    node_timestamp: histore.timestamp.Timestamp
    parent_timestamp: histore.timestamp.Timestamp

    Returns
    -------
    histore.timestamp.Timestamp
    """
    if node_timestamp.is_equal(parent_timestamp):
        return parent_timestamp
    else:
        return node_timestamp
