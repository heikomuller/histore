# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Contains the nested-merge logic for merging archives and input documents
(i.e., dataset snapshots). Assumes that archive and document  follow the same
dataset schema.
"""


class AnnotatedNode(object):
    """Simple wrapper containing an internal document node and the node key as
    defined by the document schema.
    """
    def __init__(self, node, key=None):
        """Initialize wrapped document node and node key.

        Parameters
        ----------
        node: histore.document.InternalNode
        key: list
        """
        self.node = node
        self.key = key
    def __repr__(self):
        """Unambiguous string representation of this annotated node.

        Returns
        -------
        string
        """
        return '(node=%s, key=%s)' % (str(self.node), str(self.key))


    def compare_to(self, node):
        """Compare to annotated nodes. Returns -1 if this node is considered
        lower than the given node, 0 if both nodes are considered equal, and 1
        if this node is considered greater than the given node.

        Parameters
        ----------
        node: histore.archive.merge.AnnotatedNode

        Returns
        -------
        int
        """
        if self.label < node.label:
            return -1
        elif self.label > node.label:
            return 1
        else:
            # If none of the nodes has a key return 0. Else compare the key
            # values. The keys are expected to be of same length
            if not self.key is None and node.key is None:
                return -1
            elif self.key is None and not node.key is None:
                return 1
            elif self.key is None and node.key is None:
                return 0
            else:
                for i in range(len(self.key)):
                    kv1 = self.key[i]
                    kv2 = node.key[i]
                    if kv1 < kv2:
                        return -1
                    elif kv1 > kv2:
                        return 1
                return 0

    @property
    def label(self):
        """Short-cut to get the label of the document node.

        Returns
        -------
        string
        """
        return self.node.label


class NestedMerger(object):
    """The nested merger contains the logic to merge an archive tree and a new
    dataset snapshot.
    """
    def merge(self, archive_node, doc_nodes, schema, path, version, timestamp=None):
        """Recirsively merges the children of a given archive node with the
        list of document nodes. Returns a modified copy of the archive node.

        Parameters
        ----------
        archive_node: histore.archive.node.ArchiveElement
            Archive element that mathces the parent of the given document nodes
        document_nodes: histore.document.node.Node
            List of document nodes
        schema: histore.schema.DocumentSchema
            Document schema used to annotate the nodes
        path: histore.path.Path
            Current path under which the nodes in the document appear
        version: int
            Version of the document snapshot
        timestamp: histore.timestamp.Timestamp, optional
            Timestamp of the archive node in the new archive (may differ from
            timestamp in current archive)

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        # Get the timestamp for the archive node in the new archive
        if timestamp is None:
            t = archive_node.get_timestamp().append(version)
        else:
            t = timestamp.append(version)
        # Create a sorted list of annotated document nodes
        anno_nodes = list()
        for node in doc_nodes:
            key = schema.get(path.extend(node.label))
            if not key is None:
                anno_nodes.append(AnnotatedNode(node, key=key.annotate(node)))
            else:
                anno_nodes.append(AnnotatedNode(node))
        anno_nodes = sort_nodes(anno_nodes)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def sort_nodes(nodes):
    """Sort a list of annotated nodes. Implemented as insertion sort.

    Parameters
    ----------
    nodes: list(histore.archive.merge.AnnotatedNode)

    Returns
    -------
    list(histore.archive.merge.AnnotatedNode)
    """
    for i in range(1, len(nodes)):
        node = nodes[i]
        # Move elements of nodes[0..i-1], that are greater than the node one
        # position ahead of their current position
        j = i-1
        while j >= 0 and node.compare_to(nodes[j]) < 0:
            nodes[j + 1] = nodes[j]
            j -= 1
        nodes[j + 1] = node
    return nodes
