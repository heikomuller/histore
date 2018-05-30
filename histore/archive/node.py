# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Archives are represented as trees with two types of nodes: elements and
values. Element nodes contain a label and children. Value nodes contain only a
value. Every node in the archive has a timestamp that represents the snapshots
(versions) in which the node was present.
"""

from abc import abstractmethod
import json
import StringIO
import yaml

from histore.path import Path
from histore.timestamp import Timestamp, TimeInterval
from histore.archive.serialize import CompactArchiveSerializer, DefaultArchiveSerializer


class ArchiveNode(object):
    """Basic node in an dataset archive. Each node has a timestamp that contains
    the identifier (version numebr) for the snapshots in which the node was
    present in.

    In theory, a node inherits the timestamp from its parent if it does not
    differ from the parents timestamp. In practice, we decided to keep a copy
    of each timestamp with the node. This appears beneficial when merging
    archives and snapshots as it avoids having to make copies of complete
    subtrees when their parent node changes.
    """
    def __init__(self, timestamp):
        """Set the node's timestamp.

        Raises ValueError if the timestamp is None or empty

        Parameters
        ----------
        timestamp: histore.timestamp.Timestamp
        """
        if timestamp is None or timestamp.is_empty():
            raise ValueError('invalid timestamp')
        self.timestamp = timestamp

    @abstractmethod
    def is_value(self):
        """Returns True if the node is an archive value node.

        Returns
        -------
        bool
        """
        raise NotImplementedError


class ArchiveElement(ArchiveNode):
    """Element node in an archive. Element nodes have an optional key for nodes
    that are keyed by value. If the key is None the node is assumed to be keyed
    by existence. Nodes that are keyed by value also maintain a list of node
    positions (i.e., timestamped value nodes) that represent the position of
    the node among its siblings with the same label in the different document
    versions. All element nodes have a (potentially empty) list of children.
    """
    def __init__(self, label, timestamp, key=None, positions=None, children=None):
        """Initialize the element node.

        Raises ValueError if the node label is None. The values of arguments
        key and positions are expected to either both be None or both not be
        None.

        Parameters
        ----------
        label: string
            Node label
        timestamp: histore.timestamp.Timestamp
            Timestamp of node
        key: list(), optional
            List of key values if the node is not keyed by existence
        positions: list(histore.archive.node.ValueNode)
            History of index positions for this node among its siblings with the
            same label (for keyed nodes only)
        children: list(histore.archive.node.ArchiveNode)
            Children of this node in the archive tree
        """
        super(ArchiveElement, self).__init__(timestamp=timestamp)
        # Set the node label. Ensure that the given label is not None.
        if label is None:
            raise ValueError('invalid element label')
        self.label = label
        # Set element key. If key is None the element is keyed by existence.
        if key is None and not positions is None:
            raise ValueError('invalid arguments for key and positions')
        elif not key is None and positions is None:
            raise ValueError('invalid arguments for key and positions')
        self.key = key
        self.positions = positions if not positions is None else list()
        self.children = children if not children is None else list()

    def __repr__(self):
        """Unambiguous string representation of this archive element node.

        Returns
        -------
        string
        """
        return 'ArchiveElement(%s, key=%s, t=%s)' % (self.label, str(self.key), str(self.timestamp))

    def add(self, node):
        """Add a child node to this element.

        Raises ValueError if the node is an element node and the label is not
        unique among the siblings and the node does not have a key value.

        Parameters
        ----------
        node: histore.archive.node.ArchiveNode
        """
        if not node.is_value() and node.key is None:
            for child in self.children:
                if not child.is_value() and child.label == node.label:
                    raise ValueError('duplicate elements \'' + node.label + '\' keyed by existence')
        self.children.append(node)

    def add_nodes(self, doc_nodes, schema, path):
        """Add all nodes in the document node list as children of the this
        element.

        Ensures that children are sorted based on their key values.

        Parameters
        ----------
        archive_node: histore.archive.node.ArchiveElement
        doc_nodes: list(histore.document.node.Node)
        schema: histore.schema.DocumentSchema
        path: histore.path.Path

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        for node in doc_nodes:
            child = ArchiveElement(label=node.label, timestamp=self.timestamp)
            target_path = path.extend(node.label)
            key = schema.get(target_path)
            if not key is None:
                child.key = key.annotate(node)
                child.positions.append(
                    ArchiveValue(
                        timestamp=self.timestamp,
                        value=node.index
                    )
                )
            self.add(child)
            if node.is_leaf():
                child.add(
                    ArchiveValue(
                        timestamp=self.timestamp,
                        value=node.value
                    )
                )
            else:
                child.add_nodes(node.children, schema, target_path)
        # Make sure to sort the children of the node.
        self.sort()
        return self

    def compare_to(self, node):
        """Compare two element nodes. Returns -1 if this node is considered
        lower than the given node, 0 if both nodes are considered equal, and 1
        if this node is considered greater than the given node.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement

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

    @staticmethod
    def from_document(doc, schema, label='root', version=0):
        """Create an archive element node from a given document with the given
        schema.

        Parameters
        ----------
        doc: histore.document.base.Document
        schema: histore.schema.DocumentSchema
        label: string
        version: int

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        return ArchiveElement(
                label=label,
                timestamp= Timestamp([TimeInterval(version)])
            ).add_nodes(doc_nodes=doc.nodes, schema=schema, path=Path(''))

    def is_value(self):
        """Overrides abstract method. Returns False because the node is not an
        archive value node.

        Returns
        -------
        bool
        """
        return False

    def sort(self):
        """Sort element child nodes. Implemented as insertion sort. Sorts the
        list if children in-place
        """
        nodes = self.children
        for i in range(1, len(nodes)):
            node = nodes[i]
            # Move elements of nodes[0..i-1], that are greater than the node one
            # position ahead of their current position
            j = i - 1
            while j >= 0 and node.compare_to(nodes[j]) < 0:
                nodes[j + 1] = nodes[j]
                j -= 1
            nodes[j + 1] = node

    def to_json_string(self, compact=False, schema=None):
        """ Get" nested node structure as formated Json string. Intended
        primarily for debugging.

        Returns
        -------
        string
        """
        if compact:
            serializer = CompactArchiveSerializer(schema=schema)
        else:
            serializer = DefaultArchiveSerializer()
        return json.dumps(serializer.element_to_dict(self), indent=4, sort_keys=True)

    def to_yaml_string(self, compact=False, schema=None):
        """ Get" nested node structure as formated Yaml string. Intended
        primarily for debugging.

        Returns
        -------
        string
        """
        if compact:
            serializer = CompactArchiveSerializer(schema=schema)
        else:
            serializer = DefaultArchiveSerializer()
        stream = StringIO.StringIO()
        yaml.dump(serializer.element_to_dict(self), stream, default_flow_style=False)
        return stream.getvalue()


class ArchiveValue(ArchiveNode):
    """A value node in an archive represents a timestamped value. These nodes
    are leafs in the archive document tree.
    """
    def __init__(self, timestamp, value):
        """Initialize the node timestamp and value.

        Parameters
        ----------
        value: any
            Node value
        timestamp: histore.timestamp.Timestamp
            Timestamp of node
        """
        super(ArchiveValue, self).__init__(timestamp=timestamp)
        self.value = value

    def __repr__(self):
        """Unambiguous string representation of this archive value node.

        Returns
        -------
        string
        """
        return 'ArchiveValue(%s, t=%s)' % (str(self.value), str(self.timestamp))

    def is_value(self):
        """Overrides abstract method. Returns True because the node is an
        archive value node.

        Returns
        -------
        bool
        """
        return True


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def print_node(node, indent='\t', depth=0):
    """Print an archive node. Primarily for debugging purposes.

    Parameters
    ----------
    node: histore.archive.node.ArchiveNode
    indent: string
    """
    print (indent * depth) + str(node)
    if not node.is_value():
        for child in node.children:
            print_node(child, indent=indent, depth=depth+1)
