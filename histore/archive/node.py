# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Archives are represented as trees with two types of nodes: elements and
values. Element nodes contain a label and children. Value nodes contain only a
value. Every node in the archive has a timestamp that represents the snapshots
(versions) in which the node was present.

Element nodes are annotated with key values based on a document schema. The key
values uniquely identify entities within archives and documents. They are
essential when merging document snapshots and archives.
"""

from abc import abstractmethod
import json
import StringIO
import yaml

from histore.archive.serialize import CompactArchiveSerializer, DefaultArchiveSerializer
from histore.path import Path
from histore.timestamp import Timestamp, TimeInterval


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

    @staticmethod
    def compare(node1, node2):
        """Compare two element nodes. Returns -1 if this first node is
        considered lower than the second node, 0 if both nodes are considered
        equal, and 1 if the first is considered greater than the second node.

        Element nodes are in general considered lower than value nodes. For
        a pair of value nodes the order is defined by the order of their
        respective values. For element nodes the order is defined first by their
        label and second by their respective keys.

        Parameters
        ----------
        node1: histore.archive.node.ArchiveNode
        node2: histore.archive.node.ArchiveNode

        Returns
        -------
        int
        """
        # If nodes are not of the same type an element node is considered the
        # lower ordered one.
        if node1.is_element() and node2.is_value():
            return -1
        elif node1.is_value() and node2.is_element():
            return 1
        elif node1.is_value() and node2.is_value():
            # If both nodes are values their order is determined by the order of
            # their values.
            if node1.value < node2.value:
                return -1
            elif node1.value > node2.value:
                return 1
            else:
                return 0
        else:
            # if both nodes are elements thier order is defined by (1) the
            # order of their labels and (2) the order of their respective key
            # values.
            if node1.label < node2.label:
                return -1
            elif node1.label > node2.label:
                return 1
            else:
                # If none of the nodes has a key return 0. Else compare the key
                # values. The keys are expected to be of same length.
                if not node1.key is None and node2.key is None:
                    return -1
                elif node1.key is None and not node2.key is None:
                    return 1
                elif node1.key is None and node2.key is None:
                    return 0
                else:
                    for i in range(len(node1.key)):
                        kv1 = node1.key[i]
                        kv2 = node2.key[i]
                        if kv1 < kv2:
                            return -1
                        elif kv1 > kv2:
                            return 1
                    return 0

    def is_element(self):
        """Returns True id the node is an archive element node.

        Returns
        -------
        bool
        """
        return not self.is_value()

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
        """Shortcut to add a child node to this element.

        Parameters
        ----------
        node: histore.archive.node.ArchiveNode
        """
        self.children.append(node)

    @staticmethod
    def from_document(doc, schema, label='root', version=0, strict=False):
        """Create an archive element node from a given document with the given
        schema.

        Strict mode ensures that all nodes in the annotated document have a
        unique key (i.e., there are no child nodes of any parent in the document
        with identical labels and key values).

        Parameters
        ----------
        doc: histore.document.base.Document
        schema: histore.schema.DocumentSchema
        label: string
        version: int
        strict: bool, optional

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        return NodeAnnotator(schema).annotate(
            archive_node=ArchiveElement(
                label=label,
                timestamp= Timestamp([TimeInterval(version)])
            ),
            doc_nodes=doc.nodes,
            path=Path(''),
            strict=strict
        )

    def is_value(self):
        """Overrides abstract method. Returns False because the node is not an
        archive value node.

        Returns
        -------
        bool
        """
        return False

    def sort(self, strict=False):
        """Sort element child nodes. Implemented as insertion sort. Sorts the
        list if children in-place.

        The strict flag is used to raise ValueErrors for duplicate nodes when
        annotating a document in strict mode. If set to True this ensures that
        the list of children of an element does not contain identical nodes.

        Parameters
        ----------
        strict: bool, optiona;
        """
        nodes = self.children
        for i in range(1, len(nodes)):
            node = nodes[i]
            # Move elements of nodes[0..i-1], that are greater than the node one
            # position ahead of their current position
            j = i - 1
            while j >= 0:
                comp =  ArchiveNode.compare(node, nodes[j])
                if strict and comp == 0:
                    raise ValueError('duplicate nodes \'' + str(node) + '\'')
                elif comp >= 0:
                    break
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


class NodeAnnotator(object):
    """Annotate nodes in a document snapshot with key values based on a given
    schema.
    """
    def __init__(self, schema):
        """Initialize the schema that is used for node annotation.

        Parameters
        ----------
        schema: histore.schema.document.DocumentSchema
        """
        self.schema = schema

    def annotate(self, archive_node, doc_nodes, path, strict=False):
        """Annotate the given list of document nodes recursively and add the
        result as children of the given archive node. Returns the given
        archive_node.

        When strict mode is enabled the method reaises a ValueError if it
        encounters duplicate nodes as children of the same parent node.

        Paremeters
        ----------
        archive_node: histore.archive.node.ArchiveElement
            Archive element to which the annotated document nodes are added
            as children
        doc_nodes: list(histore.document.node.Node)
            List of document nodes that are annotated`
        path: histore.path.Path
            Target path under which the document nodes appear in the document.
            The path determines the key specification that is used to annotate
            the document nodes.
        strict: bool, optional
            Flag indicating whether checking for duplicates is enabled or not.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        t = archive_node.timestamp
        for node in doc_nodes:
            child = ArchiveElement(label=node.label, timestamp=t)
            target_path = path.extend(node.label)
            key = self.schema.get(target_path)
            if not key is None:
                child.key = key.annotate(node)
                child.positions.append(
                    ArchiveValue(timestamp=t, value=node.index)
                )
            archive_node.add(child)
            if node.is_leaf():
                child.add(
                    ArchiveValue(timestamp=t, value=node.value))
            else:
                self.annotate(child, node.children, target_path, strict=strict)
        # Make sure to sort the children of the node.
        archive_node.sort(strict=strict)
        return archive_node


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
