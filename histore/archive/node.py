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
    present in. The node inherits the timestamp from its parent if it does not
    differ from the parents timestamp.

    The class contains getter and setter methods for timestamps to reflect that
    the timestamp of a node may be inherited from the parent.
    """
    def __init__(self, parent=None, timestamp=None):
        """Set the node's parent and timestamp. Only the root node does not have
        a parent (but it has to have a timestamp).

        Parameters
        ----------
        parent: histore.archive.ArchiveNode, optional
            Parent node
        timestamp: histore.timestamp.Timestamp
            Timestamp of node if it differs from the timestamp of the parent
        """
        if parent is None and timestamp is None:
            raise ValueError('invalid arguments')
        self.parent = parent
        self.timestamp = timestamp

    def get_timestamp(self):
        """Get the nodes timestamp. If the local timestamp is not set the
        parents timestamp is returned.

        Returns
        -------
        histore.timestamp.Timestamp
        """
        if not self.timestamp is None:
            return self.timestamp
        else:
            return self.parent.get_timestamp()

    def has_timestamp(self):
        """Returns True if the node has a local timestamp, i.e., has a timestamp
        that differs from the parents timestamp.

        Returns
        -------
        bool
        """
        return not self.timestamp is None

    @abstractmethod
    def is_value(self):
        """Returns True if the node is an archive value node.

        Returns
        -------
        bool
        """
        raise NotImplementedError

    def set_timestamp(self, timestamp, validate=True):
        """Set the local timestamp. The validate flag is primarily for debugging
        purposes to ensure that timestamps of tree nodes are true subsets of
        their parents timestamp.

        Parameters
        ----------
        timestamp: histore.timestamp.Timestamp
        validate: bool, optional

        Returns
        -------
        histore.timestamp.Timestamp
        """
        if validate:
            if not timestamp.is_subset_of(parent.get_timestamp()):
                raise ValueError()
        self.timestamp = timestamp
        return timestamp


class ArchiveElement(ArchiveNode):
    """Element node in an archive. Element nodes have an optional key for nodes
    that are keyed by value. If the key is None the node is assumed to be keyed
    by existence. Nodes that are keyed by value also maintain a list of node
    positions (i.e., timestamped value nodes) that represent the position of
    the node among its siblings with the same label in the different document
    versions. All element nodes have a (potentially empty) list of children.
    """
    def __init__(self, label, key=None, positions=None, children=None, parent=None, timestamp=None):
        """Initialize the element node.

        Raises ValueError if the node label is None. The values of arguments
        key and positions are expected to either both be None or both not be
        None.
        """
        super(ArchiveElement, self).__init__(parent=parent, timestamp=timestamp)
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
        return 'ArchiveElement(%s, key=%s)' % (self.label, str(self.key))

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
        return add_document_nodes(
            ArchiveElement(
                label=label,
                timestamp= Timestamp([TimeInterval(version)])
            ),
            doc_nodes=doc.nodes,
            schema=schema,
            path=Path('')
        )

    def is_value(self):
        """Overrides abstract method. Returns False because the node is not an
        archive value node.

        Returns
        -------
        bool
        """
        return False

    def merge(self, doc, schema, version):
        """Implements nested-merge. Merge node in the document with the
        respective child node in this element.

        Parameters
        ----------
        doc: histore.document.base.Document
        schema: histore.schema.DocumentSchema
        version: int

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        return add_document_nodes(
            ArchiveElement(
                label=label,
                timestamp= Timestamp([TimeInterval(version)])
            ),
            doc_nodes=doc.nodes,
            schema=schema,
            path=Path('')
        )

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
    def __init__(self, parent, value, timestamp=None):
        """Initialize the node. Value nodes have to have a parent node.

        Parameters
        ----------
        parent: histore.archive.ArchiveNode
            Parent node
        value: any
            Node value
        timestamp: histore.timestamp.Timestamp
            Timestamp of node
        """
        super(ArchiveValue, self).__init__(parent=parent, timestamp=timestamp)
        if parent is None:
            raise ValueError('invalid parent node')
        self.value = value

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
def add_document_nodes(archive_node, doc_nodes, schema, path):
    """Add all nodes in the document node list as children of the given archive
    element.

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
        child = ArchiveElement(label=node.label, parent=archive_node)
        target_path = path.extend(node.label)
        key = schema.get(target_path)
        if not key is None:
            child.key = key.annotate(node)
            child.positions.append(ArchiveValue(parent=child, value=node.index))
        archive_node.add(child)
        if node.is_leaf():
            child.add(ArchiveValue(parent=child, value=node.value))
        else:
            add_document_nodes(child, node.children, schema, target_path)
    return archive_node
