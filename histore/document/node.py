# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Documents are trees with two types of nodes: elements and values. Element
nodes are internal nodes that have a label and a list of children. Value
nodes are leaf nodes that have a scalar value.
"""

from histore.path import Path


"""Identifier for the two different document node types."""

INTERNAL_NODE = 0
LEAF_NODE = 1


class Node(object):
    """ Base class for document nodes. Each node has a label. There are two
    sub-types: internal nodes and leaf nodes. This class maintains a flag that
    is used to distinguish between the two node types.

    Each node has an optional index position that is used to define the order
    of sibling nodes with the same label.
    """
    def __init__(self, label, node_type, index=None):
        """Set the node label, the type flag that is used to distinguish between
        the two node types, and the optional node index.

        Raises ValueError if an invalid node type identifier is given.

        Parameters
        ----------
        label: string
            Element node label
        node_type: int
            One of the two node type identifier values.
        index: int, optional
            Index position for node among siblings with same element
        """
        # Set the node type. Ensure that a valid type is given
        if not node_type in [INTERNAL_NODE, LEAF_NODE]:
            raise ValueError('invalid node type \'' + str(node_type) + '\'')
        self.node_type = node_type
        # Set the node label. Ensure that the given label is not None.
        if label is None:
            raise ValueError('invalid element label')
        self.label = label
        # Set index position. te default is 0.
        self.index = index if not index is None else 0

    def __repr__(self):
        """Unambiguous string representation of this path object.

        Returns
        -------
        string
        """
        return 'Node(%s, %i)' % (self.label, self.index)

    def is_leaf(self):
        """Flag indicating if this is a leaf node.

        Returns
        -------
        bool
        """
        return self.node_type == LEAF_NODE


class InternalNode(Node):
    """Internal nodes nodes are element nodes that have a (potentially empty)
    list of children. Children are either internal or leaf nodes.
    """
    def __init__(self, label, children=None, index=None):
        """Initialize the element label, index, and children.

        Parameters
        ----------
        label: string
            Element node label
        children: list, optional
            List of children for this node
        index: int, optional
            Index position for node among siblings with same element
        """
        # Set lebel, node type, and index in the super class
        super(InternalNode, self).__init__(
            label=label,
            node_type=INTERNAL_NODE,
            index=index
        )
        self.children = children if not children is None else list()

    def __repr__(self):
        """Unambiguous string representation of this path object.

        Returns
        -------
        string
        """
        return 'InternalNode(%s, index=%i)' % (self.label, self.index)

    def add(self, node, strict=True):
        """Shortcut to append a node to the list of children.

        Raises ValueError if strict is True and children with the same label as
        node exists and the node index is not unique.

        Parameters
        ----------
        node: histore.document.node.Node
            New child node

        Returns
        -------
        histore.document.node.Node
        """
        # Ensure that the node index is unique among siblings with the same
        # label.
        if strict:
            for child in self.children:
                if child.label == node.label and child.index == node.index:
                    raise ValueError('duplicate index for \'' + str(node) + '\'')
        self.children.append(node)
        return node

    @staticmethod
    def from_dict(label, doc, index=None):
        """Create an instance of an internal node from a dictionary.

        Raises ValueError if the document contains nested lists (currently not
        supported).

        Parameters
        ----------
        label: string
            Node label
        doc: dict
            Dictionary representation of an internal node and all of it's
            children.

        Returns
        -------
        histore.document.node.InternalNode
        """
        children = list()
        for key in doc:
            val = doc[key]
            if isinstance(val, list):
                for i in range(len(val)):
                    el = val[i]
                    if isinstance(el, list):
                        raise ValueError('nested lists are not supported yet')
                    elif isinstance(el, dict):
                        children.append(InternalNode.from_dict(key, el, index=i))
                    else:
                        children.append(LeafNode(key, value=el, index=i))
            elif isinstance(val, dict):
                children.append(InternalNode.from_dict(key, val))
            else:
                children.append(LeafNode(key, value=val))
        return InternalNode(label, children=children, index=index)

    def get(self, path):
        """Get the child element at the given relative path. Accepts a single
        node label or list of labels as path expression. Returns None if no
        element with given relative path exists.

        Raises ValueError if the path is not unique, i.e., one of the
        elements along the path has siblings with the same label.

        Parameters
        ----------
        path: string or histore.path.Path
            Relative path expression referencing a child node.

        Returns
        -------
        histore.document.node.Node
        """
        if isinstance(path, basestring):
            search_path = Path(path)
        else:
            search_path = path
        parent = self
        while not search_path.is_empty():
            # Find all children that match the first element in the search path
            label = search_path.first_element()
            nodes = list()
            for node in parent.children:
                if node.label == label:
                    nodes.append(node)
            # Return None if no matching child was found
            if len(nodes) == 0:
                return None
            # Raise an exception if the label is not unique among the children
            if len(nodes) > 1:
                raise ValueError('not a unique path \'' + str(path) + '\'')
            # We found exactly one match. Return if the end of the search path
            # is reached. Otherwise, continue with next element.
            search_path = search_path.subpath()
            if search_path.is_empty():
                return nodes[0]
            else:
                parent = nodes[0]
                # If the found node is not an internal node return None
                if parent.is_leaf():
                    return None

class LeafNode(Node):
    """Leaf nodes in the document tree contain a node value."""
    def __init__(self, label, value=None, index=None):
        """Initialize the node value.

        Parameters
        ----------
        label: string
            Element node label
        value: string, int, or float, optional
            Optional node value
        index: int, optional
            Index position for node among siblings with same element
        """
        # Set lebel, node type, and index in the super class
        super(LeafNode, self).__init__(
            label=label,
            node_type=LEAF_NODE,
            index=index
        )
        self.value = value

    def __repr__(self):
        """Unambiguous string representation of this path object.

        Returns
        -------
        string
        """
        return 'LeafNode(%s, %s, index=%i)' % (self.label, str(self.value), self.index)
