# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Documents are trees with two types of nodes: elements and values. Element
nodes that have a label and a list of children and leaf nodes that a
label and contain a scalar value.
"""

from histore.document.node import InternalNode


class Document(object):
    """This class represents the root of a document tree. It contains a list
    of nodes which are the direct children of the tree root.

    Attributes
    ----------
    nodes: list(histore.document.node.Node)
        List of document root nodes.
    """
    def __init__(self, doc=None, nodes=None):
        """Initialize the list of root children. The document can either be
        initialized from a dictionary or using a given list of root children.

        Raises ValueError if both doc and nodes are not None.

        Parameters
        ----------
        doc: dict, optional
            Dictionary representing a document
        nodes: list(histore.document.node.Node), optional
            List of child nodes for document root.
        """
        if not doc is None and nodes is None:
            self.nodes = InternalNode.from_dict('root', doc).children
        elif doc is None and not nodes is None:
            self.nodes = list(nodes)
        elif doc is None and nodes is None:
            self.nodes = list()
        else:
            raise ValueError('invalid arguments for doc and nodes')
