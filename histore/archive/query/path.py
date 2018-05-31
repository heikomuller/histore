# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Path expressions to query nodes in an archive."""


class PathQuery(object):
    """A query path is a list of node expressions."""
    def __init__(self, elements=None):
        """Initialize the list of node expressions.

        Parameters
        ----------
        elements: list(histore.archive.query.path.NodeExpression), optional
        """
        self.elements = elements if not elements is None else list()

    def add(self, label, key=None):
        """Append an node expression to this query path. Returns the query path.

        Parameters
        ----------
        label: string
        key: list(), optional

        Returns
        -------
        histore.archive.query.path.QueryPath
        """
        self.elements.append(NodeExpression(label=label, key=key))
        return self

    def eval(self, node, index, result):
        """Recursively evaluate the path query on an archive element node and
        add all matching nodes to the given result set. Returns the result set.
        The index position identifies the expression in the query against which
        the node is evaluated.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement
        index: int
        result: list(histore.archive.node.ArchiveElement)

        Returns
        -------
        list(histore.archive.node.ArchiveElement)
        """
        # Get the node expression. If the index is out of range return
        # immediately.
        if index >= self.length():
            return result
        expr = self.elements[index]
        # Find all children of node that match the expression. If end ot path is
        # not reached yet match recursively.
        for child in node.children:
            if child.is_element():
                if expr.matches(child):
                    if index == self.length() - 1:
                        result.append(child)
                    else:
                        self.eval(node=child, index=index+1, result=result)
        return result

    def find_all(self, node):
        """Find all nodes in the tree rooted at the given node that match the
        path query. Returns an empty list if no matching node is found.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement

        Returns
        -------
        node: list(histore.archive.node.ArchiveElement)
        """
        return self.eval(node=node, index=0, result=list())

    def find_one(self, node, strict=False):
        """Evaluate the query on the given archive element node. Return one
        matching node or None if no node matches the path query.

        In strict mode, a ValueError() is raised if more that one node matches
        the query.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement

        Returns
        -------
        node: histore.archive.node.ArchiveElement
        """
        nodes = self.find_all(node)
        if len(nodes) >= 1:
            if strict:
                raise ValueError('multiple matching nodes')
            return nodes[0]

    def get(self, index):
        """Get the node expression at the given index position of the
        path query.

        Parameters
        ----------
        index: int

        Returns
        -------
        histore.archive.query.path.NodeExpression
        """
        return self.elements[index]

    def length(self):
        """Number of node expressions in the path query.

        Returns
        -------
        int
        """
        return len(self.elements)


class NodeExpression(object):
    """A node expression is a (label, key)-pair. Key values are lists of values
    similar to key values for archive elements. The key value is optional.
    """
    def __init__(self, label, key=None):
        """Initialize label and key value of the node expression.

        Parameters
        ----------
        label: string
        key: list(), optional
        """
        self.label = label
        self.key = key

    def matches(self, node):
        """Returns True if a given archive element node matches the node
        expression. A node is a match if its label and key matche the expression
        label and key.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement

        Returns
        -------
        bool
        """
        if self.label == node.label:
            # If both node key and expression key are None this is considered
            # as a match.
            if self.key is None and node.key is None:
                return True
            elif not self.key is None and not node.key is None:
                # Compare the respective key values if keys are of same length.
                if len(self.key) == len(node.key):
                    for i in range(len(self.key)):
                        kv1 = self.key[i]
                        kv2 = node.key[i]
                        if kv1 != kv2:
                            return False
                    return True
        return False
