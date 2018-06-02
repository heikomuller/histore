# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Path expressions to query nodes in an archive."""

from abc import abstractmethod


class PathQuery(object):
    """A query path is a list of node expressions."""
    def __init__(self, elements=None):
        """Initialize the list of node expressions.

        Parameters
        ----------
        elements: list(histore.archive.query.path.NodeExpression), optional
        """
        self.elements = elements if not elements is None else list()

    def add(self, expression=None):
        """Append an node expression to this query path. Returns the query path.
        If no node expression is given a MatchAny expression is added to the
        query path.

        Parameters
        ----------
        expression: histore.archive.query.NodeExpression, optional
        key: list(), optional

        Returns
        -------
        histore.archive.query.path.QueryPath
        """
        if expression is None:
            self.elements.append(MatchAny())
        else:
            self.elements.append(expression)
        return self

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
    """Abstract class for expressions that are evaluated on element nodes as
    part of a path query.
    """
    @abstractmethod
    def matches(self, node):
        """Returns True if a given archive element node matches the node
        expression.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement

        Returns
        -------
        bool
        """
        raise NotImplementedError()


class KeyConstraint(NodeExpression):
    """Node expression that matches a node label and node key against given
    values. Only nodes that have an exact match with the given values are valid
    matches.
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


class LabelConstraint(NodeExpression):
    """Node expression that matches a node label against given value."""
    def __init__(self, label):
        """Initialize the node label.

        Parameters
        ----------
        label: string
        """
        self.label = label

    def matches(self, node):
        """Returns True if a given archive element node matches the node
        label.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement

        Returns
        -------
        bool
        """
        return self.label == node.label


class MatchAny(NodeExpression):
    """Node expression that matches any node.
    """
    def matches(self, node):
        """Returns True for any given node.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement

        Returns
        -------
        bool
        """
        return True
