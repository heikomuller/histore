# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Default query engine to evaluate archive queries. Different implementations
of an archive may evaluate queries differently. This module contains
implementations of default querie engines.
"""

class PathQueryEngine(object):
    """Evaluate a path query on a given archive."""
    def __init__(self, query):
        """Initialize the path query.

        Parameters
        ----------
        query: histore.archive.query.path.PathQuery
        """
        self.query = query

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
        if index >= self.query.length():
            return result
        expr = self.query.get(index)
        # Find all children of node that match the expression. If end ot path is
        # not reached yet match recursively.
        for child in node.children:
            if child.is_element():
                if expr.matches(child):
                    if index == self.query.length() - 1:
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
