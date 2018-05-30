# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Key specifications define how key values are generated for a given document
node. Generating a key for a document node is also referred to as annotation.

Key values are relative and intended to uniquely identify a node among its
sibmings with the same label.

Histore supports three different types of keys:

  (1) Index keys uniquely identify siblings based on their index position (in
      document order) among the set of siblings with the same label.
  (2) Leaf value keys are only valid for leaf values. They use the node value
      as the key (intended for lists of values that are unique).
  (3) Child value keys use the values from a list of child nodes (that in turn
      have to be leaf nodes) as the unique relative key.
"""

from abc import abstractmethod


class KeySpec(object):
    """Base class for key specifications. A key specification contains the
    target path that identifies the nodes that are targeted by this
    specification. The key specification further defines, how key values are
    generated for the nodes that match the target path (a.k.a., annotation).
    """
    def __init__(self, target_path):
        """Initialize the target path.

        Parameters
        ----------
        target_path: histore.document.path.Path
        """
        self.target_path = target_path

    @abstractmethod
    def annotate(self, node):
        """Annotate a given node with the key value as defined by this
        specification. It is expected that the node mathces the target path. The
        resulting key is a list of values.

        Parameters
        ----------
        node: histore.document.node.InternalNode

        Returns
        -------
        list
        """
        raise NotImplementedError

    def matches(self, path):
        """Shortcut to test whether the key's target path matches the given
        path.

        Parameters
        ----------
        path: histore.document.path.Path

        Returns
        -------
        bool
        """
        return self.target_path.matches(path)


class KeyByChildNodes(KeySpec):
    """Child value keys use the values from a list of child nodes (that in turn
    have to be leaf nodes) as the unique relative key.
    """
    def __init__(self, target_path, value_paths):
        """Initialize the target path and the relative paths of the child nodes
        that are used as key values.

        Raises ValueError if list of value paths is None or empty.

        Parameters
        ----------
        target_path: histore.document.path.Path
        value_paths: list(histore.document.path.Path)
        """
        super(KeyByChildNodes, self).__init__(target_path)
        if value_paths is None or len(value_paths) == 0:
            raise ValueError('list of value paths cannot be empty')
        self.value_paths = value_paths

    def annotate(self, node):
        """Annotate a given node with a key value that contains the value of
        the node.

        Raises ValueError if the node is not a leaf node.

        Parameters
        ----------
        node: histore.document.node.InternalNode

        Returns
        -------
        list
        """
        key = list()
        for path in self.value_paths:
            child = node.get(path)
            if child is None:
                raise ValueError('missing key value for \'' + str(path) + '\'')
            elif not child.is_leaf():
                raise ValueError('invalid key value for \'' + str(path) + '\'')
            key.append(child.value)
        return key


class KeyByNodeIndex(KeySpec):
    """Index keys uniquely identify siblings based on their index position (in
    document order) among the set of siblings with the same label.
    """
    def __init__(self, target_path):
        """Initialize the target path.

        Parameters
        ----------
        target_path: histore.document.path.Path
        """
        super(KeyByNodeIndex, self).__init__(target_path)

    def annotate(self, node):
        """Annotate a given node with a key value that contains the value of
        the index position of the node among its siblings.

        Parameters
        ----------
        node: histore.document.node.InternalNode

        Returns
        -------
        list
        """
        return [node.index]


class KeyByNodeValue(KeySpec):
    """Node value keys are only valid for leaf nodes. This key uses the node
    value as the key.
    """
    def __init__(self, target_path):
        """Initialize the target path.

        Parameters
        ----------
        target_path: histore.document.path.Path
        """
        super(KeyByNodeValue, self).__init__(target_path)

    def annotate(self, node):
        """Annotate a given node with a key value that contains the value of
        the node.

        Raises ValueError if the node is not a leaf node.

        Parameters
        ----------
        node: histore.document.node.InternalNode

        Returns
        -------
        list
        """
        if not node.is_leaf():
            raise ValueError('expected a leaf node instead of \'' + str(node) + '\'')
        return [node.value]
