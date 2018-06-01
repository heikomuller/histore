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

from histore.path import Path


"""Unique identifier for the different key types."""
INDEX_KEY = 'INDEX'
PATHVALUES_KEY = 'PATHVALUES'
VALUE_KEY = 'VALUE'


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

    @staticmethod
    def from_dict(doc):
        """Create key specification instance from dictionary serialization.
        Expects that every key specification to_dict() method add's a 'type'
        element to the result based on which the key type is identified.
        Calls the .from_dict() method of the respective class.

        Parameters
        ----------
        doc: dict
            Dictionary representation of key specification as returned by the
            .to_dict() methods of the subclasses that implement the KeySpec
            class.

        Returns
        -------
        histore.schema.key.KeySpec
        """
        if not 'type' in doc:
            raise ValueError('missing type information for key specification')
        if doc['type'] == INDEX_KEY:
            return NodeIndexKey.from_dict(doc)
        elif doc['type'] == PATHVALUES_KEY:
            return PathValuesKey.from_dict(doc)
        elif doc['type'] == VALUE_KEY:
            return NodeValueKey.from_dict(doc)
        else:
            raise ValueError('unknown key specification type \'' + str(doc['type']) + '\'')

    @abstractmethod
    def is_keyed_by_path_values(self):
        """Return True if the key specification is a PATH VALUE specification.

        Returns
        -------
        bool
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

    @abstractmethod
    def to_dict(self):
        """Abstract method to get dictionary serialization for a key
        specification. Implementing subclasses need to ensure that a 'type'
        element is added to the returned dictionary. The value of the type is
        used to identify the type (and class) of the key specification when
        creating a key specification instance from a dictionary.

        Returns
        -------
        dict
        """
        raise NotImplementedError


class NodeIndexKey(KeySpec):
    """Index keys uniquely identify siblings based on their index position (in
    document order) among the set of siblings with the same label.
    """
    def __init__(self, target_path):
        """Initialize the target path.

        Parameters
        ----------
        target_path: histore.document.path.Path
        """
        super(NodeIndexKey, self).__init__(target_path)

    def annotate(self, node):
        """Annotate a given node with a key value that contains the value of
        the index position of the node among its siblings.

        Raises ValueError if the index of the given node is None.

        Parameters
        ----------
        node: histore.document.node.InternalNode

        Returns
        -------
        list
        """
        if node.index is None:
            raise ValueError('missing index value for \'' + str(node) + '\'')
        return [node.index]

    @staticmethod
    def from_dict(doc):
        """Create node index key specification instance from dictionary
        serialization.

        Parameters
        ----------
        doc: dict
            Dictionary representation of key specification as returned by the
            .to_dict() method.

        Returns
        -------
        histore.schema.key.NodeIndexKey
        """
        return NodeIndexKey(Path(path=doc['path']))

    def is_keyed_by_path_values(self):
        """Return False because the key specification is not a PATH VALUE
        specification.

        Returns
        -------
        bool
        """
        return False

    def to_dict(self):
        """Get dictionary serialization of the key specification.

        Returns
        -------
        dict
        """
        return {
            'type': INDEX_KEY,
            'path': self.target_path.elements
        }


class NodeValueKey(KeySpec):
    """Node value keys are only valid for leaf nodes. This key uses the node
    value as the key.
    """
    def __init__(self, target_path):
        """Initialize the target path.

        Parameters
        ----------
        target_path: histore.document.path.Path
        """
        super(NodeValueKey, self).__init__(target_path)

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

    @staticmethod
    def from_dict(doc):
        """Create node value key specification instance from dictionary
        serialization.

        Parameters
        ----------
        doc: dict
            Dictionary representation of key specification as returned by the
            .to_dict() method.

        Returns
        -------
        histore.schema.key.NodeValueKey
        """
        return NodeValueKey(Path(path=doc['path']))

    def is_keyed_by_path_values(self):
        """Return False because the key specification is not a PATH VALUE
        specification.

        Returns
        -------
        bool
        """
        return False

    def to_dict(self):
        """Get dictionary serialization of the key specification.

        Returns
        -------
        dict
        """
        return {
            'type': VALUE_KEY,
            'path': self.target_path.elements
        }


class PathValuesKey(KeySpec):
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
        super(PathValuesKey, self).__init__(target_path)
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

    @staticmethod
    def from_dict(doc):
        """Create path values key specification instance from dictionary
        serialization.

        Parameters
        ----------
        doc: dict
            Dictionary representation of key specification as returned by the
            .to_dict() method.

        Returns
        -------
        histore.schema.key.PathValuesKey
        """
        value_paths = [Path(path=p) for p in doc['values']]
        return PathValuesKey(Path(path=doc['path']), value_paths=value_paths)

    def is_keyed_by_path_values(self):
        """Return True because the key specification is the PATH VALUE
        specification.

        Returns
        -------
        bool
        """
        return True

    def to_dict(self):
        """Get dictionary serialization of the key specification.

        Returns
        -------
        dict
        """
        return {
            'type': PATHVALUES_KEY,
            'path': self.target_path.elements,
            'values': [path.elements for path in self.value_paths]
        }
