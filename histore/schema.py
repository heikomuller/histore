# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""A document schema is a list of key specifications. Each key specifies the
child nodes that are used as key for document elements that are lists."""

from histore.path import Path


"""Identifier for key specifications that define elements that are keyed by
their index or their (subtree) value.
"""
KEYED_BY_INDEX = '@index'
KEYED_BY_VALUE = '@value'


class KeySpec(object):
    """A key specification contains the target path that identifies the
    nodes that are targeted by this specification and a list of value paths
    which are the paths of the element children that are used as key values.
    An empty value_paths list (or None) indicates that the referenced nodes are
    keyed by their index position in the array.
    """
    def __init__(self, target_path, key_values=None):
        """Initialize the target path and key value paths.

        Parameters
        ----------
        target_path: histore.document.path.Path
        value_paths: list(histore.document.path.Path)
        """
        self.target_path = target_path
        if not key_values is None:
            if isinstance(key_values, basestring):
                if key_values == KEYED_BY_INDEX:
                    pass
                elif key_values == KEYED_BY_VALUE:
                    pass
                else:
                    raise ValueError()
            elif isinstance(key_values, list):
                pass
            else:
                raise ValueError()
        else:

        self.value_paths = value_paths if not value_paths is None else list()

    def annotate(self, node):
        """Annotate a given node with the key value for this specification. It
        is expected that the node mathces the target path.

        The resulting key is a list of values. If the value path in this key
        specification is empty the result will contain the index of the node.
        Otherwise, the result will contain one value for each of the value
        paths. If any of the paths do not exist, is not a leaf node, or if the
        path is not unique a ValueError will be raised.

        Parameters
        ----------
        node: histore.document.node.InternalNode

        Returns
        -------
        list
        """
        # If value pathes are empty the key is the node index
        if self.value_paths is None or len(self.value_paths) == 0:
            return [node.index]
        else:
            key = list()
            for path in self.value_paths:
                child = node.get(path)
                if child is None:
                    raise ValueError('missing key value for \'' + str(path) + '\'')
                elif not child.is_leaf():
                    raise ValueError('invalid key value for \'' + str(path) + '\'')
                key.append(child.value)
            return key

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


class DocumentSchema(object):
    """A document schema is a collection of key specifications."""
    def __init__(self, keys=None):
        """Initialize the list of key specifications in the document schema.

        Parameters
        ----------
        keys: list(histore.document.schema.KeySpec)
        """
        self.elements = dict()
        if not keys is None:
            for key in keys:
                self.add(key)

    def add(self, key, replace=False):
        """Add specification of an element key to the schema.

        Raises ValueError if a key for the target element already exists and
        replace is False.

        Parameters
        ----------
        key: histore.document,schema.KeySpec
            Key specification for document element
        replace: bool, optional
            Flag that determines whether an existing key at the given target
            position is replaced or a ValueError exception is raised.
        """
        path = key.target_path.to_key()
        if path in self.elements and not replace:
            raise ValueError('duplicate key for \'' + path + '\'')
        self.elements[path] = key

    def get(self, path):
        """Get the key specification for the given target path. Returns None
        if no key for the given target path exists.

        Parameters
        ----------
        path: histore.document.path.Path

        Returns
        -------
        histore.document.schema.KeySpec
        """
        path_key = path.to_key()
        if path_key in self.elements:
            return self.elements[path_key]

    def keys(self):
        """Get list of all key specification in the schema.

        Returns
        -------
        list(document.schema.KeySpec)
        """
        return self.elements.values()


class SimpleDocumentSchema(DocumentSchema):
    """Create a document schema for a given document. Will contain key
    specification for all elements in the document that are lists. All
    keys will have empty value paths.
    """
    def __init__(self, doc):
        """Create document schema from a given dictionary.

        Parameters
        ----------
        doc: dict
        """
        super(SimpleDocumentSchema, self).__init__()
        path = Path('')
        add_keyed_elements(self, doc, path)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------
def add_keyed_elements(schema, doc, path):
    """Add elements in the given dictionary to the schema if their values are
    lists.

    Parameters
    ----------
    schema: histore.schema.DocumentSchema
    doc: dict
    path: histore.path.Path
    """
    for key in doc:
        if isinstance(doc[key], list):
            target_path = path.extend(key)
            schema.add(KeySpec(target_path=target_path), replace=True)
            for el in doc[key]:
                if isinstance(el, list):
                    raise ValueError('nested lists are not supported')
                elif isinstance(el, dict):
                    add_keyed_elements(schema, el, target_path)
        elif isinstance(doc[key], dict):
            add_keyed_elements(schema, doc[key], path.extend(key))
