# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""A document schema is a list of key specifications. Each key specifies the
child nodes that are used as key for document elements that are lists."""

class KeySpec(object):
    """A key specification contains the target path that identifies the
    nodes that are affected by the specification and a list of value paths
    which are the paths of the element children that are used as key values.
    An empty value_paths list indicates that the referenced nodes are
    keyed by their index position in the array.
    """
    def __init__(self, target_path, value_paths=None):
        """Initialize the target path and key value paths.

        Parameters
        ----------
        target_path: histore.document.path.Path
        value_paths: list(histore.document.path.Path)
        """
        self.target_path = target_path
        self.value_paths = value_paths if not value_paths is None else list()

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
        self.keys = dict()
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
        if path in self.keys and not replace:
            raise ValueError('duplicate key for \'' + path + '\'')
        self.keys[path] = key

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
        if path_key in self.keys:
            return self.keys[path_key]
            
