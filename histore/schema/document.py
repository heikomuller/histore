# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""A document schema is a list of key specifications."""

from histore.path import Path
from histore.schema.key import NodeIndexKey, KeySpec

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

    @staticmethod
    def from_dict(doc):
        """Create document schema from dictionary serialization as returned by
        the .to_dict() method.

        Parameters
        ----------
        doc: dict
            Dictionary representation of document schema.

        Returns
        -------
        histore.schema.document.DocumentSchema
        """
        return DocumentSchema(keys=[KeySpec.from_dict(k) for k in doc['keys']])

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

    def to_dict(self):
        """Get dictionary serialization for document schema.

        Returns
        -------
        dict
        """
        return {'keys': [key.to_dict() for key in self.keys()]}


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
            schema.add(NodeIndexKey(target_path=target_path), replace=True)
            for el in doc[key]:
                if isinstance(el, list):
                    raise ValueError('nested lists are not supported')
                elif isinstance(el, dict):
                    add_keyed_elements(schema, el, target_path)
        elif isinstance(doc[key], dict):
            add_keyed_elements(schema, doc[key], path.extend(key))
