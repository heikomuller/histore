# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Base classes for maintaining metadata about the archives that are controlled
by an archive manager.
"""

import jsonschema

import histore.util as util


"""Json schema for archive descriptors."""

DESCRIPTOR_SCHEMA = {
    'type': 'object',
    'properties': {
        'id': {'type': 'string'},
        'name': {'type': 'string'},
        'description': {'type': 'string'},
        'primaryKey': {
            'type': 'array',
            'items': {'type': 'string'}
        }
    },
    'required': ['id']
}


class ArchiveDescriptor(object):
    """Wrapper around an archive descriptor dictionary object. This class
    provides access to descriptor property values and their defaults.
    """
    def __init__(self, doc, validate=True):
        """Initialize the dictionary containing the archive descriptor.
        Validates the document against the descriptor schema if the validate
        flag is True. Raises a ValidationError if validation fails.

        Parameters
        ----------
        doc: dict
            Dictionary containing an archive descriptor. The dictionary should
            adhere to the defined descriptor schema.
        validate: bool default=True
            Validate the given dictionary against the descriptor schema if this
            flag is True. Raises a ValidationError if the validation fails.

        Raises
        ------
        jsonschema.ValidationError
        """
        self.doc = doc
        if validate:
            jsonschema.validate(instance=doc, schema=DESCRIPTOR_SCHEMA)

    @staticmethod
    def create(name=None, description=None, primary_key=None):
        """Create a new archive descriptor object.

        Parameters
        ----------
        name: string, default=None
            Descriptive name that is associated with the archive.
        description: string, default=None
            Optional long description that is associated with the archive.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for rows in the
            archive.

        Returns
        -------
        histore.archive.manager.base.ArchiveDescriptor
        """
        # Ensure that the primary key is a list
        if primary_key is not None and not isinstance(primary_key, list):
            primary_key = [primary_key]
        # Create a unique identifier for the new archive.
        identifier = util.get_unique_identifier()
        # Create the archive descriptor.
        doc = {'id': identifier}
        if name is not None:
            doc['name'] = name
        if description is not None:
            doc['description'] = description
        if primary_key is not None:
            doc['primaryKey'] = primary_key
        return ArchiveDescriptor(doc)

    def description(self):
        """Get archive description. If the value is not set in the descriptor
        an empty string is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('description', '')

    def identifier(self):
        """Get the unique archive identifier value.

        Returns
        -------
        string
        """
        return self.doc['id']

    def name(self):
        """Get the archive name. If the value is not set in the descriptor the
        identifier is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('name', self.identifier())

    def primary_key(self):
        """Get list of primary key attributes.

        Returns
        -------
        list(string)
        """
        return self.doc.get('primaryKey')
