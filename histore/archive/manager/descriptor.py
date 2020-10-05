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

from datetime import datetime
from typing import Dict, List, Optional, Union

import histore.util as util


"""Json schema for archive descriptors."""

DESCRIPTOR_SCHEMA = {
    'type': 'object',
    'properties': {
        'id': {'type': 'string'},
        'createdAt': {'type': 'string'},
        'name': {'type': 'string'},
        'description': {'type': 'string'},
        'primaryKey': {
            'type': 'array',
            'items': {'type': 'string'}
        },
        'encoder': {'type': 'string'},
        'decoder': {'type': 'string'}
    },
    'required': ['id', 'createdAt']
}


class ArchiveDescriptor(object):
    """Wrapper around an archive descriptor dictionary object. This class
    provides access to descriptor property values and their defaults.
    """
    def __init__(self, doc: Dict, validate: Optional[bool] = True):
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
    def create(
        identifier: Optional[str] = None, name: Optional[str] = None,
        description: Optional[str] = None,
        primary_key: Optional[Union[List[str], str]] = None,
        encoder: Optional[str] = None, decoder: Optional[str] = None
    ):
        """Create a new archive descriptor object.

        Parameters
        ----------
        identifier: string, default=None
            Unique archive identifier.
        name: string, default=None
            Descriptive name that is associated with the archive.
        description: string, default=None
            Optional long description that is associated with the archive.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for rows in the
            archive.
        encoder: string, default=None
            Full package path for the Json encoder class that is used by the
            persistent archive.
        decoder: string, default=None
            Full package path for the Json decoder function that is used by the

        Returns
        -------
        histore.archive.manager.base.ArchiveDescriptor
        """
        # Ensure that the primary key is a list
        if primary_key is not None and not isinstance(primary_key, list):
            primary_key = [primary_key]
        # Create a unique identifier for the new archive.
        if identifier is None:
            identifier = util.get_unique_identifier()
        # Create the archive descriptor.
        doc = {'id': identifier, 'createdAt': util.current_time()}
        if name is not None:
            doc['name'] = name
        if description is not None:
            doc['description'] = description
        if primary_key is not None:
            doc['primaryKey'] = primary_key
        if encoder is not None:
            doc['encoder'] = encoder
        if decoder is not None:
            doc['decoder'] = decoder
        return ArchiveDescriptor(doc)

    def created_at(self) -> datetime:
        """Get creating timestamp for the archive.

        Returns
        -------
        datetime.datetime
        """
        return util.to_datetime(self.doc.get('createdAt'))

    def decoder(self) -> str:
        """Get package path for Json decoder used by persistent archives.

        Returns
        -------
        string
        """
        return self.doc.get('decoder')

    def description(self) -> str:
        """Get archive description. If the value is not set in the descriptor
        an empty string is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('description', '')

    def encoder(self) -> str:
        """Get package path for Json encoder used by persistent archives.

        Returns
        -------
        string
        """
        return self.doc.get('encoder')

    def identifier(self) -> str:
        """Get the unique archive identifier value.

        Returns
        -------
        string
        """
        return self.doc['id']

    def name(self) -> str:
        """Get the archive name. If the value is not set in the descriptor the
        identifier is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('name', self.identifier())

    def primary_key(self) -> List[str]:
        """Get list of primary key attributes.

        Returns
        -------
        list(string)
        """
        return self.doc.get('primaryKey')

    def rename(self, name: str):
        """Update the name of the archive.

        Parameters
        ----------
        name: string
            New archive name.
        """
        self.doc['name'] = name
