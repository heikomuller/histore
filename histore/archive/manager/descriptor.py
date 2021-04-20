# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Base classes for maintaining metadata about the archives that are controlled
by an archive manager.
"""

import json
import jsonschema

from datetime import datetime
from typing import Callable, Dict, List, Optional, Union

from histore.archive.serialize.base import ArchiveSerializer

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
            'items': {'type': 'integer'}
        },
        'encoder': {'type': 'string'},
        'decoder': {'type': 'string'},
        'serializer': {
            'type': 'object',
            'properties': {
                'clspath': {'type': 'string'},
                'kwargs': {'type': 'object'}
            },
            'required': ['clspath']
        }
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
        primary_key: Optional[List[int]] = None, description: Optional[str] = None,
        encoder: Optional[str] = None, decoder: Optional[str] = None,
        serializer: Union[Dict, Callable] = None
    ):
        """Create a new archive descriptor object.

        Parameters
        ----------
        identifier: string, default=None
            Unique archive identifier.
        name: string, default=None
            Descriptive name that is associated with the archive.
        primary_key: list of int, default=None
            Identifier(s) for column(s) that are used to generate keys for rows
            in the archive.
        description: string, default=None
            Optional long description that is associated with the archive.
        encoder: string, default=None
            Full package path for the Json encoder class that is used by the
            persistent archive.
        decoder: string, default=None
            Full package path for the Json decoder function that is used by the
        serializer: dict or callable, default=None
            Dictionary or callable that returns a dictionary that contains the
            specification for the serializer. The serializer specification is
            a dictionary with the following elements:
            - ``clspath``: Full package target path for the serializer class
            that is instantiated.
            - ``kwargs`` : Additional arguments that are passed to the
            constructor of the created serializer instance.
            Only ``clspath`` is required.

        Returns
        -------
        histore.archive.manager.base.ArchiveDescriptor
        """
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
        if serializer is not None:
            doc['serializer'] = serializer if isinstance(serializer, dict) else serializer()
        return ArchiveDescriptor(doc)

    def created_at(self) -> datetime:
        """Get creating timestamp for the archive.

        Returns
        -------
        datetime.datetime
        """
        return util.to_datetime(self.doc.get('createdAt'))

    def decoder(self) -> Callable:
        """Get Json decoder function used by persistent archives.

        Returns
        -------
        callable
        """
        return decoder_from_string(self.doc.get('decoder'))

    def description(self) -> str:
        """Get archive description. If the value is not set in the descriptor
        an empty string is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('description', '')

    def encoder(self) -> json.JSONEncoder:
        """Get Json encoder used by persistent archives.

        Returns
        -------
        json.JSONEncoder
        """
        return encoder_from_string(self.doc.get('encoder'))

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

    def primary_key(self) -> List[int]:
        """Get list of primary key attribute identifier(s).

        Returns
        -------
        list of int
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

    def serializer(self) -> ArchiveSerializer:
        """Get an instance of the serializer that is used for the archive.

        Returns
        -------
        histore.archive.serialize.base.ArchiveSerializer
        """
        return serializer_from_dict(self.doc.get('serializer'))


# -- Helper Functions ---------------------------------------------------------

def decoder_from_string(decoder: str) -> Callable:
    """Get Json decoder function from package path string.

    Parameters
    ----------
    decoder: string, default=None
        Full package path for the Json decoder function that is used by the
        persistent archive.

    Returns
    -------
    callable
    """
    return util.import_obj(decoder) if decoder else None


def encoder_from_string(encoder: str) -> json.JSONEncoder:
    """Get Json encoder from package path string.

    Parameters
    ----------
    encoder: string
        Full package path for the Json encoder class that is used by the
        persistent archive. May be None.

    Returns
    -------
    json.JSONEncoder
    """
    return util.import_obj(encoder) if encoder else None


def serializer_from_dict(doc: Dict) -> ArchiveSerializer:
    """Get an instance of an archive serializer from a dictionary serialization.

    Parameters
    ----------
    doc: dict
        Dictionary serialization for archive serializer. May be None.

    Returns
    -------
    histore.archive.serialize.base.ArchiveSerializer
    """
    return util.import_obj(doc['clspath'])(**doc.get('kwargs', {})) if doc is not None else None
