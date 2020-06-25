# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive descriptor."""

import pytest

from jsonschema import ValidationError

from histore.archive.manager.descriptor import ArchiveDescriptor

import histore.util as util


def test_archive_descriptor():
    """Test methods for creating archive descriptors."""
    # Create descriptor using a dictionary.
    doc = {
        'id': '0000',
        'createdAt': util.utc_now().isoformat(),
        'name': 'My Archive',
        'description': 'This is my archive',
        'primaryKey': ['SSN'],
        'encoder': 'myencoder',
        'decoder': 'mydecoder'
    }
    descriptor = ArchiveDescriptor(doc)
    assert descriptor.identifier() == '0000'
    assert descriptor.name() == 'My Archive'
    assert descriptor.description() == 'This is my archive'
    assert descriptor.primary_key() == ['SSN']
    assert descriptor.encoder() == 'myencoder'
    assert descriptor.decoder() == 'mydecoder'
    doc = {'id': '0001', 'createdAt': util.utc_now().isoformat()}
    descriptor = ArchiveDescriptor(doc)
    assert descriptor.identifier() == '0001'
    assert descriptor.name() == '0001'
    assert descriptor.description() == ''
    assert descriptor.primary_key() is None
    # Create descriptor from static create method
    descriptor = ArchiveDescriptor.create(
        name='My Archive',
        description='This is my archive',
        primary_key='SSN',
        encoder='myencoder',
        decoder='mydecoder'
    )
    assert descriptor.identifier() is not None
    assert descriptor.name() == 'My Archive'
    assert descriptor.description() == 'This is my archive'
    assert descriptor.primary_key() == ['SSN']
    assert descriptor.encoder() == 'myencoder'
    assert descriptor.decoder() == 'mydecoder'
    descriptor = ArchiveDescriptor.create()
    assert descriptor.identifier() is not None
    assert descriptor.name() == descriptor.identifier()
    assert descriptor.description() == ''
    assert descriptor.primary_key() is None
    # Error cases
    doc = {'id': '0000', 'primaryKey': 'SSN'}
    descriptor = ArchiveDescriptor(doc, validate=False)
    assert descriptor.primary_key() == 'SSN'
    with pytest.raises(ValidationError):
        ArchiveDescriptor(doc)
