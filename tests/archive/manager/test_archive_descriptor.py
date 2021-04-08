# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive descriptor."""

import pytest

from datetime import datetime
from jsonschema import ValidationError

from histore.archive.manager.descriptor import ArchiveDescriptor
from histore.archive.serialize.base import COMPACT, DEFAULT
from histore.archive.serialize.compact import CompactSerializer
from histore.archive.serialize.default import DefaultSerializer

import histore.util as util


def SERIALIZER():
    return {
        'clspath': 'histore.archive.serialize.default.DefaultSerializer',
        'kwargs': {'name': 'nn'}
    }


def test_archive_descriptor():
    """Test methods for creating archive descriptors."""
    # Create descriptor using a dictionary.
    doc = {
        'id': '0000',
        'createdAt': util.utc_now().isoformat(),
        'name': 'My Archive',
        'description': 'This is my archive',
        'primaryKey': ['SSN'],
        'encoder': 'histore.tests.encode.TestEncoder',
        'decoder': 'histore.tests.encode.test_decoder'
    }
    descriptor = ArchiveDescriptor(doc)
    assert descriptor.identifier() == '0000'
    assert descriptor.name() == 'My Archive'
    assert descriptor.description() == 'This is my archive'
    assert descriptor.primary_key() == ['SSN']
    dt_now = descriptor.encoder()().default(datetime.now())
    assert '$dt' in dt_now
    assert descriptor.decoder()(dt_now) == dt_now['$dt']
    assert descriptor.serializer() is None
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
        encoder='histore.tests.encode.TestEncoder',
        decoder='histore.tests.encode.test_decoder',
        serializer=SERIALIZER
    )
    assert descriptor.identifier() is not None
    assert descriptor.name() == 'My Archive'
    assert descriptor.description() == 'This is my archive'
    assert descriptor.primary_key() == ['SSN']
    dt_now = descriptor.encoder()().default(datetime.now())
    assert '$dt' in dt_now
    assert descriptor.decoder()(dt_now) == dt_now['$dt']
    serializer = descriptor.serializer()
    assert serializer.name == 'nn'
    descriptor = ArchiveDescriptor.create()
    assert descriptor.identifier() is not None
    assert descriptor.name() == descriptor.identifier()
    assert descriptor.description() == ''
    assert descriptor.primary_key() is None
    assert descriptor.encoder() is None
    assert descriptor.decoder() is None
    assert descriptor.serializer() is None
    # Error cases
    doc = {'id': '0000', 'primaryKey': 'SSN'}
    descriptor = ArchiveDescriptor(doc, validate=False)
    assert descriptor.primary_key() == 'SSN'
    with pytest.raises(ValidationError):
        ArchiveDescriptor(doc)


@pytest.mark.parametrize(
    'spec,clsdef',
    [(DEFAULT, DefaultSerializer), (COMPACT, CompactSerializer)]
)
def test_archive_serializer(spec, clsdef):
    """Test creating archive serializers from serializer specifications."""
    descriptor = ArchiveDescriptor.create(
        name='My Archive',
        serializer=spec(
            timestamp='l1',
            pos='l2',
            name='l3',
            cells='l4',
            value='l5',
            key='l6',
            rowid='l7',
            colid='l8',
            version='l9',
            valid_time='l10',
            transaction_time='l11',
            description='l12',
            action='l13'
        )
    )
    serializer = descriptor.serializer()
    assert isinstance(serializer, clsdef)
    assert serializer.timestamp == 'l1'
    assert serializer.pos == 'l2'
    assert serializer.name == 'l3'
    assert serializer.cells == 'l4'
    assert serializer.value == 'l5'
    assert serializer.key == 'l6'
    assert serializer.rowid == 'l7'
    assert serializer.colid == 'l8'
    assert serializer.version == 'l9'
    assert serializer.valid_time == 'l10'
    assert serializer.transaction_time == 'l11'
    assert serializer.description == 'l12'
    assert serializer.action == 'l13'
