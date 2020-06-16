# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the in-memory documents that are generated from Json
serializations.
"""

import jsonschema
import pytest

from histore.document.mem.json import JsonDocument
from histore.document.schema import Column


def test_json_document_with_errors():
    """Test creating instances of Json documents from serializations with
    errors.
    """
    doc = {'columns': ['Name', 1], 'data': [['Bob', 23], ['Alice', 24]]}
    JsonDocument(doc=doc, validate=False)
    with pytest.raises(jsonschema.ValidationError):
        JsonDocument(doc=doc)


def test_json_document_without_key():
    """Test creating instances of Json documents."""
    # Document without row index or key.
    doc = JsonDocument(
        doc={'columns': ['Name', 'Age'], 'data': [['Bob', 23], ['Alice', 24]]}
    )
    reader = doc.reader(schema=[Column(0, 'Name'), Column(1, 'Age')])
    keys, positions, names = list(), list(), list()
    while reader.has_next():
        row = reader.next()
        keys.append(row.key.value)
        positions.append(row.pos)
        names.append(row.values[0])
    assert keys == [0, 1]
    assert positions == [0, 1]
    assert names == ['Bob', 'Alice']


def test_json_document_with_pk():
    """Test creating an instance of the Json document with a primary key."""
    SCHEMA = [{'id': 1, 'name': 'Name'}, {'id': 0, 'name': 'Age'}]
    doc = JsonDocument(
        doc={
            'columns': SCHEMA,
            'data': [['Bob', 23], ['Alice', 24]],
            'primaryKey': ['Name']}
    )
    reader = doc.reader(schema=doc.columns)
    keys, positions, names = list(), list(), list()
    while reader.has_next():
        row = reader.next()
        keys.append(row.key.value)
        positions.append(row.pos)
        names.append(row.values[1])
    assert keys == ['Alice', 'Bob']
    assert positions == [1, 0]
    assert names == ['Alice', 'Bob']


def test_json_document_with_readindex():
    """Test creating an instance of the Json document with a row index."""
    doc = JsonDocument(
        doc={
            'columns': ['Name', 'Age'],
            'data': [['Bob', 23], ['Alice', 24]],
            'rowIndex': [1, 0]}
    )
    reader = doc.reader(schema=[Column(0, 'Name'), Column(1, 'Age')])
    keys, positions, names = list(), list(), list()
    while reader.has_next():
        row = reader.next()
        keys.append(row.key.value)
        positions.append(row.pos)
        names.append(row.values[0])
    assert keys == [0, 1]
    assert positions == [1, 0]
    assert names == ['Alice', 'Bob']
