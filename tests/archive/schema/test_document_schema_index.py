# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the document schema index generation function."""

import pytest

from histore.archive.schema import document_schema, schema_mapping
from histore.document.schema import Column, to_schema


def test_empty_schema():
    """Test generating a column index for an empty document schema."""
    id_cols, name_cols = document_schema([])
    assert len(id_cols) == 0
    assert len(name_cols) == 0


def test_identifiable_columns():
    """Test generating a schema index for a list of identifiable columns."""
    schema = to_schema(['Name', 'Age', 'Gender'])
    id_cols, name_cols = document_schema(schema)
    assert set(id_cols.keys()) == set([c.colid for c in schema])
    columns = [id_cols[i] for i in sorted(id_cols.keys())]
    assert columns == [(0, 'Name'), (1, 'Age'), (2, 'Gender')]
    assert name_cols == dict()
    # -- Duplicate column names.
    schema = to_schema(['Name', 'Age', 'Name', 'Gender'])
    id_cols, name_cols = document_schema(schema)
    columns = [id_cols[i] for i in sorted(id_cols.keys())]
    assert columns == [(0, 'Name'), (1, 'Age'), (2, 'Name'), (3, 'Gender')]
    assert set(id_cols.keys()) == set([c.colid for c in schema])
    assert name_cols == dict()
    # -- Error for duplicate column identifier --------------------------------
    with pytest.raises(ValueError):
        document_schema(schema + schema)


def test_mixed_columns():
    """Test generating a document schema index for a set of columns that are a
    mix on identifiable and non-identifiable columns.
    """
    schema = [Column(colid=2, name='Name'), Column(colid=3, name='Age')] + ['Name', 'Height', 'Gender']
    id_cols, name_cols = document_schema(schema)
    assert len(id_cols) == 2
    assert len(name_cols) == 3
    # -- ID Columns
    assert id_cols[2] == (0, 'Name')
    assert id_cols[3] == (1, 'Age')
    # -- Name columns
    assert name_cols['Name'] == 2
    assert name_cols['Height'] == 3
    assert name_cols['Gender'] == 4


def test_named_columns():
    """Test generating a document schema index for columns that are string
    values.
    """
    schema = ['Name', 'Age', 'Gender']
    id_cols, name_cols = document_schema(schema)
    assert len(id_cols) == 0
    assert len(name_cols) == len(schema)
    for pos, name in enumerate(schema):
        assert name_cols[name] == pos
    # -- Error for duplicate column names -------------------------------------
    with pytest.raises(ValueError):
        document_schema(schema + schema)


def test_schema_mapping():
    """Test the schema mapping function."""
    # -- Empty schema
    assert len(schema_mapping([])) == 0
    # -- Schema with duplicate column names
    mapping = schema_mapping(to_schema(['Name', 'Age', 'Name', 'Gender']))
    assert len(mapping) == 3
    assert len(mapping['Name']) == 2
    assert len(mapping['Age']) == 1
    assert len(mapping['Gender']) == 1
