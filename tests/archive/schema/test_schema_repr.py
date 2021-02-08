# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for printing schema objects."""

from histore.archive.schema import ArchiveSchema


def test_schema_to_string_repr():
    """Test string representations for archive schemas."""
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=0)
    schema_str = str(schema)
    assert schema_str.startswith('<ArchiveColumn (')
    assert 'name=(Name [0])' in schema_str
    assert 'name=(Age [0])' in schema_str
    assert 'name=(Salary [0])' in schema_str
