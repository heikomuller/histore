# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for edge cases for initializing the archive schema."""

import pytest

from histore.archive.schema import ArchiveSchema


def test_schema_init():
    """Test merging document schemas into an archive schema."""
    # -- Match by name and identifier -----------------------------------------
    schema = ArchiveSchema()
    schema, columns = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        version=0
    )
    # -- Initialize from list -------------------------------------------------
    schema = ArchiveSchema(list(schema.columns.values()))
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        ArchiveSchema(list(schema.columns.values()) + list(schema.columns.values()))
    with pytest.raises(ValueError):
        ArchiveSchema(set({'Name', 'Age'}))
