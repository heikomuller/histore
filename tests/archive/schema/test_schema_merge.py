# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for merging schemas when matching columns by identifier and
column name.
"""

import pytest

from histore.archive.schema import ArchiveSchema
from histore.document.schema import Column, to_schema


def test_identifical_merge():
    """Test merging the same document schema multiple times."""
    schema = ArchiveSchema()
    schema, columns = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        version=0
    )
    assert columns == [0, 1, 2]
    schema, columns = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        version=1,
        origin=0
    )
    assert columns == [0, 1, 2]
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    assert schema.at_version(1) == ['Name', 'Age', 'Salary']


def test_schema_merge():
    """Test merging document schemas into an archive schema."""
    # -- Match by name and identifier -----------------------------------------
    schema = ArchiveSchema()
    schema, columns = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        version=0
    )
    assert columns == [0, 1, 2]
    schema, columns = schema.merge(
        columns=['Name', Column(colid=1, name='BDate'), 'Gender', 'Salary'],
        version=1,
        origin=0
    )
    assert columns == [0, 1, 3, 2]
    schema, columns = schema.merge(
        columns=['BDate', 'Name', 'Gender', 'Address'],
        version=2,
        origin=1
    )
    assert columns == [1, 0, 3, 4]
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    assert schema.at_version(1) == ['Name', 'BDate', 'Gender', 'Salary']
    assert schema.at_version(2) == ['BDate', 'Name', 'Gender', 'Address']
    # -- Match by identifier --------------------------------------------------
    doc_schema = to_schema(['Name', 'Age', 'Salary'])
    schema = ArchiveSchema()
    schema, columns = schema.merge(
        columns=doc_schema,
        version=0
    )
    assert columns == [c.colid for c in doc_schema]
    doc_schema = doc_schema[::-1]
    schema, columns = schema.merge(
        columns=doc_schema,
        version=1,
        origin=0
    )
    assert columns == [c.colid for c in doc_schema]
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    assert schema.at_version(1) == ['Salary', 'Age', 'Name']


def test_schema_merge_error_cases():
    """Test error cases when merging document schema."""
    # -- Cannot match to unique column ----------------------------------------
    schema = ArchiveSchema()
    schema, _ = schema.merge(
        columns=to_schema(['Name', 'Age', 'Name', 'Salary']),
        version=0
    )
    with pytest.raises(ValueError):
        schema.merge(
            columns=['Name', 'Age', 'Salary'],
            version=1,
            origin=0
        )
    # -- Multiple matches -----------------------------------------------------
    schema = ArchiveSchema()
    schema, _ = schema.merge(
        columns=to_schema(['Name', 'Age', 'Salary']),
        version=0
    )
    with pytest.raises(ValueError):
        schema.merge(
            columns=[Column(colid=0, name='Person'), 'Age', 'Name', 'Salary'],
            version=1,
            origin=0
        )


def test_schema_merge_with_rename():
    """Test mergin document schema with renamed columns."""
    schema = ArchiveSchema()
    schema, columns = schema.merge(
        columns=to_schema(['Name', 'Age', 'Salary']),
        version=0
    )
    assert columns == [0, 1, 2]
    schema, columns = schema.merge(
        columns=['Name', 'BDate', 'Gender', 'Salary'],
        version=1,
        origin=0,
        renamed=[('Age', 'BDate')]
    )
    assert columns == [0, 1, 3, 2]
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    assert schema.at_version(1) == ['Name', 'BDate', 'Gender', 'Salary']
