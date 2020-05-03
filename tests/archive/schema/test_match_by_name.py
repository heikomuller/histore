# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for merging schemas when matching columns by name."""

import pytest

from histore.archive.schema import ArchiveSchema

import histore.archive.schema as mode


def test_complete_match_by_name():
    """Test merging snapshot schemas into an archive schema while matching
    columns by name instead of column identifier.
    """
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=0)
    schema, matched_schema, _ = schema.merge(
        columns=['Salary', 'Age', 'Name'],
        version=1,
        matching=mode.MATCH_NAME,
        origin=0
    )
    assert matched_schema[0].colid == 2
    assert matched_schema[1].colid == 1
    assert matched_schema[2].colid == 0
    schema, matched_schema, _ = schema.merge(
        columns=['Person', 'Salary', 'Age'],
        version=2,
        matching=mode.MATCH_NAME,
        origin=1
    )
    assert matched_schema[0].colid == 3
    assert matched_schema[1].colid == 2
    assert matched_schema[2].colid == 1
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    assert schema.at_version(1) == ['Salary', 'Age', 'Name']
    assert schema.at_version(2) == ['Person', 'Salary', 'Age']
    assert len(schema.columns) == 4
    # Test with column renaming
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=0)
    schema, matched_schema, _ = schema.merge(
        columns=['Salary', 'Age', 'Name'],
        version=1,
        matching=mode.MATCH_NAME,
        origin=0
    )
    schema, matched_schema, _ = schema.merge(
        columns=['Person', 'Salary', 'Age'],
        version=2,
        matching=mode.MATCH_NAME,
        origin=1,
        renamed={'Name': 'Person'}
    )
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    assert schema.at_version(1) == ['Salary', 'Age', 'Name']
    assert schema.at_version(2) == ['Person', 'Salary', 'Age']
    assert len(schema.columns) == 3
    # Error cases
    with pytest.raises(ValueError):
        schema.merge(
            columns=['Person', 'Salary', 'Person'],
            version=3,
            matching=mode.MATCH_NAME,
            origin=2
        )


def test_incomplete_match_by_name():
    """Test merging incomplete snapshot schemas when matching columns by name.
    """
    schema = ArchiveSchema()
    # Merge first schema.
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=0)
    cname, cage, csalary = schema.at_version(0)
    schema, _, _ = schema.merge(
        columns=[cname, 'Height'],
        version=1,
        matching=mode.MATCH_NAME,
        partial=True,
        origin=0
    )
    columns = schema.at_version(1)
    assert columns == ['Name', 'Age', 'Salary', 'Height']
    assert [c.colid for c in columns] == [0, 1, 2, 3]
    # Merge renamed column.
    schema, _, _ = schema.merge(
        columns=['BDate'],
        version=2,
        matching=mode.MATCH_NAME,
        partial=True,
        origin=1,
        renamed={'Age': 'BDate'}
    )
    columns = schema.at_version(2)
    assert columns == ['Name', 'BDate', 'Salary', 'Height']
    assert [c.colid for c in columns] == [0, 1, 2, 3]
