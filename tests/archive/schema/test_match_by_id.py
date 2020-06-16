# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for merging schemas when matching columns by identifier."""

from histore.archive.schema import ArchiveSchema
from histore.document.schema import Column

import histore.archive.schema as mode


def test_complete_match_by_id():
    """Test matching complete schema versions by column identifier."""
    schema = ArchiveSchema()
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        matching=mode.MATCH_ID,
        version=0
    )
    assert len(schema.columns) == 3
    assert len(matched_columns) == 3
    for col in matched_columns:
        assert isinstance(col, Column)
    assert len(unmatched_columns) == 0
    # Rename the second column
    columns = list(matched_columns)
    columns[1] = Column(colid=columns[1].colid, name='BDate')
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=columns,
        matching=mode.MATCH_ID,
        version=1
    )
    assert len(schema.columns) == 3
    assert matched_columns == ['Name', 'BDate', 'Salary']
    for col in matched_columns:
        assert isinstance(col, Column)
    assert len(unmatched_columns) == 0
    # Remove column 'BDate' and add column 'Height'
    columns = [matched_columns[0], matched_columns[2]]
    columns.append('Height')
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=columns,
        matching=mode.MATCH_ID,
        version=2
    )
    assert len(schema.columns) == 4
    assert matched_columns == ['Name', 'Salary', 'Height']
    for col in matched_columns:
        assert isinstance(col, Column)
        assert col.colid >= 0
    assert len(unmatched_columns) == 0
    # Add column  with negative identifier
    columns = list(matched_columns)
    columns.append(Column(colid=-1, name='Weight'))
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=columns,
        matching=mode.MATCH_ID,
        version=3
    )
    assert len(schema.columns) == 5
    assert matched_columns == ['Name', 'Salary', 'Height', 'Weight']
    for col in matched_columns:
        assert isinstance(col, Column)
        assert 0 <= col.colid < 5
    assert len(unmatched_columns) == 0
    # Add column  with identifier that is out of range
    columns = [
        matched_columns[0],
        Column(colid=100, name='Gender'),
        matched_columns[2]
    ]
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=columns,
        matching=mode.MATCH_ID,
        version=4
    )
    assert len(schema.columns) == 6
    assert matched_columns == ['Name', 'Gender', 'Height']
    for col in matched_columns:
        assert isinstance(col, Column)
        assert 0 <= col.colid < 6
    assert len(unmatched_columns) == 0


def test_partial_match_by_id():
    """Test matching complete schema versions by column identifier."""
    schema = ArchiveSchema()
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        matching=mode.MATCH_ID,
        version=0,
        partial=True
    )
    assert len(schema.columns) == 3
    assert len(matched_columns) == 3
    for col in matched_columns:
        assert isinstance(col, Column)
        assert 0 <= col.colid < 3
    assert len(unmatched_columns) == 0
    # Rename the second column.
    columns = [Column(colid=matched_columns[1].colid, name='BDate')]
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=columns,
        matching=mode.MATCH_ID,
        version=1,
        partial=True,
        origin=0
    )
    assert len(schema.columns) == 3
    assert matched_columns == ['BDate']
    columns = schema.at_version(1)
    assert columns == ['Name', 'BDate', 'Salary']
    for col in columns:
        assert isinstance(col, Column)
        assert 0 <= col.colid < 3
    assert len(unmatched_columns) == 2
    # Rename first column and add one column
    columns = [Column(colid=columns[0].colid, name='Person'), 'Height']
    schema, matched_columns, unmatched_columns = schema.merge(
        columns=columns,
        matching=mode.MATCH_ID,
        version=2,
        partial=True,
        origin=1
    )
    assert len(schema.columns) == 4
    assert matched_columns == ['Person', 'Height']
    columns = schema.at_version(0)
    assert columns == ['Name', 'Age', 'Salary']
    columns = schema.at_version(1)
    assert columns == ['Name', 'BDate', 'Salary']
    columns = schema.at_version(2)
    assert columns == ['Person', 'BDate', 'Salary', 'Height']
    for col in columns:
        assert isinstance(col, Column)
        assert 0 <= col.colid < 4
    assert len(unmatched_columns) == 2
    # String represenation (for completeness).
    str(schema.columns[0])
