# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for archive schemas."""

import pytest

from histore.archive.schema import ArchiveSchema, match_columns
from histore.document.schema import Column


def test_column_provenance():
    """Test provenance information for column updates."""
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        match_by_name=True,
        version=0
    )
    schema, _, _ = schema.merge(
        columns=['Age', 'Name', 'Salary'],
        match_by_name=True,
        version=1,
        origin=0
    )
    schema, _, _ = schema.merge(
        columns=['Name', 'Height'],
        renamed={'Age': 'Height'},
        match_by_name=True,
        version=2,
        origin=1
    )
    prov = schema.columns[0].diff(0, 1)
    assert prov.updated_name() is None
    assert prov.updated_position() is not None
    prov = schema.columns[1].diff(1, 2)
    assert prov.updated_name().values() == ('Age', 'Height')
    assert prov.updated_position().values() == (0, 1)
    

def test_match_columns_by_name():
    """Test match columns by name function."""
    matches = match_columns(
        columns=['A', 'B', 'C'],
        schema=[Column(colid=0, name='C'), Column(colid=1, name='B')]
    )
    assert matches == [('A', None), ('B', 'B'), ('C', 'C')]
    matches = match_columns(
        columns=['A', 'B', 'C'],
        schema=[
            Column(colid=0, name='C'),
            Column(colid=1, name='B'),
            Column(colid=2, name='D')
        ],
        renamed={'D': 'A'}
    )
    assert matches == [('A', 'D'), ('B', 'B'), ('C', 'C')]
    matches = match_columns(
        columns=['A', 'B', 'C'],
        schema=[
            Column(colid=0, name='C'),
            Column(colid=1, name='B'),
            Column(colid=2, name='D')
        ],
        renamed={'D': 'A'},
        renamed_to=False
    )
    assert matches == [('A', None), ('B', 'B'), ('C', 'C')]
    # Error cases for lists with duplictes.
    with pytest.raises(ValueError):
        match_columns(
            columns=['C', 'B', 'C'],
            schema=[Column(colid=0, name='C'), Column(colid=1, name='B')]
        )
    with pytest.raises(ValueError):
        match_columns(columns=['A', 'B', 'C'], schema=['C', 'B', 'B'])
    with pytest.raises(ValueError):
        match_columns(
            columns=['A', 'B', 'C'],
            schema=[
                Column(colid=0, name='C'),
                Column(colid=1, name='B'),
                Column(colid=2, name='D')
            ],
            renamed={'D': 'A', 'E': 'A'}
        )


def test_merging_incomplete_schemas():
    schema = ArchiveSchema()
    # Merge first schema.
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=0)
    cname, cage, csalary = schema.at_version(0)
    schema, _, _ = schema.merge(
        columns=[cname, 'Height'],
        version=1,
        partial=True,
        origin=0
    )
    columns = schema.at_version(1)
    assert columns == ['Name', 'Age', 'Salary', 'Height']
    assert [c.colid for c in columns] == [0, 1, 2, 3]


def test_merging_snapshot_schema_by_name():
    """Test merging snapshot schemas into an archive schema while matching
    columns by name instead of column identifier.
    """
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=0)
    schema, matched_schema, _ = schema.merge(
        columns=['Salary', 'Age', 'Name'],
        version=1,
        match_by_name=True,
        origin=0
    )
    assert matched_schema[0].colid == 2
    assert matched_schema[1].colid == 1
    assert matched_schema[2].colid == 0
    schema, matched_schema, _ = schema.merge(
        columns=['Person', 'Salary', 'Age'],
        version=2,
        match_by_name=True,
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
        match_by_name=True,
        origin=0
    )
    schema, matched_schema, _ = schema.merge(
        columns=['Person', 'Salary', 'Age'],
        version=2,
        match_by_name=True,
        origin=1,
        renamed={'Name': 'Person'}
    )
    assert schema.at_version(0) == ['Name', 'Age', 'Salary']
    assert schema.at_version(1) == ['Salary', 'Age', 'Name']
    assert schema.at_version(2) == ['Person', 'Salary', 'Age']
    assert len(schema.columns) == 3


def test_merging_snapshot_schemas():
    """Test merging snapshot schemas into an archive schema."""
    schema = ArchiveSchema()
    # Merge first schema.
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=0)
    cname, cage, csalary = schema.at_version(0)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    # Move columns around
    schema, _, _ = schema.merge(columns=[csalary, cage, cname], version=1)
    cname, cage, csalary = schema.at_version(0)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    csalary, cage, cname = schema.at_version(1)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    # Rename one column
    cname = Column(colid=0, name='Person')
    schema, _, _ = schema.merge(columns=[cname, csalary, cage], version=2)
    cname, cage, csalary = schema.at_version(0)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    csalary, cage, cname = schema.at_version(1)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    cname, csalary, cage = schema.at_version(2)
    assert cname == 'Person'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    # Insert and remove columns
    schema, _, _ = schema.merge(columns=[cname, cage, 'Height'], version=3)
    cname, cage, csalary = schema.at_version(0)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    csalary, cage, cname = schema.at_version(1)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    cname, csalary, cage = schema.at_version(2)
    assert cname == 'Person'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    cname, cage, cheight = schema.at_version(3)
    assert cname == 'Person'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert cheight == 'Height'
    assert cheight.colid == 3
    # Re-add salary colum
    schema, _, _ = schema.merge(
        columns=[cname, cage, csalary, cheight],
        version=4
    )
    cname, cage, csalary = schema.at_version(0)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    csalary, cage, cname = schema.at_version(1)
    assert cname == 'Name'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    cname, csalary, cage = schema.at_version(2)
    assert cname == 'Person'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    cname, csalary, cheight = schema.at_version(3)
    assert cname == 'Person'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert cheight == 'Height'
    assert cheight.colid == 3
    cname, cage, csalary, cheight = schema.at_version(4)
    assert cname == 'Person'
    assert cname.colid == 0
    assert cage == 'Age'
    assert cage.colid == 1
    assert csalary == 'Salary'
    assert csalary.colid == 2
    assert cheight == 'Height'
    assert cheight.colid == 3
