# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for merging schemas when matching columns by identifier and
column name.
"""

from histore.archive.schema import ArchiveSchema


def test_complete_match_by_idname():
    """Test matching complete schema versions by column identifier and column
    name.
    """
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        version=0
    )
    columns = schema.at_version(0)
    columns[1] = 'BDate'
    schema, _, _ = schema.merge(
        columns=columns,
        version=1,
        origin=0,
        renamed={'Age': 'BDate'}
    )
    assert len(schema.columns) == 3
    columns = schema.at_version(1)
    assert columns == ['Name', 'BDate', 'Salary']
    columns = ['Height'] + columns[::-1]
    schema, _, _ = schema.merge(
        columns=columns,
        version=2,
        origin=1
    )
    assert len(schema.columns) == 4
    columns = schema.at_version(2)
    assert columns == ['Height', 'Salary', 'BDate', 'Name']
