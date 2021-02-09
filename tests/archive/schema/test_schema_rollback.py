# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for the rollback operation for the archive schema."""

from histore.archive.schema import ArchiveSchema


def test_rollback_schema():
    """Test rollback for an archive schema."""
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(columns=['Name', 'Age', 'Salary'], version=1)
    schema, _, _ = schema.merge(columns=['Name', 'Salary'], version=2, origin=1)
    schema, _, _ = schema.merge(columns=['Name', 'Salary', 'Height'], version=3, origin=2)
    assert schema.at_version(0) == []
    assert schema.at_version(1) == ['Name', 'Age', 'Salary']
    assert schema.at_version(2) == ['Name', 'Salary']
    assert schema.at_version(3) == ['Name', 'Salary', 'Height']
    # Rollback version 3.
    schema = schema.rollback(2)
    assert schema.at_version(1) == ['Name', 'Age', 'Salary']
    # Rollback version 2.
    schema = schema.rollback(1)
    assert schema.at_version(1) == ['Name', 'Age', 'Salary']
    # Rollback version 1.
    schema = schema.rollback(0)
    assert schema.at_version(0) == []
