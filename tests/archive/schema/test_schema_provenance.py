# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for schema provenance information."""

from histore.archive.schema import ArchiveSchema

import histore.archive.schema as mode


def test_column_provenance():
    """Test provenance information for column updates."""
    schema = ArchiveSchema()
    schema, _, _ = schema.merge(
        columns=['Name', 'Age', 'Salary'],
        matching=mode.MATCH_NAME,
        version=0
    )
    schema, _, _ = schema.merge(
        columns=['Age', 'Name', 'Salary'],
        matching=mode.MATCH_NAME,
        version=1,
        origin=0
    )
    schema, _, _ = schema.merge(
        columns=['Name', 'Height'],
        renamed={'Age': 'Height'},
        matching=mode.MATCH_NAME,
        version=2,
        origin=1
    )
    # Updated column provenance
    prov = schema.columns[0].diff(0, 1)
    assert prov.updated_name() is None
    assert prov.updated_position() is not None
    prov = schema.columns[1].diff(1, 2)
    assert prov.updated_name().values() == ('Age', 'Height')
    assert prov.updated_position().values() == (0, 1)
    # Deleted column provenance
    prov = schema.columns[2].diff(1, 2)
    assert prov.is_delete()
    assert prov.key == 2
    assert schema.columns[2].diff(2, 3) is None
