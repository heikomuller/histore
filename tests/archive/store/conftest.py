# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Fixtures for archive store unit tests."""

import pytest

from histore.archive.row import ArchiveRow
from histore.archive.schema import ArchiveSchema
from histore.archive.timestamp import SingleVersion
from histore.archive.value import SingleVersionValue
from histore.key import NumberKey


@pytest.fixture
def archives():
    ts = SingleVersion(version=0)
    schema_v1, _ = ArchiveSchema().merge(columns=['A', 'B'], version=0)
    # First version
    row1_v1 = ArchiveRow(
        rowid=0,
        key=NumberKey(0),
        pos=SingleVersionValue(value=0, timestamp=ts),
        cells={
            0: SingleVersionValue(value='a', timestamp=ts),
            1: SingleVersionValue(value='b', timestamp=ts)
        },
        timestamp=ts
    )
    # Second version.
    schema_v2, _ = schema_v1.merge(columns=['A', 'B'], version=1)
    row1_v2 = row1_v1.merge(pos=0, values={1: 'a', 2: 'c'}, version=1)
    ts = SingleVersion(version=1)
    row2_v2 = ArchiveRow(
        rowid=1,
        key=NumberKey(1),
        pos=SingleVersionValue(value=1, timestamp=ts),
        cells={
            0: SingleVersionValue(value='d', timestamp=ts),
            1: SingleVersionValue(value='e', timestamp=ts)
        },
        timestamp=ts
    )
    return {
        1: (schema_v1, [row1_v1], 0),
        2: (schema_v2, [row1_v2, row2_v2], 1)
    }
