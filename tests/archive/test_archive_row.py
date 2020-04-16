# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for rows in an archived dataset."""

from histore.archive.row import ArchiveRow
from histore.archive.value import OmnipresentCell
from histore.archive.timestamp import Timestamp


def test_merge_archive_rows():
    """Test merging cell values into an archive row."""
    ts = Timestamp(version=1)
    # First version []
    row = ArchiveRow(
        identifier=0,
        index=OmnipresentCell(value=2, timestamp=ts),
        cells=dict(),
        timestamp=ts
    )
    pos, values = row.at_version(version=1, columns=[])
    assert pos == 2
    assert len(values) == 0
    # Second version ['A', 1, 'a']
    ts = Timestamp(version=2)
    row = row.merge(pos=1, values={1: 'A', 2: 1, 3: 'a'}, version=2)
    pos, values = row.at_version(version=2, columns=[1, 2, 3])
    assert pos == 1
    assert values == ['A', 1, 'a']
    # Third version ['B', 1, 'b']
    row = row.merge(pos=1, values={1: 'B', 2: 1, 3: 'b'}, version=3)
    pos, values = row.at_version(version=3, columns=[1, 2, 3])
    assert pos == 1
    assert values == ['B', 1, 'b']
    # Fourth version change col 1 from source = 3
    row = row.merge(
        pos=2,
        values={1: 'C'},
        version=4,
        unchanged=[2, 3],
        origin=3
    )
    pos, values = row.at_version(version=4, columns=[1, 2, 3])
    assert pos == 2
    assert values == ['C', 1, 'b']
    # Fifth version change col 2 with source 2
    row = row.merge(
        pos=2,
        values={2: 0},
        version=5,
        unchanged=[1, 3],
        origin=2
    )
    pos, values = row.at_version(version=5, columns=[1, 2, 3])
    assert pos == 2
    assert values == ['A', 0, 'a']
