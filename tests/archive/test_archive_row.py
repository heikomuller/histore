# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for rows in an archived dataset."""

import pytest

from histore.archive.row import ArchiveRow
from histore.archive.value import SingleVersionValue
from histore.archive.timestamp import Timestamp


def test_compare_row_keys():
    """test comparing row keys."""
    ts = Timestamp(version=1)
    pos = SingleVersionValue(value=0, timestamp=ts)
    row = ArchiveRow(identifier=0, index=pos, cells=dict(), timestamp=ts)
    assert row.comp(0) == 0
    assert row.comp(2) == -1
    assert row.comp(-2) == 1
    row = ArchiveRow(identifier='B', index=pos, cells=dict(), timestamp=ts)
    assert row.comp('B') == 0
    assert row.comp('C') == -1
    assert row.comp('A') == 1
    row = ArchiveRow(identifier=(0, 1), index=pos, cells=dict(), timestamp=ts)
    assert row.comp((0, 1)) == 0
    assert row.comp((0, 2)) == -1
    assert row.comp((-1, 10)) == 1


def test_extend_archive_row():
    """Test extending the timestampes for archive rows relative to a given
    version of origin.
    """
    ts = Timestamp(version=1)
    pos = SingleVersionValue(value=0, timestamp=ts)
    row = ArchiveRow(identifier=0, index=pos, cells=dict(), timestamp=ts)
    row = row.merge(pos=1, values={1: 'A', 2: 1, 3: 'a'}, version=2)
    row = row.merge(pos=1, values={1: 'B', 2: 1, 3: 'b'}, version=3)
    row = row.extend(version=4, origin=2)
    pos, values = row.at_version(version=4, columns=[1, 2, 3])
    assert pos == 1
    assert values == ['A', 1, 'a']
    row = row.extend(version=5, origin=1)
    with pytest.raises(ValueError):
        row.at_version(version=5, columns=[1, 2, 3])
    pos, values = row.at_version(
        version=5,
        columns=[1, 2, 3],
        raise_error=False
    )
    assert pos == 0
    assert values == [None, None, None]


def test_merge_archive_rows():
    """Test merging cell values into an archive row."""
    ts = Timestamp(version=1)
    # First version []
    row = ArchiveRow(
        identifier=0,
        index=SingleVersionValue(value=2, timestamp=ts),
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
