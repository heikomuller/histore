# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for rows in an archived dataset."""

import pytest

from histore.archive.row import ArchiveRow
from histore.archive.timestamp import SingleVersion, Timestamp, TimeInterval
from histore.archive.value import MultiVersionValue, SingleVersionValue
from histore.key import NumberKey, StringKey


def test_compare_row_keys():
    """Test comparing row keys."""
    ts = SingleVersion(version=1)
    pos = SingleVersionValue(value=0, timestamp=ts)
    row = ArchiveRow(
        rowid=0,
        key=NumberKey(0),
        pos=pos,
        cells=dict(),
        timestamp=ts
    )
    assert row.comp(NumberKey(0)) == 0
    assert row.comp(NumberKey(2)) == -1
    assert row.comp(NumberKey(-2)) == 1
    assert row.comp(StringKey('0')) == -1
    row = ArchiveRow(
        rowid=0,
        key=StringKey('B'),
        pos=pos,
        cells=dict(),
        timestamp=ts
    )
    assert row.comp(StringKey('B')) == 0
    assert row.comp(StringKey('C')) == -1
    assert row.comp(StringKey('A')) == 1
    row = ArchiveRow(
        rowid=0,
        key=(NumberKey(0), NumberKey(1)),
        pos=pos,
        cells=dict(),
        timestamp=ts
    )
    assert row.comp((NumberKey(0), NumberKey(1))) == 0
    assert row.comp((NumberKey(0), NumberKey(2))) == -1
    assert row.comp((NumberKey(-1), NumberKey(10))) == 1


def test_extend_archive_row():
    """Test extending the timestampes for archive rows relative to a given
    version of origin.
    """
    ts = SingleVersion(version=1)
    pos = SingleVersionValue(value=0, timestamp=ts)
    row = ArchiveRow(
        rowid=0,
        key=NumberKey(0),
        pos=pos,
        cells=dict(),
        timestamp=ts
    )
    assert str(row) == '\n'.join([
        '<ArchiveRow (',
        '\tid=0',
        '\tkey=0',
        '\ttimestamp=[[1]]',
        '\tpos=(0 [1])',
        '\tvalues={})>'
    ])
    row = row.merge(pos=1, values={1: 'A', 2: 1, 3: 'a'}, version=2)
    row = row.merge(pos=1, values={1: 'B', 2: 1, 3: 'b'}, version=3)
    row = row.extend(version=4, origin=2)
    pos, values = row.at_version(version=4, columns=[1, 2, 3])
    assert pos == 1
    assert values == ['A', 1, 'a']
    row = row.extend(version=5, origin=1)
    # Error for unknown version.
    with pytest.raises(ValueError):
        row.at_version(version=5, columns=[1, 2])
    # Error for unknown column.
    with pytest.raises(ValueError):
        row.at_version(version=4, columns=[1, 5], raise_error=True)
    pos, values = row.at_version(version=4, columns=[1, 5], raise_error=False)
    assert pos == 1
    assert values == ['A', None]
    pos, values = row.at_version(
        version=5,
        columns=[1, 2, 3],
        raise_error=False
    )
    assert pos == 0
    assert values == [None, None, None]
    assert row == row.extend(version=7, origin=6)


def test_merge_archive_rows():
    """Test merging cell values into an archive row."""
    ts = SingleVersion(version=1)
    # First version []
    row = ArchiveRow(
        rowid=0,
        key=NumberKey(0),
        pos=SingleVersionValue(value=2, timestamp=ts),
        cells=dict(),
        timestamp=ts
    )
    pos, values = row.at_version(version=1, columns=[])
    assert pos == 2
    assert len(values) == 0
    # Second version ['A', 1, 'a']
    ts = SingleVersion(version=2)
    row = row.merge(pos=1, values={1: 'A', 2: 1, 3: 'a'}, version=2)
    pos, values = row.at_version(version=2, columns=[1, 2, 3])
    assert pos == 1
    assert values == ['A', 1, 'a']
    # Third version ['B', 1, 'b']
    row = row.merge(pos=1, values={1: 'B', 2: 1, 3: 'b'}, version=3)
    pos, values = row.at_version(version=3, columns=[1, 2, 3])
    assert pos == 1
    assert values == ['B', 1, 'b']
    # Fourth version deletes column 3.
    row = row.merge(
        pos=2,
        values={1: 'C', 2: 1},
        version=4
    )
    pos, values = row.at_version(version=4, columns=[1, 2])
    assert pos == 2
    assert values == ['C', 1]
    # Fifth version introduces a new column 4.
    row = row.merge(
        pos=2,
        values={1: 'C', 2: 0, 4: 'x'},
        version=5
    )
    pos, values = row.at_version(version=5, columns=[1, 2, 4])
    assert pos == 2
    assert values == ['C', 0, 'x']


def test_row_provenance():
    """Test diff operation for archive rows."""
    row = ArchiveRow(
        rowid=0,
        key=NumberKey(0),
        pos=MultiVersionValue(
            values=[
                SingleVersionValue(
                    value=1,
                    timestamp=Timestamp(intervals=[TimeInterval(1, 3)])
                ),
                SingleVersionValue(
                    value=2,
                    timestamp=Timestamp(intervals=[TimeInterval(4, 5)])
                )
            ]
        ),
        cells=dict({
            1: SingleVersionValue(
                value='A',
                timestamp=Timestamp(intervals=TimeInterval(2, 3))
            ),
            2: MultiVersionValue(
                values=[
                    SingleVersionValue(
                        value='X',
                        timestamp=Timestamp(intervals=[TimeInterval(1, 3)])
                    ),
                    SingleVersionValue(
                        value='Y',
                        timestamp=Timestamp(intervals=[TimeInterval(4, 5)])
                    )
                ]
            )
        }),
        timestamp=Timestamp(intervals=[TimeInterval(1, 5)])
    )
    assert row.diff(0, 0) is None
    assert row.diff(0, 1).is_insert()
    assert row.diff(5, 6).is_delete()
    prov = row.diff(1, 2)
    assert prov.updated_position() is None
    upd_cells = prov.updated_cells()
    assert len(upd_cells) == 1
    upd_cells[1].old_value is None
    upd_cells[1].new_value == 'A'
    assert row.diff(2, 3) is None
    prov = row.diff(3, 4)
    assert prov.updated_position().old_value == 1
    assert prov.updated_position().new_value == 2
    upd_cells = prov.updated_cells()
    assert len(upd_cells) == 2
    upd_cells[1].old_value == 'A'
    upd_cells[1].new_value is None
    upd_cells[2].old_value == 'X'
    upd_cells[1].new_value == 'Y'


def test_row_rollback():
    """Test rollback for an archive row."""
    row = ArchiveRow(
        rowid=0,
        key=NumberKey(0),
        pos=MultiVersionValue(
            values=[
                SingleVersionValue(
                    value=1,
                    timestamp=Timestamp(intervals=[TimeInterval(1, 3)])
                ),
                SingleVersionValue(
                    value=2,
                    timestamp=Timestamp(intervals=[TimeInterval(4, 5)])
                )
            ]
        ),
        cells=dict({
            1: SingleVersionValue(
                value='A',
                timestamp=Timestamp(intervals=[TimeInterval(3, 4)])
            ),
            2: MultiVersionValue(
                values=[
                    SingleVersionValue(
                        value='X',
                        timestamp=Timestamp(intervals=[TimeInterval(1, 3)])
                    ),
                    SingleVersionValue(
                        value='Y',
                        timestamp=Timestamp(intervals=[TimeInterval(4, 5)])
                    )
                ]
            )
        }),
        timestamp=Timestamp(intervals=[TimeInterval(1, 5)])
    )
    row = row.rollback(version=4)
    assert len(row.cells) == 2
    row = row.rollback(version=2)
    assert len(row.cells) == 1
    assert row.rollback(version=0) is None
