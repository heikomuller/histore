# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the default archive serializer."""

import pytest

from histore.key.base import NumberKey, StringKey
from histore.archive.row import ArchiveRow
from histore.archive.schema import ArchiveColumn
from histore.archive.snapshot import Snapshot
from histore.archive.value import (
    MultiVersionValue, SingleVersionValue
)
from histore.archive.timestamp import Timestamp, TimeInterval
from histore.archive.serialize.compact import CompactSerializer
from histore.archive.serialize.default import DefaultSerializer

import histore.util as util


def test_invalid_label():
    """Test error when creating a serializer with an invalid label value."""
    s = DefaultSerializer(timestamp='ts')
    assert s.timestamp == 'ts'
    with pytest.raises(ValueError):
        DefaultSerializer(timestamp='$ts')


def test_serialize_column():
    """Test (de-)serialization of archive schema columns."""
    serializer = DefaultSerializer()
    ts = Timestamp(version=1)
    column = ArchiveColumn(
        identifier=0,
        name=SingleVersionValue(value='A', timestamp=ts),
        pos=SingleVersionValue(value=1, timestamp=ts),
        timestamp=ts
    )
    obj = serializer.serialize_column(column)
    column = serializer.deserialize_column(obj)
    assert column.identifier == 0
    assert column.name.value == 'A'
    assert column.pos.value == 1
    assert column.timestamp.is_equal(ts)


def test_serialize_row():
    """Test (de-)serialization of archive rows."""
    serializer = DefaultSerializer()
    ts = Timestamp(intervals=TimeInterval(start=1, end=5))
    pos = SingleVersionValue(value=0, timestamp=ts)
    key = (NumberKey(0), StringKey('A'))
    cells = {
        0: SingleVersionValue(value=0, timestamp=ts),
        1: MultiVersionValue(values=[
            SingleVersionValue(
                value='A',
                timestamp=Timestamp(intervals=TimeInterval(start=1, end=3))
            ),
            SingleVersionValue(
                value='B',
                timestamp=Timestamp(intervals=TimeInterval(start=4, end=5))
            )
        ])
    }
    row = ArchiveRow(rowid=0, key=key, pos=pos, cells=cells, timestamp=ts)
    obj = serializer.serialize_row(row)
    row = serializer.deserialize_row(obj)
    assert row.rowid == 0
    assert row.pos.value == 0
    assert len(row.cells) == 2
    assert row.key == key


def test_serialize_snapshot():
    """Test (de-)serialization of snapshot descriptors."""
    serializer = DefaultSerializer()
    vt = util.utc_now()
    # -- Snapshot without description -----------------------------------------
    snapshot = Snapshot(0, valid_time=vt)
    obj = serializer.serialize_snapshot(snapshot)
    s = serializer.deserialize_snapshot(obj)
    assert s.version == snapshot.version
    assert s.valid_time == snapshot.valid_time
    assert s.transaction_time == snapshot.transaction_time
    assert s.description == snapshot.description
    # For completeness, test deserializing a snapshot object without
    # description element.
    snapshot.description = None
    obj = serializer.serialize_snapshot(snapshot)
    s = serializer.deserialize_snapshot(obj)
    assert s.version == snapshot.version
    assert s.valid_time == snapshot.valid_time
    assert s.transaction_time == snapshot.transaction_time
    assert s.description == ''
    # -- Snapshot with description --------------------------------------------
    snapshot = Snapshot(0, valid_time=vt, description='First snapshot')
    obj = serializer.serialize_snapshot(snapshot)
    s = serializer.deserialize_snapshot(obj)
    assert s.version == snapshot.version
    assert s.valid_time == snapshot.valid_time
    assert s.transaction_time == snapshot.transaction_time
    assert s.description == snapshot.description


@pytest.mark.parametrize('serializer_cls', [DefaultSerializer, CompactSerializer])
def test_serialize_timestamp(serializer_cls):
    """Test (de-)serialization of timestamp objects."""
    serializer = serializer_cls()
    ts = serializer.deserialize_timestamp([[1, 2], [4, 6], [8, 8]])
    for v in [1, 2, 4, 5, 6, 8]:
        assert ts.contains(v)
    for v in [0, 3, 7, 9]:
        assert not ts.contains(v)
    obj = serializer.serialize_timestamp(ts)
    ts = serializer.deserialize_timestamp(obj)
    assert ts.is_equal(Timestamp([TimeInterval(start=1, end=2), TimeInterval(start=4, end=6), TimeInterval(8)]))
    ts = serializer.deserialize_timestamp(obj)
    for v in [1, 2, 4, 5, 6, 8]:
        assert ts.contains(v)
    for v in [0, 3, 7, 9]:
        assert not ts.contains(v)
    with pytest.raises(ValueError):
        serializer.deserialize_timestamp([[1, 2], [3, 6], [8, 8]])


@pytest.mark.parametrize('serializer_cls', [DefaultSerializer, CompactSerializer])
def test_serialize_value(serializer_cls):
    """Test (de-)serialization of timestamped values."""
    serializer = serializer_cls()
    # -- Single version value -------------------------------------------------
    ts = Timestamp(version=1)
    value = SingleVersionValue(value=1, timestamp=ts)
    obj = serializer.serialize_value(value=value, ts=ts)
    value = serializer.deserialize_value(obj=obj, ts=ts)
    assert value.timestamp.is_equal(Timestamp(version=1))
    assert value.value == 1
    ts = ts.append(2)
    obj = serializer.serialize_value(value=value, ts=ts)
    value = serializer.deserialize_value(obj=obj, ts=ts)
    assert value.timestamp.is_equal(Timestamp(version=1))
    assert value.value == 1
    # -- Multi-version value --------------------------------------------------
    value = SingleVersionValue(value=1, timestamp=Timestamp(version=1))
    value = value.merge(value='A', version=2)
    obj = serializer.serialize_value(value=value, ts=ts)
    value = serializer.deserialize_value(obj=obj, ts=ts)
    assert len(value.values) == 2
    for v in value.values:
        if v.timestamp.contains(1):
            assert v.value == 1
        else:
            assert v.timestamp.contains(2)
            assert v.value == 'A'
