# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for archived cell values."""

import pytest

from histore.archive.value import MultiVersionValue, SingleVersionValue
from histore.archive.timestamp import Timestamp, TimeInterval


def test_cell_history():
    """Test adding values to the history of a dataset row cell."""
    cell = SingleVersionValue(value=1, timestamp=Timestamp(version=1))
    assert cell.at_version(version=1) == 1
    assert cell.is_single_version()
    assert not cell.is_multi_version()
    with pytest.raises(ValueError):
        cell.at_version(version=2)
    assert cell.at_version(version=2, raise_error=False) is None
    cell = cell.merge(value=1, version=2)
    assert cell.at_version(version=1) == 1
    assert cell.at_version(version=2) == 1
    assert cell.diff(original_version=1, new_version=2) is None
    assert cell.at_version(version=3, raise_error=False) is None
    prov = cell.diff(original_version=2, new_version=3)
    assert prov is not None
    assert prov.old_value == 1
    assert prov.new_value is None
    cell = SingleVersionValue(value=1, timestamp=Timestamp(version=1))
    cell = cell.merge(value='1', version=2)
    assert len(cell.values) == 2
    assert cell.at_version(version=1) == 1
    assert cell.at_version(version=2) == '1'
    prov = cell.diff(original_version=1, new_version=2)
    assert prov is not None
    assert prov.old_value == 1
    assert prov.new_value == '1'
    with pytest.raises(ValueError):
        cell.at_version(version=3)
    cell = cell.merge(value=1, version=3)
    assert len(cell.values) == 2
    assert cell.at_version(version=1) == 1
    assert cell.at_version(version=2) == '1'
    assert cell.at_version(version=3) == 1
    assert not cell.is_single_version()
    assert cell.is_multi_version()


def test_extend_cell_value_timestamp():
    """Test extending the timestamp of a cell value."""
    cell = SingleVersionValue(value=1, timestamp=Timestamp(version=1))
    cell = cell.extend(version=2, origin=1)
    assert not cell.timestamp.contains(0)
    assert cell.timestamp.contains(1)
    assert cell.timestamp.contains(2)
    assert not cell.timestamp.contains(3)
    cell = cell.extend(version=4, origin=0)
    assert not cell.timestamp.contains(0)
    assert cell.timestamp.contains(1)
    assert cell.timestamp.contains(2)
    assert not cell.timestamp.contains(3)
    assert not cell.timestamp.contains(4)
    cell = cell.merge(value='1', version=3)
    cell = cell.merge(value=1, version=4)
    cell = cell.extend(version=5, origin=4)
    cell = cell.extend(version=6, origin=3)
    assert cell.at_version(1) == 1
    assert cell.at_version(2) == 1
    assert cell.at_version(3) == '1'
    assert cell.at_version(4) == 1
    assert cell.at_version(5) == 1
    assert cell.at_version(6) == '1'
    with pytest.raises(ValueError):
        cell.at_version(0)


def test_rollback_multi_value():
    """Test rollback for single version values."""
    value = MultiVersionValue([
        SingleVersionValue(
            value=1,
            timestamp=Timestamp(intervals=[TimeInterval(start=2, end=3)])
        ),
        SingleVersionValue(
            value=2,
            timestamp=Timestamp(intervals=[TimeInterval(start=4, end=5)])
        )
    ])
    value = value.rollback(4)
    assert isinstance(value, MultiVersionValue)
    assert len(value.values) == 2
    assert value.at_version(3) == 1
    assert value.at_version(4) == 2
    value = value.rollback(2)
    assert isinstance(value, SingleVersionValue)
    assert value.value == 1
    # -- Rollback to version that did not contain the value -------------------
    value = MultiVersionValue([
        SingleVersionValue(
            value=1,
            timestamp=Timestamp(intervals=[TimeInterval(start=2, end=3)])
        ),
        SingleVersionValue(
            value=2,
            timestamp=Timestamp(intervals=[TimeInterval(start=4, end=5)])
        )
    ])
    assert value.rollback(1) is None


def test_rollback_single_value():
    """Test rollback for single version values."""
    value = SingleVersionValue(
        value=1,
        timestamp=Timestamp(intervals=[TimeInterval(start=1, end=3)])
    )
    value = value.rollback(2)
    assert value.value == 1
    assert value.timestamp.contains(1)
    assert value.timestamp.contains(2)
    assert not value.timestamp.contains(3)
    assert value.rollback(0) is None
