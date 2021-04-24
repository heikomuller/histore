# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for timestamp objects."""

import pytest

from histore.archive.timestamp import Timestamp, SingleVersion


def test_timestamp_append():
    """Test appending values to a timestamp."""
    t = Timestamp()
    t1 = t.append(1)
    assert t.is_empty()
    assert not t1.is_empty()
    t2 = t1.append(2)
    t3 = t2.append(3)
    t4 = t3.append(5)
    assert len(t1.intervals) == 1
    assert len(t2.intervals) == 1
    assert len(t3.intervals) == 1
    assert len(t4.intervals) == 2
    assert t4.contains(1)
    assert t4.contains(2)
    assert t4.contains(3)
    assert not t4.contains(4)
    assert t4.contains(5)
    with pytest.raises(ValueError):
        t4.append(1)


def test_timestamp_contains():
    """Test contains function of timestamp."""
    t = Timestamp([[1, 5], [7, 9], 11, [14, 16], 18])
    for version in [1, 2, 3, 4, 5, 7, 8, 9, 11, 14, 15, 16, 18]:
        assert t.contains(version=version)
    for version in [0, 6, 10, 12, 13, 17, 19]:
        assert not t.contains(version=version)
    assert not Timestamp().contains(0)


def test_timestamp_init():
    """Test timestamp initialization."""
    t = Timestamp()
    assert t.is_empty()
    assert t.last_version() is None
    t = Timestamp([1, 3])
    assert not t.is_empty()
    assert t.last_version() == 3
    t = SingleVersion(version=1)
    assert not t.is_empty()
    assert t.contains(version=1)
    assert not t.contains(version=0)
    assert t.last_version() == 1


def test_timestamp_is_equal():
    """Test contains function of timestamp."""
    t = Timestamp([[1, 5], [7, 9], 10, [14, 16]])
    t1 = Timestamp([[1, 5], [7, 9], [10], [14, 16]])
    t2 = Timestamp([[1, 5], [7, 9], [14, 17]])
    t3 = Timestamp([[1, 5], [7, 9], [14, 16], [18]])
    t4 = Timestamp([[1, 5], [7, 9], 12])
    t5 = Timestamp([[1, 5], [7, 9], [12]])
    assert t.is_equal(t1)
    assert not t.is_equal(t2)
    assert not t.is_equal(t3)
    assert not t.is_equal(t4)
    assert t4.is_equal(t5)
    for ts in [t, t1, t2, t3, t4, t5]:
        assert ts.is_equal(ts)


@pytest.mark.parametrize(
    'version,intervals',
    [
        (1, []),
        (2, [2]),
        (4, [[2, 3]]),
        (5, [[2, 3], [5, 5]]),
        (7, [[2, 3], [5, 7]]),
        (8, [[2, 3], [5, 7]]),
        (9, [[2, 3], [5, 7], 9]),
        (11, [[2, 3], [5, 7], [9], [11]]),
        (12, [[2, 3], [5, 7], [9], [11, 12]])
    ]
)
def test_timestamp_rollback(version, intervals):
    """Test removing versions from a timestamp."""
    ts = Timestamp(intervals=[[2, 3], [5, 7], 9, [11, 12]])
    assert ts.rollback(version).is_equal(Timestamp(intervals=intervals))


def test_timestamp_rollback_edge_cases():
    """Test edge cases for timestamp rollback."""
    assert Timestamp(intervals=[[2, 3]]).rollback(4).intervals == [[2, 3]]
    assert Timestamp(intervals=[2, 3]).rollback(4).intervals == [2, 3]


def test_timestamp_tostring():
    """Test timestamp string representation."""
    t = Timestamp([[1, 5], [7, 9], [14, 16]])
    assert str(t) == str([[1, 5], [7, 9], [14, 16]])
    t = Timestamp([[1, 5], [7], [14, 16]])
    assert str(t) == str([[1, 5], [7], [14, 16]])
    assert str(SingleVersion(1)) == '[1]'
