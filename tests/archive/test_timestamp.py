# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for timestamp objects."""

import pytest

from histore.archive.timestamp import TimeInterval, Timestamp


def test_interval():
    """Test interval initialization, containment and overlap."""
    # Test contains
    i1 = TimeInterval(start=0, end=10)
    assert i1.contains(interval=TimeInterval(1))
    assert i1.contains(interval=TimeInterval(10))
    assert i1.contains(interval=TimeInterval(start=0, end=5))
    assert i1.contains(interval=TimeInterval(start=5, end=8))
    assert i1.contains(interval=TimeInterval(start=9, end=10))
    assert i1.contains(interval=TimeInterval(start=0, end=10))
    assert not i1.contains(interval=TimeInterval(start=0, end=11))
    assert not i1.contains(interval=TimeInterval(start=1, end=11))
    # Test overlaps
    i1 = TimeInterval(start=10, end=20)
    assert not i1.overlap(interval=TimeInterval(start=1, end=5))
    assert i1.overlap(interval=TimeInterval(start=0, end=10))
    assert i1.overlap(interval=TimeInterval(start=5, end=15))
    assert i1.overlap(interval=TimeInterval(start=5, end=25))
    assert i1.overlap(interval=TimeInterval(start=10, end=20))
    assert i1.overlap(interval=TimeInterval(start=15, end=25))
    assert i1.overlap(interval=TimeInterval(start=19, end=25))
    assert i1.overlap(interval=TimeInterval(start=20, end=25))
    assert not i1.overlap(interval=TimeInterval(start=21, end=25))
    with pytest.raises(ValueError):
        i1.contains()
    # String representation
    assert str(i1) == '10-20'
    assert str(TimeInterval(start=10)) == '10'
    # Initialize
    with pytest.raises(ValueError):
        TimeInterval(start=10, end=9)
    with pytest.raises(ValueError):
        TimeInterval(start=-10, end=9)


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
    t = Timestamp([
        TimeInterval(1, 5),
        TimeInterval(7, 9),
        TimeInterval(14, 16)
    ])
    for version in [1, 2, 3, 4, 5, 7, 8, 9, 14, 15, 16]:
        assert t.contains(version=version)
    for version in [0, 6, 10, 11, 12, 13, 17, 18]:
        assert not t.contains(version=version)


def test_timestamp_from_string():
    """Test generating timestamps from stings."""
    t = Timestamp.from_string('5')
    assert len(t.intervals) == 1
    assert t.contains(5)
    t = Timestamp.from_string('1,3,5')
    assert len(t.intervals) == 3
    for i in [1, 3, 5]:
        assert t.contains(i)
    t = Timestamp.from_string('1-3,5')
    assert len(t.intervals) == 2
    for i in [1, 2, 3, 5]:
        assert t.contains(i)
    with pytest.raises(ValueError):
        Timestamp.from_string('1,3--5')
        Timestamp.from_string('abc')


def test_timestamp_init():
    """Test timestamp initialization."""
    t = Timestamp()
    assert t.is_empty()
    assert t.last_version() is None
    t = Timestamp([TimeInterval(1), TimeInterval(3)])
    assert not t.is_empty()
    assert t.last_version() == 3
    t = Timestamp(version=1)
    assert not t.is_empty()
    assert t.contains(version=1)
    assert not t.contains(version=0)
    assert t.last_version() == 1
    with pytest.raises(ValueError):
        Timestamp([TimeInterval(1, 3), TimeInterval(3, 4)])
    with pytest.raises(ValueError):
        Timestamp([TimeInterval(1, 3), TimeInterval(2, 3)])
    with pytest.raises(ValueError):
        Timestamp([TimeInterval(-1, 3), TimeInterval(2, 3)])
    with pytest.raises(ValueError):
        Timestamp([TimeInterval(-1)])
    with pytest.raises(ValueError):
        Timestamp(intervals=[TimeInterval(2, 3)], version=1)


def test_timestamp_is_equal():
    """Test contains function of timestamp."""
    t = Timestamp([
        TimeInterval(1, 5),
        TimeInterval(7, 9),
        TimeInterval(14, 16)
    ])
    t1 = Timestamp([
        TimeInterval(1, 5),
        TimeInterval(7, 9),
        TimeInterval(14, 16)
    ])
    t2 = Timestamp([
        TimeInterval(1, 5),
        TimeInterval(7, 9),
        TimeInterval(14, 17)
    ])
    t3 = Timestamp([
        TimeInterval(1, 5),
        TimeInterval(7, 9),
        TimeInterval(14, 16),
        TimeInterval(18)
    ])
    t4 = Timestamp([TimeInterval(1, 5), TimeInterval(7, 9)])
    assert t.is_equal(t1)
    assert not t.is_equal(t2)
    assert not t.is_equal(t3)
    assert not t.is_equal(t4)


def test_timestamp_tostring():
    """Test timestamp string representation."""
    t = Timestamp([
        TimeInterval(1, 5),
        TimeInterval(7, 9),
        TimeInterval(14, 16)
    ])
    assert str(t) == '1-5,7-9,14-16'
    t = Timestamp([TimeInterval(1, 5), TimeInterval(7), TimeInterval(14, 16)])
    assert str(t) == '1-5,7,14-16'
    assert str(Timestamp([TimeInterval(1)])) == '1'
