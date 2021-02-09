# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Timestamps are sequences of snapshot (version) identifier that define when
a node was part of the archived document.
"""

from __future__ import annotations
from typing import List, Optional


class TimeInterval(object):
    """Interval of integer numbers. Defines that start and end of the interval.
    Ensures that start is lower or equal that end.
    """
    def __init__(self, start, end=None):
        """Initialize the interval from a given integer pair. If end is None it
        is assumed to be equal to start.

        Ensures that start is lower or equal than end (if given). Interval
        boundaries cannot be negative values.

        Parameters
        ----------
        start: int
            First version in the interval.
        end: int, optional
            Last version (inclusive) in the interval.
        """
        self.start = start
        self.end = end if end is not None else start
        if self.start < 0 or self.end < self.start:
            raise ValueError('invalid interval {}'.format(self))

    def __repr__(self):
        """Unambiguous string representation of this time interval.

        Returns
        -------
        string
        """
        if self.start < self.end:
            return '{}-{}'.format(self.start, self.end)
        else:
            return str(self.start)

    def contains(self, interval=None, version=None):
        """Returns true if this interval contains the given interval or value.

        Raises ValueError if both arguments are None or not None.

        Parameters
        ----------
        interval: histore.archive.timestamp.TimeInterval, optional
            Interval of version numbers.
        version: int, optional
            Single version number

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        """
        if interval is not None and version is None:
            return self.start <= interval.start and self.end >= interval.end
        elif interval is None and version is not None:
            return self.start <= version <= self.end
        else:
            raise ValueError('invalid arguments')

    def is_equal(self, interval):
        """Returns True if the two intervals have equal start and end values.

        Parameters
        ----------
        interval: histore.archive.timestamp.TimeInterval
            Interval of version numbers.

        Returns
        -------
        bool
        """
        return self.start == interval.start and self.end == interval.end

    def overlap(self, interval):
        """Test if this interval overlaps with the given interval. To intervals
        overlap if they have at least one version in common.

        Parameters
        ----------
        interval: histore.archive.timestamp.TimeInterval
            Interval of version numbers.

        Returns
        -------
        bool
        """
        if self.start <= interval.start:
            return self.end >= interval.start
        else:
            return interval.end >= self.start


class Timestamp(object):
    """A timestamp is a sequence of integer numbers represented as a list of
    integer intervals.
    """
    def __init__(
        self, intervals: Optional[List[TimeInterval]] = None,
        version: Optional[int] = None
    ):
        """Initialize the timestamp using an list of time intervals. Ensures
        that intervals are not adjacent or overlapping. Raises a ValueError if
        both arguments are not None.

        Parameters
        ----------
        intervals: list(histore.archive.timestamp.TimeInterval), optional
            List of intervals.
        version: int
            Unique version identifier.

        Raises
        ------
        ValueError
        """
        if intervals is not None and version is not None:
            raise ValueError('invalid arguments')
        if version is not None:
            self.intervals = [TimeInterval(version)]
        else:
            if intervals is None:
                self.intervals = list()
            elif isinstance(intervals, TimeInterval):
                self.intervals = [intervals]
            else:
                self.intervals = intervals
            # Validate the given list of intervals
            for i in range(1, len(self.intervals)):
                if self.intervals[i - 1].end + 1 >= self.intervals[i].start:
                    raise ValueError('ajacent or overlapping intervals')

    def __str__(self) -> str:
        """Get a string representation of the timestamp.

        Returns
        -------
        string
        """
        return ','.join([str(interval) for interval in self.intervals])

    def append(self, version: int) -> Timestamp:
        """Append the given version to the timestamp. Returns a new modified
        timestamp. Raises ValueError if the given value is lower or equal to
        the last version in the sequence.

        Parameters
        ----------
        version: int
            Version identifier.

        Returns
        -------
        histore.archive.timestamp.Timestamp
        """
        if len(self.intervals) > 0:
            last_interval = self.intervals[-1]
            if last_interval.end >= version:
                msg = 'cannot append {} to [{}]'
                raise ValueError(msg.format(version, self))
            elif version == last_interval.end + 1:
                interval = TimeInterval(start=last_interval.start, end=version)
                return Timestamp(intervals=self.intervals[:-1] + [interval])
            else:
                interval = TimeInterval(version)
                return Timestamp(intervals=self.intervals + [interval])
        else:
            return Timestamp(intervals=[TimeInterval(version)])

    def contains(self, version: int) -> bool:
        """Returns True if the timestamp contains the given value.

        Parameters
        ----------
        version: int
            Version identifier.

        Returns
        -------
        bool
        """
        # Search from end of interval list since the majority of requests will
        # access more recent values (i.e., database versions).
        for interval in self.intervals[::-1]:
            if interval.contains(version=version):
                return True
            elif interval.end < version:
                return False
        return False

    @staticmethod
    def from_string(text: str) -> Timestamp:
        """Create a timestamp instance from a string representation as
        generated by the __str__ method.

        Parameters
        ----------
        text: string

        Returns
        -------
        histore.archive.timestamp.Timestamp
        """
        intervals = list()
        for token in text.strip().split(','):
            pos = token.find('-')
            if pos > 0:
                start = int(token[:pos])
                end = int(token[pos + 1:])
                intervals.append(TimeInterval(start, end))
            else:
                intervals.append(TimeInterval(int(token)))
        return Timestamp(intervals=intervals)

    def is_empty(self) -> bool:
        """Returns True if the timestamp is empty.

        Returns
        -------
        bool
        """
        return len(self.intervals) == 0

    def is_equal(self, timestamp: Timestamp) -> bool:
        """Returns True if the two timestamps are equal, i.e., represent the
        same sequence of snapshots. Given that intervals cannot be consecutive,
        equality means (i) same number of intervals, and (ii) corresponding
        intervals have same start/end values.

        Parameters
        ----------
        timestamp: histore.archive.timestamp.Timestamp

        Returns
        -------
        bool
        """
        if len(self.intervals) == len(timestamp.intervals):
            for i in range(len(self.intervals)):
                if not self.intervals[i].is_equal(timestamp.intervals[i]):
                    return False
            return True
        return False

    def last_version(self) -> int:
        """Get the last version in the timestamp. If the timestamp is empty the
        result is None.

        Returns
        -------
        int
        """
        return self.intervals[-1].end if self.intervals else None

    def rollback(self, version: int) -> Timestamp:
        """Remove all versions after the given version from the timestamp.

        Returns a modifed timestamp.

        Parameters
        ----------
        version: int
            Last version in the rolled back timestamp. The result will not
            contain version numbers that are greater than the given version.

        Returns
        -------
        histore.archive.timestamp.Timestamp
        """
        # Create modified list of intervals.
        intervals = list()
        for interval in self.intervals:
            if interval.start > version:
                # Intervals are sorted. If the current interval starts after
                # the rollback version there is nothing more to be added.
                break
            if interval.end <= version:
                # Add interval since it ended before the rollback version.
                intervals.append(interval)
            else:
                # The rollbacl version falls inside the interval. Clip the
                # interval and add it to the new timestamp. All following
                # intervals can be ignored.
                intervals.append(TimeInterval(start=interval.start, end=version))
                break
        return Timestamp(intervals=intervals)
