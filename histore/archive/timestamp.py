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
from typing import List, Optional, Union


def TimeInterval(start: int, end: Optional[int] = None):
    """Get a timestamp interval for the given parameters.

    Mainly used for test purposes.

    Parameters
    ----------
    start: int
        First version in the interval.
    end: int, optional
        Last version (inclusive) in the interval.
    """
    return start if end is None else [start, end]


class Timestamp(object):
    """A timestamp is a sequence of integer numbers represented as a list of
    integer intervals.
    """
    def __init__(self, intervals: List[Union[int, List[int]]] = None):
        """Initialize the list of time intervals.

        Parameters
        ----------
        intervals: list of histore.archive.timestamp.TimeInterval, optional
            List of time intervals.
        """
        self.intervals = intervals

    def __str__(self) -> str:
        """Get a string representation of the timestamp.

        Returns
        -------
        string
        """
        return str(self.intervals)

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
        if self.intervals:
            last_interval = self.interval(-1)
            current_end = last_interval[-1]
            if current_end >= version:
                raise ValueError('cannot append {} to [{}]'.format(version, self))
            elif version == current_end + 1:
                interval = [last_interval[0], version]
                return Timestamp(intervals=self.intervals[:-1] + [interval])
            else:
                return Timestamp(intervals=self.intervals + [version])
        else:
            return Timestamp(intervals=[version])

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
        if self.intervals:
            # Search from end of interval list since the majority of requests will
            # access more recent values (i.e., database versions).
            for index in range(len(self.intervals), 0, -1):
                interval = self.interval(index - 1)
                if interval[0] <= version <= interval[-1]:
                    return True
                elif interval[-1] < version:
                    return False
        return False

    def is_empty(self) -> bool:
        """Returns True if the timestamp is empty.

        Returns
        -------
        bool
        """
        return not self.intervals

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
                i1 = self.interval(i)
                i2 = timestamp.interval(i)
                if not i1[0] == i2[0] and not i1[-1] == i2[-1]:
                    return False
            return True
        return False

    def interval(self, index: int) -> List[int]:
        """Get the time interval at the given index position.

        Parameters
        ----------
        index: int
            Interval index position.

        Returns
        -------
        list of int
        """
        value = self.intervals[index]
        return value if isinstance(value, list) else [value]

    def last_version(self) -> int:
        """Get the last version in the timestamp. If the timestamp is empty the
        result is None.

        Returns
        -------
        int
        """
        return self.interval(-1)[-1] if self.intervals else None

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
        for ival in self.intervals:
            int_start, int_end = (ival[0], ival[-1]) if isinstance(ival, list) else (ival, ival)
            if int_start > version:
                # Intervals are sorted. If the current interval starts after
                # the rollback version there is nothing more to be added.
                break
            if int_end <= version:
                # Add interval since it ended before the rollback version.
                intervals.append([int_start, int_end] if int_start != int_end else int_start)
            else:
                # The rollback version falls inside the interval. Clip the
                # interval and add it to the new timestamp. All following
                # intervals can be ignored.
                intervals.append([int_start, version] if int_start != version else int_start)
                break
        return Timestamp(intervals=intervals)


class SingleVersion(Timestamp):
    """A timestamp containing a single version."""
    def __init__(self, version: int):
        """Initialize the list of time intervals.

        Parameters
        ----------
        version: int
            Version number that defines the timestamp as the only element in
            the time sequence.
        """
        return super(SingleVersion, self).__init__(intervals=[version])
