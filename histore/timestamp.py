# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Timestamps are sequences of snapshot (version) identifier that define when
a node was part of the archived document.
"""

class TimeInterval(object):
    """Interval of integer numbers. Defines that start and end of the interval.
    Ensures that start is lower or equal that end.
    """
    def __init__(self, start, end=None):
        """Initialize the interval from a given integer pair. Ensures that
        start is lower or equal than end (if given). If end is None it is
        assumed to be equal to start

        Parameters
        ----------
        start: int
        end: int, optional
        """
        self.start = start
        if not end is None:
            if end < start:
                raise ValueError('invalid interval \'[' + str(start) + ',' + str(end) + ']\'')
            self.end = end
        else:
            self.end = start

    def __repr__(self):
        """Unambiguous string representation of this time interval.

        Returns
        -------
        string
        """
        return '[%i,%i]' % (self.start, self.end)

    def __str__(self):
        """Readable string representation of this time interval.

        Returns
        -------
        string
        """
        if self.start < self.end:
            return '%i-%i' % (self.start, self.end)
        else:
            return str(self.start)

    def contains(self, interval):
        """Returns true if this interval contains the given interval.

        Parameters
        ----------
        interval: histore.timestamp.TimeInterval

        Returns
        -------
        bool
        """
        return (self.start <= interval.start) and (self.end >= interval.end)

    def overlap(self, interval):

    	if (self.start == interval.start) or (self.start == interval.end) or (self.end == interval.start) or (self.end == interval.end):
            return True;

    	if self.start < interval.start:
    	    return self.end > interval.start
    	elif self.start > interval.start:
    	    return interval.end > self.start
    	else:
            return True;


class Timestamp(object):
    """A timestamp is a sequence of integer numbers represented as a list of
    integer intervals.
    """
    def __init__(self, intervals=None):
        """Initialize the timestamp using an list of time intervals. Ensures
        that intervals are not adjacent or overlapping.

        Parameters
        ----------
        intervals: list(histore.timestamp.TimeInterval)
        """
        # Validate the given list of intervala
        if not intervals is None:
            if len(intervals) > 0:
                for i in range(1, len(intervals)):
                    if intervals[i - 1].end >= intervals[i].start:
                        raise ValueError('ajacent or overlapping intervals \'' + str(intervals[i - 1]) + ' and ' + str(intervals[i]) + '\'')
            self.intervals = intervals
        else:
            self.intervals = list()

    def append(self, value):
        """Append the given value to the timestamp. Returns a new timestamp.

        Raises ValueError if the given value is lower or equal to the last value
        in the sequence.

        Parameters
        ----------
        value: int

        Returns
        -------
        histore.timestamp.Timestamp
        """
        if len(self.intervals) > 0:
            last_val = self.intervals[-1].end
    	    if last_val >= value:
                raise ValueError('attempt to append value \'' + str(value) + '\' to timestamp ending at \'' + str(last_val) + '\'');
    	    elif value == (last_val + 1):
                intervals = list(self.intervals[:-1])
                intervals.append(TimeInterval(start=self.intervals[-1].start, end=value))
                return Timestamp(intervals)
    	    else:
                intervals = list(self.intervals)
                intervals.append(TimeInterval(value))
                return Timestamp(intervals)
        else:
            return Timestamp([TimeInterval(value)])

    def is_empty(self):
        """Returns True if the timestamp is empty.

        Returns
        -------
        bool
        """
        return len(self.intervals) == 0

    def is_subset_of(self, timestamp):
        """Returns true if this timestamp is a true subset of the given
        timestamp.

        Parameters
        ----------
        timestamp: histore.timestamp.Timestamp

        Returns
        -------
        bool
        """
        idxI = 0
        idxJ = 0

        is_equal = True

        while idxI < len(timestamp.intervals) and idxJ < len(self.intervals):
            intervalI = timestamp.intervals[idxI]
            intervalJ = self.intervals[idxJ]
            if intervalI.end < intervalJ.start:
                idxI += 1
            elif intervalI.start > intervalJ.start:
                return False;
            elif intervalI.end < intervalJ.end:
                return False
            else:
                if intervalI.start != intervalJ.start or intervalI.end != intervalJ.end:
                    is_equal = False
                idxJ += 1

        return idxJ == timestamp.intervals and not is_equal;
