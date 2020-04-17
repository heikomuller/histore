# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper methods for timestamp management."""

from datetime import datetime, timezone


"""List of recognized time formats."""
TIMEFORMAT = [
    '%Y-%m-%d',
    '%Y-%m-%dT%H:%M:%S.%f%z',
    '%Y-%m-%dT%H:%M:%S.%f',
    '%Y-%m-%dT%H:%M:%S'
]


def to_datetime(timestamp):
    """Converts a timestamp string in ISO format into a datatime object in
    UTC timezone.

    Parameters
    ----------
    timstamp : string
        Timestamp in ISO format

    Returns
    -------
    datetime.datetime
        Datetime object
    """
    for format in TIMEFORMAT:
        try:
            dt = datetime.strptime(timestamp, format)
            if not dt.tzinfo == timezone.utc:
                dt = dt.astimezone(timezone.utc)
            return dt
        except ValueError:
            pass
    ValueError('unknown time format')


def to_localtime(self, ts):
    """Conbert timestamp in UTC timezone to local time.

    Parameters
    ----------
     ts: datetime.datetime
        Timestamp in UTC format

    Returns
    -------
    datetime.datetime
    """
    return ts.astimezone()


def utc_now():
    """Get the current time in UTC timezone.

    Returns
    -------
    datetime.datetime
    """
    return datetime.now(timezone.utc)
