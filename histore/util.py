# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper methods for timestamp management."""

from abc import ABCMeta, abstractmethod
from datetime import datetime, timezone

import errno
import gzip
import os


# -- Datetime -----------------------------------------------------------------

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


def to_localtime(ts):
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


# -- I/O ----------------------------------------------------------------------

def createdir(directory, abs=False):
    """Safely create the given directory path if it does not exist.

    Parameters
    ----------
    directory: string
        Path to directory that is being created.
    abs: boolean, optional
        Return absolute path if true

    Returns
    -------
    string
    """
    # Based on https://stackoverflow.com/questions/273192/
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    if abs:
        return os.path.abspath(directory)
    else:
        return directory


def inputstream(filename, compression=None):
    """Open the given file for input. The compression mide string determines
    which compression algorithm is being used (or no compression if None).

    Parameters
    ----------
    compression: string, default=None
        String representing the compression mode for the output file.

    Returns
    -------
    histore.util.IOStream
    """
    if compression is None:
        return PlainTextFile(open(filename, 'r'))
    elif compression == 'gzip':
        return GZipFile(gzip.open(filename, 'rb'))
    raise ValueError('unknown compression mode {}'.format(compression))


def outputstream(filename, compression=None):
    """Open the given file for output. The compression mide string determines
    which compression algorithm is being used (or no compression if None).

    Parameters
    ----------
    compression: string, default=None
        String representing the compression mode for the output file.

    Returns
    -------
    histore.util.IOStream
    """
    if compression is None:
        return PlainTextFile(open(filename, 'w'))
    elif compression == 'gzip':
        return GZipFile(gzip.open(filename, 'wb'))
    raise ValueError('unknown compression mode {}'.format(compression))


class IOStream(metaclass=ABCMeta):
    def __init__(self, f):
        """Initialize the IO stream.

        Parameters
        ----------
        f: FileObject
            Open file object.
        """
        self.f = f

    def close(self):
        """Close the IO Stream."""
        self.f.close()

    @abstractmethod
    def readline(self):
        """Read sting line from input file. The returned line is stripped of
        any leading or trailing whitespace characters.

        Returns
        ----------
        string
        """
        raise NotImplementedError()

    @abstractmethod
    def writeline(self, line):
        """Write sting line to output file.

        Parameters
        ----------
        line: string
            Output line that is bein written.
        """
        raise NotImplementedError()


class GZipFile(IOStream):
    """IO stream wrapper for gzip compressed files."""
    def __init__(self, f):
        """Initialize the IO stream.

        Parameters
        ----------
        f: FileObject
            Open file object.
        """
        super(GZipFile, self).__init__(f)

    def readline(self):
        """Read sting line from input file. The returned line is stripped of
        any leading or trailing whitespace characters.

        Returns
        ----------
        string
        """
        return self.f.readline().decode('utf8').strip()

    def writeline(self, line):
        """Write sting line to output file.

        Parameters
        ----------
        line: string
            Output line that is bein written.
        """
        self.f.write(str.encode(line))
        self.f.write(b'\n')


class PlainTextFile(IOStream):
    """IO stream for plain text files."""
    def __init__(self, f):
        """Initialize the IO stream.

        Parameters
        ----------
        f: FileObject
            Open file object.
        """
        super(PlainTextFile, self).__init__(f)

    def readline(self):
        """Read sting line from input file. The returned line is stripped of
        any leading or trailing whitespace characters.

        Returns
        ----------
        string
        """
        return self.f.readline().strip()

    def writeline(self, line):
        """Write sting line to output file.

        Parameters
        ----------
        line: string
            Output line that is bein written.
        """
        self.f.write(line)
        self.f.write('\n')