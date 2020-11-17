# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper methods for timestamp management."""

from abc import ABCMeta, abstractmethod
from datetime import datetime
from dateutil.parser import isoparse
from dateutil.tz import UTC
from importlib import import_module

import errno
import gzip
import os
import shutil
import uuid


# -- Datetime -----------------------------------------------------------------

def current_time() -> str:
    """Get ISO format string for the current time.

    Returns
    -------
    str
    """
    return utc_now().isoformat()


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
    if isinstance(timestamp, datetime):
        return timestamp
    try:
        dt = isoparse(timestamp)
        if not dt.tzinfo == UTC:
            dt = dt.astimezone(UTC)
        return dt
    except ValueError:
        raise ValueError('unknown time format {}'.format(timestamp))


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
    return datetime.now(UTC)


# -- Dynamic import ----------------------------------------------------------

def import_obj(import_path):
    """Import an object (function or class) from a given package path.

    Parameters
    ----------
    import_path: string
        Full package target path for the imported object. Assumes that path
        components are separated by '.'.

    Returns
    -------
    any
    """
    pos = import_path.rfind('.')
    module_name = import_path[:pos]
    class_name = import_path[pos+1:]
    module = import_module(module_name)
    return getattr(module, class_name)


# -- I/O ----------------------------------------------------------------------

def cleardir(directory):
    """Remove all files in the given directory.

    Parameters
    ----------
    directory: string
        Path to directory that is being created.
    """
    for filename in os.listdir(directory):
        file = os.path.join(directory, filename)
        if os.path.isfile(file) or os.path.islink(file):
            os.unlink(file)
        else:
            shutil.rmtree(file)


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
        except OSError as e:  # pragma: no cover
            if e.errno != errno.EEXIST:
                raise
    if abs:
        return os.path.abspath(directory)
    else:
        return directory


def inputstream(filename, compression=None):
    """Open the given file for input. The compression mode string determines
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
    """Open the given file for output. The compression mode string determines
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
    def readline(self):  # pragma: no cover
        """Read sting line from input file. The returned line is stripped of
        any leading or trailing whitespace characters.

        Returns
        ----------
        string
        """
        raise NotImplementedError()

    @abstractmethod
    def writeline(self, line):  # pragma: no cover
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
        self.f.write(str.encode(line, 'utf8'))
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


# -- Unique identifier --------------------------------------------------------

def get_unique_identifier():
    """Create a new unique identifier.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')
