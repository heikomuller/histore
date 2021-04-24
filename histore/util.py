# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper methods for timestamp management."""

from abc import ABCMeta, abstractmethod
from datetime import datetime
from dateutil.parser import isoparse
from dateutil.tz import UTC
from importlib import import_module
from typing import IO, Iterator, List, Optional, Union

import errno
import gzip
import os
import shutil
import uuid


# -- CLI ----------------------------------------------------------------------

def get_delimiter(delimiter):
    """Get delimiter. Replace encodings for tabulator with tab character.

    Parameters
    ----------
    delimiter: string
        One-character used to separate fields.

    Returns
    -------
    string
    """
    if delimiter is None:
        return ','
    if delimiter.lower() in ['tab', '\\t']:
        return '\t'
    return delimiter


# -- Datetime -----------------------------------------------------------------

def current_time() -> str:
    """Get ISO format string for the current time.

    Returns
    -------
    str
    """
    return utc_now().isoformat()


def to_datetime(timestamp: str) -> datetime:
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
    class_name = import_path[pos + 1:]
    module = import_module(module_name)
    return getattr(module, class_name)


# -- I/O ----------------------------------------------------------------------

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
    def write(self, line):  # pragma: no cover
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

    def write(self, line):
        """Write sting line to output file.

        Parameters
        ----------
        line: string
            Output line that is bein written.
        """
        self.f.write(str.encode(line, 'utf8'))


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

    def write(self, line):
        """Write sting line to output file.

        Parameters
        ----------
        line: string
            Output line that is bein written.
        """
        self.f.write(line)


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


def inputstream(filename: Union[str, IO], compression: Optional[str] = None) -> Iterator[str]:
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
        f = open(filename, 'r') if isinstance(filename, str) else filename

        def file_iterator(f: IO):
            """Turn an open file handle into an iterator."""
            for line in f:
                yield line

        return file_iterator(f)
    elif compression == 'gzip':
        f = gzip.open(filename, 'rb')

        def file_iterator(f: IO):
            """Turn an open file handle into an iterator."""
            for line in f:
                yield line.decode('utf8')

        return file_iterator(f)

    raise ValueError('unknown compression mode {}'.format(compression))


def outputstream(filename, compression: Optional[str] = None) -> IOStream:
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
        return PlainTextFile(open(filename, 'w') if isinstance(filename, str) else filename)
    elif compression == 'gzip':
        return GZipFile(gzip.open(filename, 'wb'))
    raise ValueError('unknown compression mode {}'.format(compression))


# -- Sorting ------------------------------------------------------------------

def keyvalue(row: List, columns: List[int]):
    """Get the sort key for a given row. From
    https://github.com/richardpenman/csvsort/blob/master/__init__.py

    Parameters
    ----------
    row: list
        List of cell values in a document row.
    columns: list
        List of index positions for sort columns.

    Returns
    -------
    list
    """
    return [row[column] for column in columns]


# -- Unique identifier --------------------------------------------------------

def get_unique_identifier():
    """Create a new unique identifier.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')
