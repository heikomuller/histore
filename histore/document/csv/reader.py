# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Reader for CSV files."""

from typing import Optional

import csv
import gzip

from histore.document.base import DataRow


class CSVReader(object):
    """Iterable reader for rows in a CSV file. Instances of this class act as
    an iterator for an open CSV file reader.
    """
    def __init__(
        self, filename: str, delim: Optional[str] = None, compressed: Optional[bool] = None,
        quotechar: Optional[str] = '"', encoding: Optional[str] = None,
        none_is: Optional[str] = None
    ):
        """Initialize the reader and the file handle.

        Parameters
        ----------
        filename: string
            Path to the input file.
        delim: string, default=None
            The column delimiter used in the CSV file.
        compressed: bool, default=None
            Flag indicating if the file contents are compressed using gzip.
        quotechar: string, default='"'
            CSV quote char.
        encoding: string, default=None
            The csv file encoding e.g. utf-8, utf-16 etc.
        none_is: string, default=None
            String that was used to encode None values in the input file. If
            given, all cell values that match the given string are substituted
            by None.
        """
        # Open file depending on whether it is gzip compressed or not.
        if compressed:
            self.file = gzip.open(filename, 'rt')
        else:
            self.file = open(filename, 'rt', encoding=encoding)
        # Open CSV reader.
        delim = delim if delim is not None else ','
        self.reader = csv.reader(self.file, delimiter=delim, quotechar=quotechar)
        self.none_is = none_is

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def close(self):
        """Close the associated file handle and set it to None (to avoid
        repeated attempts to close an already closed file).
        """
        if self.file is not None:
            self.file.close()
            self.file = None
            self.buffer = None

    def __next__(self) -> DataRow:
        """Read the next row in the document.

        Returns
        -------
        histore.document.base.DataRow
        """
        # Read the next line into the buffer before returning the current
        # buffer result. Make sure to catch StopIteration errors if the end
        # of the file is reached.
        try:
            row = next(self.reader)
            if self.none_is is not None:
                row = [v if v != self.none_is else None for v in row]
            return row
        except StopIteration:
            self.close()
            raise StopIteration()
