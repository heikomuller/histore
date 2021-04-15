# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Writer for CSV documents."""

import csv
import gzip

from typing import List, Optional

from histore.document.base import Scalar


class CSVWriter(object):
    """Context manager for an open CSV file writer."""
    def __init__(
        self, filename: str, delim: Optional[str] = None, compressed: Optional[bool] = None,
        encoding: Optional[str] = None, none_as: Optional[str] = None
    ):
        """Initialize the CSV writer.

        Parameters
        ----------
        filename: string
            Path to the output file.
        delim: string, default=None
            The column delimiter used in the CSV file.
        compressed: bool, default=None
            Flag indicating if the file contents are to be compressed using
            gzip.
        encoding: string, default=None
            The csv file encoding e.g. utf-8, utf-16 etc.
        none_as: string, default=None
            String that is used to encode None values in the output file. If
            given, all cell values that are None are substituted by the string.
        """
        # Open file depending on whether it is gzip compressed or not.
        if compressed:
            self.file = gzip.open(filename, 'wt', newline='')
        else:
            self.file = open(filename, 'w', newline='', encoding=encoding)
        # Open te CSV writer.
        self.writer = csv.writer(self.file, delimiter=delim if delim is not None else ',')
        self.none_as = none_as

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        self.close()
        return False

    def close(self):
        """Close the associated file handle and set it to None (to avoid
        repeated attempts to close an already closed file).
        """
        if self.file is not None:
            self.file.close()
            self.file = None

    def write(self, row: List[Scalar]):
        """Write a row to the CSV file.

        Parameters
        ----------
        row: list
            List of cell values for a dataset row.
        """
        if self.none_as is not None:
            row = [v if v is not None else self.none_as for v in row]
        return self.writer.writerow(row)
