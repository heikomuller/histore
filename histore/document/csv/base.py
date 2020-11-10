# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Collection of helper classes to read data frames from CSV files."""

from typing import Optional

import csv
import gzip
import pandas as pd

from histore.document.base import DataIterator, DataReader
from histore.document.schema import Column, Schema


# -- CSV Reader and Writer ----------------------------------------------------

class CSVReader(DataIterator):
    """Iterable reader for rows in a CSV file. Instances of this class act as a
    context manager for an open CSV file reader.
    """
    def __init__(
        self, reader: csv.reader, file: int, skip_header: bool,
        none_is: Optional[str] = None
    ):
        """Initialize the reader and the file handle.

        Parameters
        ----------
        reader: csv.reader
            Reader for the data rows in the streamed CSV file.
        file: File object
            Handle for the open CSV file.
        skip_header: bool
            Skip first row if True.
        none_is: string, default=None
            String that was used to encode None values in the input file. If
            given, all cell values that match the given string are substituted
            by None.
        """
        self.reader = reader
        self.file = file
        self.none_is = none_is
        # Initialize the rowid counter and read the first line from the file
        # if no list of columns is passed to the object at instantiation.
        self.rowid = 0
        # Read the first row if the skip_header flag is True.
        if skip_header:
            next(reader)

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        self.close()
        return False

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def __next__(self):
        """Return next row from CSV reader. The CSV reader will raise a
        StopIteration error when the end of the file is reached.
        """
        row = next(self.reader)
        if self.none_is is not None:
            row = [v if v != self.none_is else None for v in row]
        rowid = self.rowid
        self.rowid += 1
        return rowid, row

    def close(self):
        """Close the associated file handle and set it to None (to avoid
        repeated attempts to close an already closed file).
        """
        if self.file is not None:
            self.file.close()
            self.file = None


class CSVWriter(object):
    """Context manager for an open CSV file writer."""
    def __init__(self, writer, file, none_as: Optional[str] = None):
        """Initialize the writer and the file handle.

        Parameters
        ----------
        writer: csv.writer
            Writer for a streamed CSV file.
        file: File object
            Handle for the open CSV file.
        none_as: string, default=None
            String that is used to encode None values in the output file. If
            given, all cell values that are None are substituted by the string.
        """
        self.writer = writer
        self.file = file
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

    def write(self, row):
        """Write a row to the CSV file."""
        if self.none_as is not None:
            row = [v if v is not None else self.none_as for v in row]
        return self.writer.writerow(row)


# -- CSV file wrapper ---------------------------------------------------------

class CSVFile(DataReader):
    """Helper class to open a CSV reader for a given data file."""
    def __init__(
        self, filename: str, header: Optional[Schema] = None,
        delim: Optional[str] = None, compressed: Optional[bool] = None,
        quotechar: Optional[str] = '"', encoding: Optional[str] = None,
        none_is: Optional[str] = None, skip_lines: Optional[int] = None,
        write: Optional[bool] = False
    ):
        """Set the file name, delimiter and the flag that indicates if the file
        is compressed using gzip.

        If the file is opened for writing and no header is given no attempt is
        made to reader the header from file.

        Parameters
        ----------
        filename: string
            Path to the CSV file that is being read.
        header: list of string, default=None
            Optional header. If no header is given it is assumed that the first
            row in the CSV file contains the header information.
        delim: string, default=None
            The column delimiter used in the CSV file.
        compressed: bool, default=None
            Flag indicating if the file contents have been compressed using
            gzip.
        encoding: string, default=None
            The csv file encoding e.g. utf-8, utf-16 etc.
        none_is: string, default=None
            String that was used to encode None values in the input file. If
            given, all cell values that match the given string are substituted
            by None.
        skip_lines: int, default=None
            Skip the first n lines.
        write: bool, default=False
            Indicate if the file is opened for writing.
        """
        self.filename = filename
        self.quotechar = quotechar
        self.none_is = none_is
        self.skip_lines = skip_lines
        # Keep track if the header information was None intially, i.e., if the
        # header is in the file or not.
        self._has_header = header is None
        # Infer the delimiter from the filename if not given. Files that have
        # '.tsv' in their name are expscted to be tab-dlimited. All other files
        # are expected to be standard CSV files. In the future we may want to
        # use a sniffer here.
        if delim is None:
            if filename.endswith('.tsv') or filename.endswith('.tsv.gz'):
                delim = '\t'
            else:
                delim = ','
        self.delim = delim
        # Infer compression if gzip is not set. Files that end with '.gz' are
        # assumed to be compressed using gzip.
        if compressed is None:
            compressed = filename.endswith('.gz')
        self.compressed = compressed
        self.encoding = encoding
        # Read the header information from the first line of the input file if
        # no header is given.
        if header is None and not write:
            reader, csvfile = self._openreader()
            try:
                header = list()
                for name in next(reader):
                    cid = len(header)
                    header.append(Column(colid=cid, name=name, colidx=cid))
            finally:
                csvfile.close()
        super(CSVFile, self).__init__(columns=header)

    def open(self) -> CSVReader:
        """Get a CSV reader for the associated CSV file.

        Returns
        -------
        histore.document.csv.base.CSVReader
        """
        reader, csvfile = self._openreader()
        return CSVReader(
            reader=reader,
            file=csvfile,
            skip_header=self._has_header,
            none_is=self.none_is
        )

    def _openreader(self):
        """Open the associated CSV file. Returns a file handle for the opened
        file.

        Returns
        -------
        csv.reader, file object
        """
        # Open file depending on whether it is gzip compressed or not.
        if self.compressed:
            csvfile = gzip.open(self.filename, 'rt')
        else:
            csvfile = open(self.filename, 'r', encoding=self.encoding)
        reader = csv.reader(csvfile, delimiter=self.delim, quotechar=self.quotechar)
        if self.skip_lines:
            # Skip the first n lines. Will raise an error if EOF is reached.
            for i in range(self.skip_lines):
                next(reader)
        return reader, csvfile

    def read_df(self) -> pd.DataFrame:
        """Read CSV fiel as pandas DataFrame.

        Returns
        -------
        pd.DataFrame
        """
        data, index = list(), list()
        with self.open() as reader:
            for rowid, row in reader:
                index.append(rowid)
                data.append(row)
        return pd.DataFrame(data=data, index=index, columns=self.columns)

    def write(
        self, header: Optional[Schema] = None, none_as: Optional[str] = None
    ) -> CSVWriter:
        """Get a CSV writer for the associated CSV file. Writes the dataset
        header information to the opened output file.

        Parameters
        ----------
        header: list of string, default=None
            Optional header information that overwrites the header information
            in the file.
        none_as: string, default=None
            String that is used to encode None values in the output file. If
            given, all cell values that are None are substituted by the string.

        Returns
        -------
        histore.document.csv.base.CSVWriter
        """
        # Open file depending on whether it is gzip compressed or not.
        if self.compressed:
            csvfile = gzip.open(self.filename, 'wt', newline='')
        else:
            csvfile = open(self.filename, 'w', newline='', encoding=self.encoding)
        # Open te CSV writer and output the dataset header.
        writer = csv.writer(csvfile, delimiter=self.delim)
        if header is not None:
            writer.writerow(header)
        elif self.columns is not None:
            writer.writerow(self.columns)
        return CSVWriter(writer=writer, file=csvfile, none_as=none_as)
