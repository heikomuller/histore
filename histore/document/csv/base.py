# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Collection of helper classes to read data frames from CSV files."""

from typing import List, Optional, Tuple

import os

from histore.document.base import DataRow, Document, DocumentIterator, RowIndex
from histore.document.csv.reader import CSVReader
from histore.document.csv.writer import CSVWriter
from histore.document.external import ExternalDocument
from histore.document.schema import DocumentSchema
from histore.document.sort import SortEngine


class CSVIterator(DocumentIterator):
    """Document iterator for CSV documents."""
    def __init__(
        self, filename: str, delim: Optional[str] = None, compressed: Optional[bool] = None,
        quotechar: Optional[str] = '"', encoding: Optional[str] = None,
        none_is: Optional[str] = None, skip_header: Optional[bool] = False
    ):
        """Initialize the input file.

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
        skip_header: bool, default=False
            Skip first row if the CSV file contains header information.
        """
        self.reader = CSVReader(
            filename=filename,
            delim=delim,
            compressed=compressed,
            quotechar=quotechar,
            encoding=encoding,
            none_is=none_is
        )
        # Skip the column name row.
        if skip_header:
            next(self.reader)
        # Initialize the row index.
        self._rowindex = 0

    def close(self):
        """Close the associated Json reader."""
        self.reader.close()

    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the document.

        Returns the row position, row index and the list of cell values for each
        of the document columns. Raises a StopIteration error if an attempt is
        made to read past the end of the document.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        # Get the row values.
        values = next(self.reader)
        # Increment the row counter.
        rowidx = self._rowindex
        self._rowindex += 1
        # Return row information.
        return rowidx, rowidx, values


class CSVFile(ExternalDocument):
    """Read and write documents that are stored as CSV files."""
    def __init__(
        self, filename: str, header: Optional[DocumentSchema] = None,
        delim: Optional[str] = None, compressed: Optional[bool] = None,
        quotechar: Optional[str] = '"', encoding: Optional[str] = None,
        none_is: Optional[str] = None, skip_lines: Optional[int] = None,
        delete_on_close: Optional[bool] = False
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
        delete_on_close: bool, default=False
            If True, delete the file when the document is closed.
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
        self.none_is = none_is
        # Read the header information from the first line of the input file if
        # no header is given.
        if header is None and os.path.isfile(filename):
            # Open the reader for the input file to get the list of column names
            # from the first row. If the file is empty the list of rows is empty.
            reader = CSVReader(
                filename=self.filename,
                delim=self.delim,
                compressed=self.compressed,
                quotechar=self.quotechar,
                encoding=self.encoding,
                none_is=self.none_is
            )
            try:
                header = next(reader)
            except StopIteration:
                # Empty file. The list of column nmaes is empty.
                header = []
            finally:
                reader.close()
        super(CSVFile, self).__init__(
            columns=header,
            filename=filename,
            delete_on_close=delete_on_close
        )

    def open(self) -> CSVIterator:
        """Get a row iterator for the associated CSV file.

        Returns
        -------
        histore.document.csv.base.CSVIterator
        """
        return CSVIterator(
            filename=self.filename,
            delim=self.delim,
            compressed=self.compressed,
            quotechar=self.quotechar,
            encoding=self.encoding,
            none_is=self.none_is,
            skip_header=self._has_header
        )

    def sorted(self, keys: List[int], buffersize: Optional[float] = None) -> Document:
        """Sort the document rows based on the values in the key columns.

        Key columns are specified by their index position. Returns a new
        document.

        Parameters
        ----------
        keys: list of int
            Index position of sort columns.
        buffersize: float, default=None
            Maximum size (in bytes) of file blocks that are kept in main-memory.

        Returns
        -------
        histore.document.base.Document
        """
        return SortEngine(buffersize=buffersize).sorted(doc=self, keys=keys)

    def writer(self) -> CSVWriter:
        """Get a CSV writer for the associated CSV file.

        Returns
        -------
        histore.document.csv.base.CSVWriter
        """
        return CSVWriter(
            filename=self.filename,
            delim=self.delim,
            compressed=self.compressed,
            encoding=self.encoding,
            none_as=self.none_is
        )
