# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Functions for sorting CSV documents in main-memory or using secondary
storage. The functions in this module are inspired by csvsort
(https://github.com/richardpenman/csvsort).

For HISTORE we made a few modifications that allow us to handle CSV files that
can be sorted in main-memory directly instead of writing them to disk after
sorting.
"""

from tempfile import NamedTemporaryFile as TempFile
from typing import List, Optional, Tuple

import csv
import heapq
import os
import sys


from histore.document.base import Document
from histore.document.csv.base import CSVReader
from histore.document.mem.base import InMemoryDocument
from histore.document.reader import DocumentReader
from histore.document.row import DocumentRow
from histore.key.base import to_key

import histore.config as config


# -- Document class for sorted CSV files --------------------------------------

class SortedCSVDocument(Document):
    """CSV file containing a document that is sorted by one or more of the
    document columns.
    """
    def __init__(self, filename, columns, primary_key):
        """Initialize the object properties.

        Parameters
        ----------
        filename: string
            Path to the CSV file that contains the document.
        primary_key: list
            List of index positions for sort columns.
        """
        self.filename = filename
        self.columns = columns
        self.primary_key = primary_key

    def close(self):
        """Remove the sorted (temporary) file when merging is done."""
        os.remove(self.filename)

    def partial(self, reader):
        """If the document in the CSV file is a partial document we read it
        into a data frame and use the respective data frame document method.
        This requires that partial documents in CSV files fit into main memory.

        Parameters
        ----------
        reader: histore.archive.reader.RowPositionReader
            Reader for row (key, position) tuples from the original
            snapshot version.

        Returns
        -------
        histore.document.base.Document
        """
        rows = list()
        readorder = list()
        with open(self.filename, 'r', newline='') as f:
            csvreader = csv.reader(f)
            for row in csvreader:
                rows.append(row)
                key = rowkey(row, self.primary_key)
                pos = len(readorder)
                readorder.append((pos, key, pos))
        return InMemoryDocument(
            columns=self.columns,
            rows=rows,
            readorder=readorder
        ).partial(reader)

    def reader(self, schema):
        """Get reader for the CSV document.

        Parameters
        ----------
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the column list of this document (corresponding to
            their position in the list). The schema columns provide the unique
            column identifier that are required by the document reader to
            generate document rows.

        Returns
        -------
        histore.document.reader.DocumentReader
        """
        return SortedCSVDocumentReader(
            filename=self.filename,
            schema=schema,
            primary_key=self.primary_key
        )


class SortedCSVDocumentReader(DocumentReader):
    """Document reader for a sorted CSV document."""
    def __init__(self, filename, schema, primary_key):
        """Initialize the CSV reader and the list of column names.

        Parameters
        ----------
        reader: csv.reader
            Reader for the document file. If the file has a header it is
            assumed that the corresponding row has already been read.
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the document rows (based on list position). The schema
            columns provide the unique column identifier that are required to
            generate the returned document rows.
        primary_key: list
            List of index positions for sort columns.
        """
        self.fh = open(filename, 'r', newline='')
        self.reader = csv.reader(self.fh)
        self.schema = schema
        self.primary_key = primary_key
        self._next_row = None
        self._read_index = 0
        self.next()

    def has_next(self):  # pragma: no cover
        """Test if the reader has more rows to read. If this reader has more
        rows the internal row buffer is not empty.

        Returns
        -------
        bool
        """
        return self._next_row is not None

    def next(self):
        """Read the next row in the document. Returns None if the end of the
        document has been reached.

        Returns
        -------
        histore.document.row.DocumentRow
        """
        result = self._next_row
        try:
            row = next(self.reader)
            # If a next row was read ensure that the number of values
            # is the same as the number of columns in the schema.
            if len(row) != len(self.schema):
                self.fh.close()
                raise ValueError('invalid row %d' % (self.reader.line_num))
            # Create document row object for the read values.
            rowpos = self._read_index
            values = dict()
            for i, col in enumerate(self.schema):
                values[col.colid] = row[i]
            key = rowkey(row, self.primary_key)
            self._next_row = DocumentRow(
                key=key,
                pos=rowpos,
                values=values
            )
            self._read_index += 1
        except StopIteration:
            # Close the file if the file is reached.
            self._next_row = None
            self.fh.close()
        # Return the buffered result
        return result


# -- Helper methods for document classes --------------------------------------

def rowkey(row, primary_key):
    """Get the key value for a row.

    Parameters
    ----------
    row: list
        List of cells in a row.
    primary_key: list
        List of index positions for primary key columns.

    Returns
    -------
    scalar or tuple
    """
    if len(primary_key) == 1:
        return to_key(row[primary_key[0]])
    else:
        return tuple([to_key(row[c]) for c in primary_key])


# -- Helper methods for external merge sort -----------------------------------

def decorated_buffer(buffer, sortkey):
    """Iterator for in-memory buffer of data rows.

    Parameters
    ----------
    buffer: list
        Buffer of rows from the input file that were not written to file.
    sortkey: list
        List of index positions for sort columns.
    """
    buffer.sort(key=lambda row: keyvalue(row, sortkey))
    for row in buffer:
        yield keyvalue(row, sortkey), row


def decorated_csv(filename, columns):
    """Iterator for sorted CSV file block. From
    https://github.com/richardpenman/csvsort/blob/master/__init__.py

    Parameters
    ----------
    filename: string
        Temporary CSV file (block) on disk.
    columns: list
        List of index positions for sort columns.
    """
    with open(filename, 'r', newline='') as f:
        for row in csv.reader(f):
            yield keyvalue(row, columns), row


def keyvalue(row, columns):
    """Get the sort key for a given row. From
    https://github.com/richardpenman/csvsort/blob/master/__init__.py

    Parameters
    ----------
    row: list
        List of cell values in a CSV file row.
    columns: list
        List of index positions for sort columns.

    Returns
    -------
    list
    """
    return [row[column] for column in columns]


def mergesort(buffer, filenames, sortkey):
    """2-way merge sort for blocks of a CSV file. Adopted from
    https://github.com/richardpenman/csvsort/blob/master/__init__.py

    Returns name of file contaiing the sorted output.

    Parameters
    ----------
    buffer: list
        Buffer of rows from the input file that were not written to file.
    filename: list
        List of names for sorted temporary files that were generated by the
        CSV split method.
    sortkey: list
        List of index positions for sort columns.

    Returns
    -------
    string
    """
    # Merge the buffer if it contains rows.
    if buffer:
        mergefile = filenames[0]
        with TempFile(delete=False, mode='w', newline='') as f_out:
            writer = csv.writer(f_out)
            files = [
                decorated_buffer(buffer, sortkey),
                decorated_csv(mergefile, sortkey)
            ]
            for _, row in heapq.merge(*files):
                writer.writerow(row)
            filenames = filenames[1:] + [f_out.name]
        os.remove(mergefile)
    while len(filenames) > 1:
        mergefiles, filenames = filenames[:2], filenames[2:]
        with TempFile(delete=False, mode='w', newline='') as f_out:
            writer = csv.writer(f_out)
            files = [decorated_csv(f, sortkey) for f in mergefiles]
            for _, row in heapq.merge(*files):
                writer.writerow(row)
            filenames.append(f_out.name)
        for filename in mergefiles:
            os.remove(filename)
    return filenames[0]


def split(
    reader: CSVReader, sortkey: List[int], buffer_size: Optional[float] = None
) -> Tuple[List, List[str]]:
    """Split a CSV file into blocks of maximum size. Individual blocks are
    written to temporary files on disk. Only the final buffer is maintained in
    memory. Returns the memory buffer and the list of names for temporary files
    that are created. Adopted from
    https://github.com/richardpenman/csvsort/blob/master/__init__.py

    Returns a tuple of: main-memory buffer for data rows, and list of names for
    temporary files.

    Parameters
    ----------
    reader: histore.document.csv.base.CSVReader
        CSV file reader. It is assumed that any header row has been read, i.e,
        the next row in the reader is the first data row of the CSV file.
    sortkey: list
        List of index positions for sort columns.
    buffer_size: float, default=None
        Maximum size of CSV file blocks that are kept in main-memory. If no
        buffer size is given the value from the environment variable
        HISTORE_SORTBUFFER is used.

    Returns
    -------
    list(rows), list(filenames)
    """
    # Ensure that the buffer size is set.
    if buffer_size is None:
        buffer_size = config.SORTBUFFER()
    # Convert buffer size from MB to bytes.
    max_size = buffer_size * 1024 * 1024
    # Split CSV file rows into blocks
    buffer = list()
    current_size = 0
    tmp_filenames = list()
    for _, row in reader:
        buffer.append(row)
        current_size += sys.getsizeof(row)
        if current_size > max_size:
            # Sort buffer.
            buffer.sort(key=lambda row: keyvalue(row, sortkey))
            # Write buffer to disk.
            with TempFile(delete=False, mode='w', newline='') as f_out:
                writer = csv.writer(f_out)
                for r in buffer:
                    writer.writerow(r)
                # Append file name to result file name list.
                tmp_filenames.append(f_out.name)
            # Clear buffer.
            buffer = list()
            current_size = 0
    return buffer, tmp_filenames
