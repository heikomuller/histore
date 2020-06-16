# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Base function to read CSV files as HISTORE documents."""

import csv
import gzip

from histore.document.csv.simple import SimpleCSVDocument
from histore.document.csv.sort import SortedCSVDocument
from histore.document.mem.base import InMemoryDocument

import histore.document.csv.sort as sort
import histore.document.schema as schema
import histore.key.annotate as anno


def read_csv(
    filename, delimiter=',', quotechar='"', compression=None, has_header=True,
    columns=None, primary_key=None, max_size=None
):
    """Read a document from a csv file. If the primary key attributes are given
    the returned document is sorted by the primary key values. Otherwise, the
    original order of rows is kept and each row is assigned a unique index that
    is equal to the row position. Sorts the document by the primary key values
    (if given).

    Parameters
    ----------
    filename: string
        Path to the CSV file that is being read.
    delimiter: string, default=','
        A one-character string used to separate fields.
    quotechar: string, default='"'
        A one-character string used to quote fields containing special
        characters, such as the delimiter or quotechar, or which contain
        new-line characters.
    compression: string, default=None
        String identifier for the file compression format. Will apply to the
        input file and the output file. Currently only 'gzip' compression is
        supported.
    has_header: bool, default=True
        Flag indicating whether the first row of the given file contains the
        column name header.
    columns: list(string), default=None
        List of column names to use. Overrides the names of columns in the file
        if the file has a header.
    primary_key: string or list, default=None
        Column(s) that are used to generate identifier for snapshot rows. The
        columns may be identified by their name or index position. If a string
        in the primary key list refers to a non-unique column name in the file,
        a ValueError is raised.
    max_size: float, default=None
        Maximum size (in MB) of the main-memory buffer for blocks of the CSV
        file that are sorted in main-memory.

    Returns
    -------
    histore.document.base.Document

    Raises
    ------
    ValueError
    """
    # Open the file. Ensure that a valid compression identifier is given.
    if compression is not None and compression != 'gzip':
        raise ValueError("invalid compression format '{}'".format(compression))
    # Raise an error if has_header is False and no list of column names is
    # given. If names are given they have to be all strings.
    if not has_header and columns is None:
        raise ValueError('no column information')
    elif columns is not None:
        if not isinstance(columns, list):
            columns = [columns]
        for col in columns:
            if not isinstance(col, str):
                raise ValueError("invalid column name '{}'".format(col))
    # Open the input file.
    f = open(filename, 'r') if not compression else gzip.open(filename, 'rt')
    try:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
        if has_header:
            # Read the header and use the read names as columns (only if no
            # separate list of columns were given).
            header = next(reader)
            if columns is None:
                # Use the header as the list of columns if the list of columns
                # is currently undefined.
                columns = header
        # Create document instance depending on whether a primary key was given
        # or not.
        if primary_key is not None:
            # If a primary key is given we first need to get the index position
            # for the key attributes in the document schema and then sort the
            # input file.
            pk = schema.column_index(schema=columns, columns=primary_key)
            buffer, filenames = sort.split(
                reader=reader,
                sortkey=pk,
                buffer_size=max_size
            )
            if not filenames:
                # If the file fits into main-memory return a sorted in-memory
                # document.
                return InMemoryDocument(
                    columns=columns,
                    rows=buffer,
                    readorder=anno.pk_readorder(rows=buffer, primary_key=pk)
                )
            else:
                # Merge the CSV file blocks and return a sorted CSV document
                # object that wrapps the sorted CSV file.
                return SortedCSVDocument(
                    filename=sort.mergesort(
                        buffer=buffer,
                        filenames=filenames,
                        sortkey=pk
                    ),
                    columns=columns,
                    primary_key=pk
                )
        else:
            # In this case we do not need to sort the document. The document
            # will always be read in original order. Return a file reader for
            # the csv file.
            return SimpleCSVDocument(
                filename=filename,
                columns=columns,
                has_header=has_header,
                delimiter=delimiter,
                quotechar=quotechar,
                compression=compression
            )
    finally:
        # Ensure that the file is colsed.
        f.close()
