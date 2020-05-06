# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Snapshots are wrappers around data frames. A snapshot ensures that row
identifier for the data frame are either -1 or a unique integer.
"""

from abc import ABCMeta
import numpy as np

from histore.document.reader import DocumentReader


class Document(metaclass=ABCMeta):
    """The document class wraps a pandas data frame. The data frame represents
    a snapshot in the history of the archived dataset. The document class
    provides identifier for each row in the data frame. These identifier are
    either derived using a primary key (i.e., from the cell values in each row
    of the data frame) or from the row index of the data frame. There exist
    different implementations of the abstract document class for each case.
    """
    def __init__(self, df, schema, rows):
        """Initialize the object properties. The abstract document class
        maintains a list of row (identifier, position) tuples in the order
        in which rows are being returned by the document reader (i.e., sorted
        by their row identifier).

        Raises a ValueError if the list of columns in the schema does not match
        the number of columns in the data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame representing the snapshot of an archived datset.
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the data frame (based on list position). The schema
            columns provide the unique column identifier that are required by a
            document reader to generate document rows. An error is raised if
            the number of elements in the schema does not match the number of
            columns in the data frame.
        rows: list
            List of row (identifier, position) pairs. This list determines the
            order in which rows are returned by the document reader.

        Raises
        ------
        ValueError
        """
        # Raise an error if the number of elements in the schema does not match
        # the number of columns in the data frame.
        if len(df.columns) != len(schema):
            raise ValueError('invalid schema for data frame')
        self.df = df
        self.schema = schema
        self.rows = rows

    def reader(self):
        """Get reader for data frame rows ordered by their row identifier.

        Returns
        -------
        histore.document.reader.DocumentReader
        """
        return DocumentReader(df=self.df, schema=self.schema, rows=self.rows)


class PartialDocument(Document):
    """In a partial document the position information for rows in not aligned
    with the row positions in the full document. This class constructor aligns
    the row positions in the partial document with those for the full document
    given by a document index reader.

    The row positions in the original snapshot are used as the basis for
    alignment. That is, rows in the partial document will inherit the position
    of their counterpart in the snapshot document. New (unmatched) rows are
    appended at the end of the document.
    """
    def __init__(self, doc, row_index):
        """Align row positions in the partial document with row positions in
        the given row index reader.

        Parameters
        ----------
        doc: histore.document.base.Document
            Partial input document.
        row_index: histore.archive.reader.RowIndexReader
            Reader for row (identifier, position) tuples from the original
            snapshot version.
        """
        matched_rows, unmatched_rows = list(), list()
        rows = doc.rows
        readidx = 0
        orig_row = row_index.next()
        max_pos = -1
        while orig_row and readidx < len(rows):
            new_row = rows[readidx]
            if orig_row[0] < new_row[0]:
                # The original row is not included in the partial document.
                # Get information for next row in the original snapshot.
                orig_row = row_index.next()
            elif orig_row[0] > new_row[0]:
                # The row in the partial document is a new row that was not
                # present in the original document. Add the row identifier to
                # the list of unmatched rows.
                unmatched_rows.append(new_row[0])
                readidx += 1
            else:
                # Matched row. Adjust position of the row to the position of
                # the row in the original document.
                matched_rows.append(orig_row)
                orig_row = row_index.next()
                readidx += 1
            # Keep track of the maximum position in the original document.
            if orig_row and orig_row[1] > max_pos:
                max_pos = orig_row[1]
        # Finish reading the original document to get the maximum position
        # value.
        while orig_row:
            if orig_row[1] > max_pos:
                max_pos = orig_row[1]
            orig_row = row_index.next()
        # Append unmatched rows to the matched rows with adjusted row position.
        for key in unmatched_rows:
            max_pos += 1
            matched_rows.append((key, max_pos))
        # Append remaining rows in the partial document to the matched rows
        # list with adjusted position.
        while readidx < len(rows):
            max_pos += 1
            matched_rows.append((rows[readidx][0], max_pos))
            readidx += 1
        # Initialize the superclass with adjusted row positions.
        matched_rows.sort(key=lambda r: r[0])
        super(PartialDocument, self).__init__(
            df=doc.df,
            schema=doc.schema,
            rows=matched_rows
        )


# -- Documents for different row identifier types -----------------------------

class PKDocument(Document):
    """Snapshot document that derives row identifier from cell values in
    document rows using a primary key definition.
    """
    def __init__(self, df, schema, primary_key):
        """Initialize the object properties. Creates identifier for all rows
        in the document based on the given primary key.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame representing the snapshot of an archived datset.
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the data frame (based on list position). The schema
            columns provide the unique column identifier that are required by a
            document reader to generate document rows. An error is raised if
            the number of elements in the schema does not match the number of
            columns in the data frame.
        primary_key: string or list
            Name(s) of columns in the data frame schema from which the row keys
            are generated. If the primary key is not given the row index in the
            data frame is used as the row identifier. In the latter case all
            row identifier are expected to be integer or converted to integer.
            Negative values are assumed to represent new rows that will receive
            a new row identifier during merge.
        """
        # Generate row identifier from the column(s) in the primary key.
        if type(primary_key) not in [list, tuple]:
            # For convenience we ensure that we have a list of columns.
            primary_key = [primary_key]
        # For performance reasons we use two different implementations here
        # that distinguish between primary keys with a single column or with
        # multiple columns.
        rows = list()
        rowpos = 0
        if len(primary_key) == 1:
            keycol = primary_key[0]
            for _, values in df.iterrows():
                rows.append((values[keycol], rowpos))
                rowpos += 1
        else:
            for _, values in df.iterrows():
                rowid = tuple([values[c] for c in primary_key])
                rows.append((rowid, rowpos))
                rowpos += 1
        # Initialize the superclass.
        rows.sort(key=lambda r: r[0])
        super(PKDocument, self).__init__(df=df, schema=schema, rows=rows)


class RIDocument(Document):
    """Snapshot document that derives row identifier from the row index of the
    data frame. The identifier for new rows will be None.
    """
    def __init__(self, df, schema):
        """Initialize the object properties. Creates identifier for all rows
        in the document based on the data frame row index. Rows without an
        identifier are added at the end.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame representing the snapshot of an archived datset.
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the data frame (based on list position). The schema
            columns provide the unique column identifier that are required by a
            document reader to generate document rows. An error is raised if
            the number of elements in the schema does not match the number of
            columns in the data frame.
        """
        # Ensure that all row identifier are integers. Generate identifier for
        # those rows that have a non-integer index, None, or a negative value
        # as their index. These rows are considered new rows.
        rows, new_rows = list(), list()
        for pos in range(len(df.index)):
            rowid = df.index[pos]
            if not type(rowid) in [int, np.int64] or rowid < 0:
                new_rows.append((None, pos))
            else:
                rows.append((rowid, pos))
        # Initialize the superclass.
        rows = sorted(rows, key=lambda r: r[0]) + new_rows
        super(RIDocument, self).__init__(df=df, schema=schema, rows=rows)
