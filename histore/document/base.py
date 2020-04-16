# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Snapshots are wrappers around data frames. A snapshot ensures that row
identifier for the data frame are either -1 or a unique integer.
"""
import numpy as np

from histore.document.reader import DocumentReader


class Document(object):
    """The document class wraps a pandas data frame. The data frame represents
    a snapshot in the history of the archived dataset. The document class
    generates identifier for each ro in the data frame. These identifier are
    either derived form columns of the data frame (i.e., the primary key) or
    from the row index of the data frame.
    """
    def __init__(self, df, schema, primary_key=None):
        """Initialize the object properties. Generate a list of row identifier
        and position pairs that are used to sort data frame rows for merging.

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
        primary_key: string or list, default=None
            Name(s) of columns in the data frame schema from which the row keys
            are generated. If the primary key is not given the row index in the
            data frame is used as the row identifier. In the latter case all
            row identifier are expected to be integer or converted to integer.
            Negative values are assumed to represent new rows that will receive
            a new row identifier during merge.

        Raises
        ------
        ValueError
        """
        # Raise an error if the number of elements in the schema does not match
        # the number of column sin the data frame.
        if len(df.columns) != len(schema):
            raise ValueError('invalid schema for data frame')
        self.df = df
        self.schema = schema
        # Create a list of (rowid, rowpos) pairs. Depending on whether the
        # primary key column(s) are given or not the row identifier are either
        # generated from the data frame rows of the row index.
        if primary_key is None:
            # Ensure that all row
            # identifier are integers. Split rows into those that have non-
            # negative identifier (assumed to be existing rows) and those that
            # have negative identifier (assumed to be new rows).
            pos_rows, neg_rows = list(), list()
            for pos in range(len(df.index)):
                rowid = df.index[pos]
                if not type(rowid) in [int, np.int64]:
                    rowid = -1
                row = (rowid, pos)
                if rowid >= 0:
                    pos_rows.append(row)
                else:
                    neg_rows.append(row)
            # Create a sorted list of rows where existing rows are sorted by
            # ascending row identifier and negative rows are appended at the
            # end, sorted by ascending row position.
            pos_rows.sort(key=lambda r: r[0])
            neg_rows.sort(key=lambda r: r[1])
            self.rows = pos_rows + neg_rows
        else:
            # Generate row identifier from the column(s) in the promary key.
            if type(primary_key) not in [list, tuple]:
                # For convenience we ensure that we have a list of columns.
                primary_key = [primary_key]
            # For performance reasons we use two different implementations here
            # that distinsuish between primary keys with single columns or
            # multiple columns.
            self.rows = list()
            rowpos = 0
            if len(primary_key) == 1:
                keycol = primary_key[0]
                for _, values in df.iterrows():
                    self.rows.append((values[keycol], rowpos))
                    rowpos += 1
            else:
                for _, values in df.iterrows():
                    rowid = tuple([values[c] for c in primary_key])
                    self.rows.append((rowid, rowpos))
                    rowpos += 1
            # Sort rows by ascending key values.
            self.rows.sort(key=lambda r: r[0])

    def reader(self):
        """Get reader for data frame rows ordered by their row identifier.

        Returns
        -------
        histore.document.reader.DocumentReader
        """
        return DocumentReader(df=self.df, schema=self.schema, rows=self.rows)
