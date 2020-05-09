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
