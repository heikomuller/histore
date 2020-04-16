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

from histore.snapshot.reader import SnapshotReader


class Snapshot(object):
    """This class represents the root of a document tree. It contains a list
    of nodes which are the direct children of the tree root.

    Attributes
    ----------
    nodes: list(histore.document.node.Node)
        List of document root nodes.
    """
    def __init__(self, df, columns):
        """Initialize the list of root children. The document can either be
        initialized from a dictionary or using a given list of root children.

        Raises ValueError if both doc and nodes are not None.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame representing the snapshot of an archived datset.
        columns: list
            List of unique identifier corresponding to the columns in the
            data frame.

        Raises
        ------
        ValueError
        """
        self.df = df
        self.columns = columns
        # Create a list of (rowid, rowpos) pairs. Ensure that all row
        # identifier are integers. Split rows into those that have non-negative
        # identifier (assumed to be existing rows) and those that have negative
        # identifier (assumed to be new rows).
        pos_rows = list()
        neg_rows = list()
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
        # ascending row identifier and negative rows are appended at the end
        # sorted by ascending row position. Ensure that rows with non-negative
        # identifier are unique.
        pos_rows.sort(key=lambda r: r[0])
        for i in range(1, len(pos_rows)):
            if pos_rows[i - 1][0] >= pos_rows[i][0]:
                raise ValueError('non unique identifier %d and %d' % (
                    pos_rows[i - 1][0],
                    pos_rows[i][0]
                ))
        neg_rows.sort(key=lambda r: r[1])
        self.rows = pos_rows + neg_rows

    def reader(self):
        """Get reader for data frame rows ordered by their row identifier.

        Returns
        -------
        histore.snapshot.reader.SnapshotReader
        """
        return SnapshotReader(df=self.df, columns=self.columns, rows=self.rows)
