# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Iterator for data frame rows. Provides a reader that allows to iterate over
the rows in a data frame sorted by their row identifier.
"""

from histore.snapshot.row import SnapshotRow


class SnapshotReader(object):
    """Reader for rows in a data frame. Reads rows in order defined by the
    sorted row identifer.
    """
    def __init__(self, df, columns, rows):
        """Initialize the wrapped data frame, the mapping of data frame columns
        to archive schema columns, and the information about row identifier and
        their position in the data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame representing the dataset snapshot.
        columns: list
            List of identifier from the archive schema. The position of values
            in data frame rows corresponds to the identifier of their column in
            the archive.
        rows: list
            List of (rowid, rowpos). The list is sorted by row identifier.
        """
        self.df = df
        self.columns = columns
        self.rows = rows
        # Maintain the position of the current row for the reader
        self.read_index = 0

    def has_next(self):
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otehrwise, the next() method will return
        None.

        Returns
        -------
        bool
        """
        return self.read_index < len(self.rows)

    def next(self):
        """Read the next row in the data frame. Returns None if the end of the
        data frame rows has been reached.

        Returns
        -------
        histore.snapshot.row.SnapshotRow
        """
        # Return None if the reader has no more rows.
        if not self.has_next():
            return None
        # Create mapping for values in the data frame row to the columns in the
        # archive schema.
        values = dict()
        row = self.df.iloc[self.read_index]
        for i in range(len(self.columns)):
            values[self.columns[i]] = row[i]
        # Return a snapshot row for the data frame row. Ensure to advance the
        # read index.
        rowid, rowpos = self.rows[self.read_index]
        self.read_index += 1
        return SnapshotRow(identifier=rowid, pos=rowpos, values=values)
