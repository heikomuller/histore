# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Iterator for data frame rows. Provides a reader that allows to iterate over
the rows in a data frame sorted by their row identifier.
"""

import math

from histore.document.row import DocumentRow


class DocumentReader(object):
    """Reader for rows in a data frame. Reads rows in order defined by the
    sorted row identifer.
    """
    def __init__(self, df, schema, rows):
        """Initialize the wrapped data frame, the mapping of data frame columns
        to archive schema columns, and the information about row identifier and
        their position in the data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame representing the dataset snapshot.
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the data frame (based on list position). The schema
            columns provide the unique column identifier that are required to
            generate document rows.
        rows: list
            List of (rowid, rowpos). The list is sorted by row identifier.
        """
        self.df = df
        self.schema = schema
        self.rows = rows
        # Maintain the position of the current row for the reader
        self.read_index = 0

    def has_next(self):
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otherwise, the next() method will return
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
        histore.document.row.DocumentRow
        """
        # Return None if the reader has no more rows.
        if not self.has_next():
            return None
        # Get identifier and position in the data frame for the next row that
        # is being returned.
        rowid, rowpos = self.rows[self.read_index]
        self.read_index += 1
        # Create mapping for values in the data frame row to the columns in the
        # archive schema.
        values = dict()
        row = self.df.iloc[rowpos]
        for i in range(len(self.schema)):
            val = row[i]
            try:
                val = None if math.isnan(val) else val
            except TypeError:
                pass
            values[self.schema[i].colid] = val
        return DocumentRow(key=rowid, pos=rowpos, values=values)
