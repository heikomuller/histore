# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Reader for document rows that are accessible in an array in main memory."""

import math

from histore.document.reader import DocumentReader
from histore.document.row import DocumentRow


class InMemoryDocumentReader(DocumentReader):
    """Reader for rows in an array. Reads rows in order defined by the given
    read order.
    """
    def __init__(self, schema, rows, readorder):
        """Initialize the wrapped data frame, the mapping of data frame columns
        to archive schema columns, and the information about row identifier and
        their position in the data frame.

        Parameters
        ----------
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the document rows (based on list position). The schema
            columns provide the unique column identifier that are required to
            generate the returned document rows.
        rows: list
            List of document rows. Each row in the list is an array of the same
            length as the number of columns in the schema.
        readorder: list
            The read order of a document is a sorted list of 3-tuples with
            (list index, key, position). The list is sorted by the row key.
        """
        self.schema = schema
        self.rows = rows
        self.readorder = readorder
        # Maintain the position of the current row for the reader
        self.readindex = 0

    def has_next(self):
        """Test if the reader has more rows to read. If True the next() method
        will return the next row. Otherwise, the next() method will return
        None.

        Returns
        -------
        bool
        """
        return self.readindex < len(self.readorder)

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
        rowidx, key, pos = self.readorder[self.readindex]
        self.readindex += 1
        # Create mapping for values in the data frame row to the columns in the
        # archive schema.
        values = dict()
        row = self.rows[rowidx]
        for i in range(len(self.schema)):
            val = row[i]
            try:
                val = None if math.isnan(val) else val
            except TypeError:
                pass
            values[self.schema[i].colid] = val
        return DocumentRow(key=key, pos=pos, values=values)
