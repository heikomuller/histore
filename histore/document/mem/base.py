# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of the document interface for documents that have been read
completely into main memory.
"""

from histore.document.base import Document
from histore.document.mem.reader import InMemoryDocumentReader


class InMemoryDocument(Document):
    """The in-memory document maintains a array of document rows (each as a
    list of cell values) and a sorted row read order as a list of pairs of row
    key and original row position in the document.  These row keys are either
    derived using a primary key (i.e., from the cell values in each row) or
    from the row index of a data frame.
    """
    def __init__(self, columns, rows, readorder):
        """Initialize the object properties. The abstract document class
        maintains a list of row (identifier, position) tuples in the order
        in which rows are being returned by the document reader (i.e., sorted
        by their row key).

        Parameters
        ----------
        columns: list
            List of column names. The number of values in each row is expected
            to be the same as the number of columns and the order of values in
            each row is expected to correspond to their respective column in
            this list.
        rows: list
            List of document rows. Each row in the list is an array of the same
            length as the number of columns in the document.
        readorder: list
            The read order of a document is a sorted list of 3-tuples with
            (list index, key, position). The list is sorted by the row key. The
            list index is the index of a row in the given row list. The key is
            the unique key value for the associated row. The position is the
            position of the row in the document. The list index and the
            position may differ for a row, especially for partial documents.
        """
        super(InMemoryDocument, self).__init__(columns=columns)
        self.rows = rows
        self.readorder = readorder

    def close(self):
        """There are no resources that need to be released by the in-memory
        document.
        """
        pass

    def partial(self, reader):
        """Return a copy of the document where the rows are aligned with the
        position of the corresponding row in the document origin. The original
        row order is accessible via the given row position reader.

        Parameters
        ----------
        reader: histore.archive.reader.RowPositionReader
            Reader for row (key, position) tuples from the original
            snapshot version.

        Returns
        -------
        histore.document.base.Document
        """
        matched_rows, unmatched_rows = list(), list()
        readidx = 0
        orig_row = reader.next()
        max_pos = -1
        while orig_row and readidx < len(self.readorder):
            rowidx, key, pos = self.readorder[readidx]
            if orig_row[0] < key:
                # The original row is not included in the partial document.
                # Get information for next row in the original snapshot.
                orig_row = reader.next()
            elif orig_row[0] > key:
                # The row in the partial document is a new row that was not
                # present in the original document. Add the row identifier to
                # the list of unmatched rows.
                unmatched_rows.append((rowidx, key))
                readidx += 1
            else:
                # Matched row. Adjust position of the row to the position of
                # the row in the original document.
                matched_rows.append((rowidx, key, orig_row[1]))
                orig_row = reader.next()
                readidx += 1
            # Keep track of the maximum position in the original document.
            if orig_row and orig_row[1] > max_pos:
                max_pos = orig_row[1]
        # Finish reading the original document to get the maximum position
        # value.
        while orig_row:
            if orig_row[1] > max_pos:
                max_pos = orig_row[1]
            orig_row = reader.next()
        # Append unmatched rows to the matched rows with adjusted row position.
        for rowidx, key in unmatched_rows:
            max_pos += 1
            matched_rows.append((rowidx, key, max_pos))
        # Append remaining rows in the partial document to the matched rows
        # list with adjusted position.
        while readidx < len(self.readorder):
            max_pos += 1
            rowidx, key, pos = self.readorder[readidx]
            matched_rows.append((rowidx, key, max_pos))
            readidx += 1
        # Ensure that all document rows have been matched (currently only done
        # by comparing the length of two lists). This is a sanity check in case
        # of a bug in the above code.
        if len(self.readorder) != len(matched_rows):  # pragma: no cover
            raise RuntimeError('did not match all rows')
        # Initialize the superclass with adjusted row positions.
        matched_rows.sort(key=lambda r: r[1])
        return InMemoryDocument(
            columns=self.columns,
            rows=self.rows,
            readorder=matched_rows
        )

    def reader(self, schema):
        """Get reader for document rows ordered by their row key.

        Parameters
        ----------
        schema: list(histore.document.schema.Column)
            List of columns in the document schema. Each column corresponds to
            a column in the column list of this document (corresponding to
            their position in the list). The schema columns provide the unique
            column identifier that are required by the document reader to
            generate document rows. An error is raised if the number of
            elements in the schema does not match the number of columns in the
            data frame.

        Returns
        -------
        histore.document.mem.reader.InMemoryDocumentReader

        Raises
        ------
        ValueError
        """
        # Raise an error if the number of elements in the schema does not match
        # the number of columns in the document.
        if len(self.columns) != len(schema):
            raise ValueError('invalid schema for data frame')
        return InMemoryDocumentReader(
            schema=schema,
            rows=self.rows,
            readorder=self.readorder
        )
