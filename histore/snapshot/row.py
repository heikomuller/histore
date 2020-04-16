# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Rows in a snapshot dataset. Each row has an identifier, the index position
of the row in the dataset, and a list of cell values for all dataset columns.
"""


class SnapshotRow(object):
    """Each row in snapshot has a row identifier, an index position and the
    set of cell values for columns in the dataset snapshot. Row identifier may
    be negative (and not unique) indicating new rows for which unique
    identifier need to be generated.
    """
    def __init__(self, identifier, pos, values):
        """Initialize the row identifier, the index positions, and the cell
        values. Cell values are provided as a dictionary that maps column
        identifier (from the dataset archive schema) to cell values.

        Parameters
        ----------
        identifier: int
            Row identifier (may be negative).
        pos: int
            Index position for the row in the dataset.
        values: dict(int, scalar)
            Mapping of column identifier from the archive schema to cell values
            in the corresponding data frame row cells.
        """
        self.identifier = identifier
        self.pos = pos
        self.values = values
