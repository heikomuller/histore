# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Rows in a dataset snapshot document. Each row has an identifier, the index
position of the row in the dataset, and a list of cell values for all dataset
columns.
"""

from histore.key.base import KeyValue


class DocumentRow(object):
    """Each row in snapshot document has a row identifier, an index position
    and the set of cell values for columns in the dataset snapshot. Row
    identifier may either be derived from a primary key column(s) or be an
    integer from the data frame row index. Row identifier may be negative (and
    not unique) indicating new rows for which unique identifier need to be
    generated.
    """
    def __init__(self, key, pos, values):
        """Initialize the row identifier, the index positions, and the cell
        values. Cell values are provided as a dictionary that maps column
        identifier (from the dataset archive schema) to cell values.

        Parameters
        ----------
        key: int, string, or tuple
            Derived row identifier.
        pos: int
            Index position for the row in the dataset.
        values: dict(int, scalar)
            Mapping of column identifier from the archive schema to cell values
            in the corresponding data frame row cells.
        """
        if isinstance(key, tuple):
            for k in key:
                assert isinstance(k, KeyValue)
        else:
            assert isinstance(key, KeyValue)
        self.key = key
        self.pos = pos
        self.values = values

    def __repr__(self):
        """Unambiguous string representation of the document row.

        Returns
        -------
        string
        """
        return '<DocumentRow(key={}, pos={}, values={})'.format(
            self.key,
            self.pos,
            self.values
        )
