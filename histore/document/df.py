# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Document wrapper around data frames."""

from __future__ import annotations
from typing import Any, List, Optional, Tuple

import math
import pandas as pd

from histore.document.base import DataRow, Document, DocumentIterator, RowIndex


class DataFrameIterator(DocumentIterator):
    """Document iterator for data frames."""
    def __init__(self, df: pd.DataFrame, readorder: Optional[List[int]] = None):
        """Initialize the data frame and the row read order.

        If no read order is given the read order is determined by the sorted
        data frame index.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame that is being wrapped by the document class.
        readorder: list of int, default=None
            Order of row positions when iterating through the dataset.
        """
        self._df = df
        self.readorder = readorder if readorder is not None else rowindex_readorder(df)
        self._readindex = 0

    def close(self):
        """Set the associated data frame and read order to None."""
        self._df = None
        self.readorder = None

    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the document.

        Returns the row position, row index and the list of cell values for each
        of the document columns. Raises a StopIteration error if an attempt is
        made to read past the end of the document.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        rowpos = self._readindex
        try:
            origpos = self.readorder[rowpos]
        except IndexError:
            # Raise error if the end of the data frame has been reached.
            raise StopIteration()
        rowidx = self._df.index[origpos]
        # By definition, a row with an index that is None or has a negative
        # value will be treated as a new row.
        if rowidx is None or rowidx < 0:
            rowidx = -1
        values = [nan_to_none(v) for v in self._df.iloc[origpos]]
        self._readindex += 1
        return origpos, rowidx, values


class DataFrameDocument(Document):
    """Create an in-memory document for a pandas data frame."""
    def __init__(self, df: pd.DataFrame, readorder: Optional[List[int]] = None):
        """Initialize the data frame that represents the document.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame that is being wrapped by the document class.
        readorder: list of int, default=None
            Order of row positions when iterating through the dataset.
        """
        super(DataFrameDocument, self).__init__(columns=list(df.columns))
        self._df = df
        self.readorder = readorder

    def close(self):
        """Set the internal reference to the data frame to None when the
        document is closed.
        """
        self._df = None

    def open(self) -> DataFrameIterator:
        """Open the document to get a iterator for the rows in the document.

        Returns
        -------
        histore.document.df.DataFrameIterator
        """
        return DataFrameIterator(df=self._df, readorder=self.readorder)

    def to_df(self) -> pd.DataFrame:
        """Return the associated data frame.

        Returns
        -------
        pd.DataFrame
        """
        return self._df

    def sorted(self, keys: List[int], buffersize: Optional[float] = None) -> DataFrameDocument:
        """Sort the document rows based on the values in the key columns.

        Key columns are specified by their index position. Returns a new
        document.

        Parameters
        ----------
        keys: list of int
            Index position of sort columns.
        buffersize: float, default=None
            Maximum size (in bytes) of file blocks that are kept in main-memory.
            Ignored. Included for API completeness.

        Returns
        -------
        histore.document.df.DataFrameDocument
        """
        rows = list()
        if len(keys) == 1:
            ridx = keys[0]
            for row in self._df.itertuples(index=False, name=None):
                rows.append((len(rows), row[ridx]))
        else:
            for row in self._df.itertuples(index=False, name=None):
                rows.append((len(rows), tuple(row[k] for k in keys)))
        return DataFrameDocument(
            df=self._df,
            readorder=[pos for pos, _ in sorted(rows, key=lambda x: x[1])]
        )


# -- Helper Functions ---------------------------------------------------------

def nan_to_none(value: Any) -> Any:
    """Convert NaN values to None.

    Parameters
    ----------
    value: any
        Cell value.

    Returns
    -------
    any
    """
    if isinstance(value, int) or isinstance(value, float):
        return None if math.isnan(value) else value
    return value


def rowindex_readorder(df: pd.DataFrame) -> List[int]:
    """Get read order for rows in a pandas data frame based on their index.

    Sorts the rows based on the index and returns a list with initial row
    positions in sorted order. Index values that are None or -1 are considered
    new rows that are moved to the end of the sorted list.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.

    Returns
    -------
    list of int
    """
    keyed_rows, new_rows = list(), list()
    for pos, index in zip(range(len(df.index)), df.index):
        if index is None or index == -1:
            new_rows.append(pos)
        else:
            keyed_rows.append((pos, index))
    return [pos for pos, _ in sorted(keyed_rows, key=lambda x: x[1])] + new_rows
