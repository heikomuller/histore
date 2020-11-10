# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Document wrapper around data frames."""

from histore.document.mem.base import InMemoryDocument

import histore.document.schema as schema
import histore.key.annotate as anno


class DataFrameDocument(InMemoryDocument):
    """Create an in-memory document for a pandas data frame."""
    def __init__(self, df, primary_key=None):
        """Initialize the document columns, rows, and read order. Depending on
        whether a primary key is given or not the document will be keyed by the
        key values or the data frame row index.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame that is being wrapped by the document class.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for snapshot rows.
        """
        columns = list(df.columns)
        rows = Rows(df=df)
        if primary_key is not None:
            readorder = anno.pk_readorder(
                rows=rows,
                primary_key=schema.column_index(
                    schema=columns,
                    columns=primary_key
                )
            )
        else:
            readorder = anno.rowindex_readorder(index=df.index)
        super(DataFrameDocument, self).__init__(
            columns=columns,
            rows=rows,
            readorder=readorder
        )


class Rows(object):
    """Iterable list for rows in a data frame."""
    def __init__(self, df):
        """Initialize the wrapped data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Data frame that is wrapped by this class.
        """
        self.df = df

    def __getitem__(self, key):
        """Get data frame row at the given position in the list.

        Parameters
        ----------
        key: int
            List index position for a data frame row.

        Returns
        -------
        pd.Series
        """
        return self.df.iloc[key]

    def __iter__(self):
        """Iterator for rows in the data frame.

        Returns
        -------
        iterator
        """
        return row_stream(self.df)


# -- Helper functions ---------------------------------------------------------

def row_stream(df):
    """Iterator over rows in a data frame.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas data frame.

    Returns
    -------
    pd.Series
    """
    for i in range(len(df.index)):
        yield df.iloc[i]
