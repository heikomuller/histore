# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Snapshots are wrappers around data frames. A snapshot ensures that row
identifier for the data frame are either -1 or a unique integer.
"""


class DataFrameRows(object):
    def __init__(self, df):
        self.df = df

    def __getitem__(self, key):
        return self.df.iloc[key]

    def __iter__(self):
        return multi_column_stream(self.df)


def multi_column_stream(df):
    """Iterator over values in multiple columns in a data frame.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas data frame.

    Returns
    -------
    tuple
    """
    for _, values in df.iterrows():
        yield values
