# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Columns in histore data frames have a unique identifier and a column name.
The column class extends the Python String class to be able to be used as a
column value in a Pandas data frame.
"""

from typing import List, Optional, Tuple, Union

import pandas as pd


class Column(str):
    """Columns in histore data frames are subclasses of Python strings that
    contain a unique column identifier. This implementation is based on:
    https://bytes.com/topic/python/answers/32098-my-experiences-subclassing-string

    The order of creation is that the __new__ method is called which returns
    the object then __init__ is called.
    """
    def __new__(cls, colid: int, name: str, colidx: Optional[int] = None):
        """Initialize the String object with the given column name. Ignore the
        column identifier.

        Parameters
        ----------
        colid: int
            Unique column identifier
        name: string
            Column name
        colidx: int, default=None
            Index position of the column in a dataset schema.
        """
        return str.__new__(cls, str(name))

    def __init__(self, colid: int, name: str, colidx: Optional[int] = None):
        """Initialize the unique column identifier. The column name has already
        been initialized by the __new__ method that is called prior to the
        __init__ method.

        Parameters
        ----------
        colid: int
            Unique column identifier
        name: string
            Column name
        colidx: int, default=None
            Index position of the column in a dataset schema.
        """
        self.colid = colid
        self.colidx = colidx


"""Type alias for column lists."""
# Reference to a column in a dataset schema. Columns can be referenced by
# their name or index position in the dataset schema.
ColumnRef = Union[int, str]
# Reference to one or more columns in a dataset schema.
Columns = Union[ColumnRef, List[ColumnRef]]
# The schema of a dataset is a list of column names.
Schema = List[str]


# -- Helper methods -----------------------------------------------------------

def as_list(columns: Columns) -> List[Union[int, str, Column]]:
    """Ensure that the given columns argument is a list.

    Parameters
    ----------
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.

    Returns
    -------
    list
    """
    if isinstance(columns, pd.Index):
        return list(columns)
    elif isinstance(columns, tuple):
        return list(columns)
    elif not isinstance(columns, list):
        return [columns]
    else:
        return columns


def column_index(schema: Schema, columns: Columns):
    """Get the list of column index positions in a given schema (list of
    column names). Columns are either specified by name or by index position.
    The result is a list of column index positions.

    Raises errors if invalid columns positions or unknown column names are

    provided.
    Parameters
    ----------
    schema: list(string)
        List of column names.
    columns: int, str, or list(int or str)
        List of column index positions or column names.

    Returns
    -------
    (list, list)
    Raises
    ------
    ValueError
    """
    _, colidx = select_clause(schema=schema, columns=columns)
    return colidx


def column_ref(schema: Schema, column: ColumnRef) -> Tuple[str, int]:
    """Get the column name and index position for a referenced column in the
    given schema. Columns may be referenced by their name or index. This
    function returns both, the name and the index of the referenced column.

    Raises a ValueError if an unknown column is referenced.

    Parameters
    ----------
    schema: list of string
        List of column names in a dataset schema
    column: int, string, or Column
        Reference to a column in the dataset schema.

    Returns
    -------
    tuple(string, int)
    """
    if isinstance(column, int):
        # Raise value error if the specified index is invalid
        try:
            colname = schema[column]
            colidx = column
        except IndexError as ex:
            raise ValueError(ex)
    elif isinstance(column, Column) and column.colidx is not None:
        colidx = column.colidx
        if colidx < 0 or colidx >= len(schema):
            msg = 'invalid column index <{} {} {} />'
            raise ValueError(msg.format(column, column.colid, column.colidx))
        if schema[colidx] != column:
            msg = 'column name mismatch  <{} {} {} />'
            raise ValueError(msg.format(column, column.colid, column.colidx))
        colname = column
    elif isinstance(column, str):
        colidx = -1
        for i in range(len(schema)):
            if schema[i] == column:
                colname = schema[i]
                colidx = i
                break
        # Raise value error if the column name is unknown
        if colidx == -1:
            raise ValueError('unknown column name {}'.format(column))
    else:
        raise ValueError("invalid column reference '{}'".format(column))
    return colname, colidx


def select_clause(schema: Schema, columns: Columns) -> Tuple[List[str], List[int]]:
    """Get the list of column name objects and index positions in a data frame
    for list of columns that are specified either by name or by index position.

    The result is a tuple containing two lists: the list of column objects and
    the list of column index positions.

    Raises errors if invalid columns positions or unknown column names or types
    are provided.

    Parameters
    ----------
    schema: List of string
        List of columns in a dataset schema.
    columns: int, string or list of int or string
        Single column reference or a list of column index positions or column
        names.

    Returns
    -------
    (list, list)

    Raises
    ------
    ValueError
    """
    # Ensure that columns is a list.
    columns = as_list(columns)
    # Ensure that all elements in the selected column list are names.
    column_names = list()
    column_index = list()
    for col in columns:
        colname, colidx = column_ref(schema=schema, column=col)
        column_names.append(colname)
        column_index.append(colidx)
    return column_names, column_index
