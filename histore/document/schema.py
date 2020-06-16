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


class Column(str):
    """Columns in openclean data frames are subclasses of Python strings that
    contain a unique column identifier. This implementation is based on:
    https://bytes.com/topic/python/answers/32098-my-experiences-subclassing-string

    The order of creation is that the __new__ method is called which returns
    the object then __init__ is called.
    """
    def __new__(cls, colid, name, *args, **keywargs):
        """Initialize the String object with the given column name. Ignore the
        column identifier.

        Parameters
        ----------
        colid: int
            Unique column identifier
        name: string
            Column name
        """
        return str.__new__(cls, str(name))

    def __init__(self, colid, name):
        """Initialize the unique column identifier. The column name has already
        been initialized by the __new__ method that is called prior to the
        __init__ method.

        Parameters
        ----------
        colid: int
            Unique column identifier
        name: string
            Column name
        """
        self.colid = colid


# -- Helper methods -----------------------------------------------------------


def column_index(schema, columns):
    """Get the list of column index positions in a given schema (list of
    column names). Columns are either specified by name or by index position.

    The result is a list of column index positions.

    Raises errors if invalid columns positions or unknown column names are
    provided.

    Parameters
    ----------
    schema: list(string)
        List of column names.
    columns: list(int or str)
        List of column index positions or column names.

    Returns
    -------
    (list, list)

    Raises
    ------
    ValueError
    """
    # Ensure that columns is a list.
    columns = columns if isinstance(columns, list) else [columns]
    result = list()
    for col in columns:
        if isinstance(col, int):
            # Raise value error if the specified index is invalid
            if col >= len(schema):
                raise ValueError("invalid index position '%d'" % (col))
            colidx = col
        else:
            colidx = -1
            for i, name in enumerate(schema):
                if name == col:
                    colidx = i
                    break
            # Raise value error if the column name is unknown
            if colidx == -1:
                raise ValueError('unknown column name {}'.format(col))
        result.append(colidx)
    return result
