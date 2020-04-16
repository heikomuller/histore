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
