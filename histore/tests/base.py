# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper classes for unit tests."""


class ListReader(object):
    """Implementation of the reader interface. Returns values from a given list
    until the list is empty.
    """
    def __init__(self, values):
        """Initialize the list of returned values.

        Parameters
        ----------
        values: list
            List of returned values.
        """
        # Reverse the list of values to be able to remove values in original
        # order using pop()
        self.values = values[::-1]

    def has_next(self):
        """Returns True if the internal list is not empty (i.e., there are
        values left to be returned by the next() method).

        Returns
        -------
        bool
        """
        return self.values

    def next(self):
        """Return the next value in the list.

        Returns
        -------
        object
        """
        if self.has_next():
            return self.values.pop()
