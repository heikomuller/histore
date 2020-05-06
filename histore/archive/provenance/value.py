# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Provenance information representing updates to scalar values. This class is
used to represent changes in dataset cell values, row positions, column names,
and column positions.
"""


class UpdateValue(object):
    """Provenance object representing an updated scalar value. Maintains a pair
    of old value (before update) and new value (after update).
    """
    def __init__(self, old_value, new_value):
        """Initialize the object properties.

        Parameters
        ----------
        old_value: scalar
            Cell value before update.
        new_value: scalar
            Cell value after update.
        """
        self.old_value = old_value
        self.new_value = new_value

    def __repr__(self):
        """Unambiguous string representation of the value update.

        Returns
        -------
        string
        """
        return '{} TO {}'.format(self.old_value, self.new_value)

    def values(self):
        """Get tuple containing old and new value.

        Returns
        -------
        scalar, scalar
        """
        return self.old_value, self.new_value
