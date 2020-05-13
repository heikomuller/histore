# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Base classes for row key values."""

from abc import ABCMeta
from functools import total_ordering

import numpy as np


class KeyValue(metaclass=ABCMeta):
    """Abstract base class for key values. We currently distinguish four types
    of key values: numeric, string, None, and new rows. The distinction between
    None and new rows is of importance for documents that are keyed by a list
    of numeric row index values. In that case, non-numeric or negative values
    are considered new rows that are assigned row identifier during the merge
    process.

    Supclasses primarily implement comparison operators for sorting. The
    general sort order is: numeric, string, null, new
    """
    def __init__(self, value=None):
        """Initialize the actual key value.

        Parameters
        ----------
        value: scalar, default=None
            This is the actual key value for a row.
        """
        self.value = value

    def is_new(self):
        """Test if the key represents a new row.

        Returns
        -------
        bool
        """
        return isinstance(self, NewRow)

    def is_null(self):
        """Test if the key represents a null value (None).

        Returns
        -------
        bool
        """
        return isinstance(self, NullKey)

    def is_number(self):
        """Test if the key is a numeric value.

        Returns
        -------
        bool
        """
        return isinstance(self, NumberKey)

    def is_string(self):
        """Test if the key is a string value.

        Returns
        -------
        bool
        """
        return isinstance(self, StringKey)


@total_ordering
class NewRow(KeyValue):
    """Key for new document rows in documents that are keyed by the row index.
    Having a dedicated object for these type of keys allows to distinguish
    betweeen primary key values that are None and the new rows.
    """
    def __init__(self, identifier=None):
        """Initialize the optional identifier. This is primarily used for test
        purposes.

        Parameters
        ----------
        identifier: scalar
            Key identifier used for test purposes.
        """
        self.identifier = identifier

    def __eq__(self, other):
        """Objects of this class are equal to any other object of the same
        class.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        bool
        """
        return other.is_new()

    def __lt__(self, other):
        """Objects of this class will always be greater than any other key
        value.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        int
        """
        return False

    def __repr__(self):
        """String representation for the key value.

        Returns
        -------
        string
        """
        return '<NewRow ({})>'.format(self.identifier)


@total_ordering
class NullKey(KeyValue):
    """Object for key values that are None."""
    def __init__(self, identifier=None):
        """Initialize the optional identifier. This is primarily used for test
        purposes.

        Parameters
        ----------
        identifier: scalar
            Key identifier used for test purposes.
        """
        self.identifier = identifier

    def __eq__(self, other):
        """Objects of this class are equal to any other object of the same
        class.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        bool
        """
        return other.is_null()

    def __lt__(self, other):
        """Objects of this class will only be lower than new row keys.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        int
        """
        return True if other.is_new() else False

    def __repr__(self):
        """String representation for the key value.

        Returns
        -------
        string
        """
        return '<Null ({})>'.format(self.identifier)


@total_ordering
class NumberKey(KeyValue):
    """Object for numeric key values."""
    def __init__(self, value):
        """Initialize the actual key value.

        Parameters
        ----------
        value: number
            Numeric key value.
        """
        super(NumberKey, self).__init__(value=value)

    def __eq__(self, other):
        """Use the build-in equality comparison operator when comparing to
        other numeric key values.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        bool
        """
        return self.value == other.value if other.is_number() else False

    def __lt__(self, other):
        """Objects of this class will be lower than any other key type. When
        comparing to a numeric key, use the build in operator to compare the
        key values.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        bool
        """
        return self.value < other.value if other.is_number() else True

    def __repr__(self):
        """String representation for the key value.

        Returns
        -------
        string
        """
        return str(self.value)


@total_ordering
class StringKey(KeyValue):
    """Object for string key values."""
    def __init__(self, value):
        """Initialize the actual key value.

        Parameters
        ----------
        value: string
            String key value.
        """
        super(StringKey, self).__init__(value=value)

    def __eq__(self, other):
        """Use the build-in equality comparison operator when comparing to
        other string key values.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        bool
        """
        return self.value == other.value if other.is_string() else False

    def __lt__(self, other):
        """Objects of this class will be lower than null keys and new row keys.
        They are greater than numeric keys. When comparing to a string key, use
        the build in operator to compare the key values.

        Parameters
        ----------
        other: histore.key.base.KeyValue
            Object that this key is compared against.

        Returns
        -------
        bool
        """
        if other.is_number():
            return False
        elif other.is_string():
            return self.value < other.value
        else:
            return True

    def __repr__(self):
        """String representation for the key value.

        Returns
        -------
        string
        """
        return self.value


# -- Helper Functions ---------------------------------------------------------

def to_key(value):
    """Create an instance of the key value classes for a given scalar value.

    Parameters
    ----------
    value: scalar
        Scalar key value.

    Returns
    -------
    histore.key.base.KeyValue
    """
    if value is None:
        return NullKey()
    elif isinstance(value, KeyValue):
        return value
    elif isinstance(value, int) or isinstance(value, np.integer):
        return NumberKey(value=value)
    elif isinstance(value, float) or isinstance(value, np.floating):
        return NumberKey(value=value)
    elif isinstance(value, str):
        return StringKey(value=value)
    else:
        # By default, use a string representation of the value.
        return StringKey(value=str(value))
