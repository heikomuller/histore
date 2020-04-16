# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Values in a database archive. Represent cells in dataset rows. Values may
either have multiple timestamped values (i.e., the different values in a
row cell over the history of the data set) or there is only one (omnipresent)
value for a cell, if the cell never changed over the history of the dataset.
"""

from abc import ABCMeta, abstractmethod

from histore.archive.timestamp import Timestamp


class ArchiveValue(metaclass=ABCMeta):
    """The archive value represents the history of a cell in a tabular dataset.
    """
    @abstractmethod
    def at_version(self, version):
        """Get cell value for the given version. Raises ValueError if the cell
        does not have a value for the given version.

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        scalar

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()

    @abstractmethod
    def extend(self, version, origin):
        """Extend the timestamp of the value that was valid at the given source
        version with the new version identifier. Raises a ValueError if no
        value was valid at the given source version.

        Parameters
        ----------
        version: int
            Unique identifier of the new version.
        origin: int
            Unique identifier of the version for which the valid values
            timestamp is being extended.

        Returns
        -------
        histore.archive.value.ArchiveValue

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()

    @abstractmethod
    def is_timestamped(self):
        """Returns true if the value has a timestamp that differs from the
        parent.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def merge(self, value, version):
        """Add value for the given version into the cell history. Returns a
        modified copy of the value.

        Parameters
        ----------
        value: scalar
            Scalar cell value.
        version: int
            Unique version identifier.

        Returns
        -------
        hisotre.archive.value.ArchiveValue
        """
        raise NotImplementedError()


class OmnipresentCell(ArchiveValue):
    """A omnipresent cell value has never changed over to history of the
    dataset row that contains the cell. The value therefore inherits the
    timestamp from the row.
    """
    def __init__(self, value, timestamp):
        """Initialize the cell value and the reference to the timestamp of the
        containing row. the latter is required when adding new values to the
        cell history.

        Parameters
        ----------
        value: scalar
            Scalar cell value.
        timestamp: histore.archive.timestamp.Timestamp
            Timestamp of the dataset row.
        """
        self.value = value
        self.timestamp = timestamp

    def __repr__(self):
        """Unambiguous string representation of the omnipresent cell.

        Returns
        -------
        string
        """
        tsvalue = TimestampedValue(value=self.value, timestamp=self.timestamp)
        return str(tsvalue)

    def at_version(self, version):
        """Get cell value for the given version. Assumes that inclusion of the
        version in the timestamp of the value has been validated. Returns the
        associated value for any given version.

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        scalar

        Raises
        ------
        ValueError
        """
        return self.value

    def extend(self, version, origin):
        """Extend the timestamp of the value with the given version identifier.
        Assumes that inclusion of the source version in the timestamp of the
        value has been validated. Raises a ValueError if the new version is
        not one greater than the last version in the current timestamp.

        Parameters
        ----------
        version: int
            Unique identifier of the new version.
        origin: int
            Unique identifier of the version for which the valid values
            timestamp is being extended.

        Returns
        -------
        histore.archive.value.ArchiveValue

        Raises
        ------
        ValueError
        """
        # Expects a new version that is one greater than the last value of the
        # current timestamp.
        if version != self.timestamp.last_version() + 1:
            raise ValueError('cannot extend %s with %d' % (
                str(self.timestamp),
                version
            ))
        return OmnipresentCell(
            value=self.value,
            timestamp=self.timestamp.append(version)
        )

    def is_timestamped(self):
        """the onipresent cell is not timestamped.

        Returns
        -------
        bool
        """
        return False

    def merge(self, value, version):
        """Add value for the given version into the cell history. Depending on
        whether the value is different from previous cell values either returns
        the current object itself or a timestamped cell value.

        Parameters
        ----------
        value: scalar
            Scalar cell value.
        version: int
            Unique version identifier.

        Returns
        -------
        hisotre.archive.value.ArchiveValue
        """
        if self.value == value:
            # No need to make any changes but for the timestamp.
            return OmnipresentCell(
                value=self.value,
                timestamp=self.timestamp.append(version)
            )
        # Return a timestamped value
        return TimestampedCell(values=[
            TimestampedValue(value=self.value, timestamp=self.timestamp),
            TimestampedValue(value=value, timestamp=Timestamp(version=version))
        ])


class TimestampedCell(ArchiveValue):
    """A timestamped cell value has multiple different values over the history
    of the row that contains the cell. Each value is associated with its own
    timestamp.
    """
    def __init__(self, values):
        """Initialize the list of timestamped values in the cell history.

        Parameters
        ----------
        values: list(histore.archive.value.TimestampedValue)
            Values in the cell history.
        """
        self.values = values

    def __repr__(self):
        """Unambiguous string representation of the timestamped cell.

        Returns
        -------
        string
        """
        return ','.join(str(v) for v in self.values)

    def at_version(self, version):
        """Get cell value for the given version. Raises ValueError if the cell
        does not have a value for the given version.

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        scalar

        Raises
        ------
        ValueError
        """
        for val in self.values:
            if val.timestamp.contains(version):
                return val.value
        # No value for the version.
        raise ValueError('unknown version %d' % version)

    def extend(self, version, origin):
        """Extend the timestamp of the value that was valid at the given source
        version with the new version identifier. Raises a ValueError if no
        value was valid at the given source version.

        Parameters
        ----------
        version: int
            Unique identifier of the new version.
        origin: int
            Unique identifier of the version for which the valid values
            timestamp is being extended.

        Returns
        -------
        histore.archive.value.ArchiveValue

        Raises
        ------
        ValueError
        """
        modified_values = list(self.values)
        for i in range(len(modified_values)):
            val = modified_values[i]
            if val.timestamp.contains(origin):
                # Found the value that was valid at the source version. Modify
                # the associated timestamp and return immediately.
                modified_values[i] = val.append(version)
                return TimestampedCell(values=modified_values)
        # No value for the source version.
        raise ValueError('unknown version %d' % version)

    def is_timestamped(self):
        """Returns True for the timestamped cell value.

        Returns
        -------
        bool
        """
        return True

    def merge(self, value, version):
        """Add value for the given version into the cell history. Returns a
        modified copy of the value.

        Parameters
        ----------
        value: scalar
            Scalar cell value.
        version: int
            Unique version identifier.

        Returns
        -------
        hisotre.archive.value.TimestampedCell
        """
        # Create modified list of values in the cell history.
        cell_history = list()
        # Check if the value existed in the history of the cell. If a match is
        # found the timestamp of the value is adjusted accordingly.
        existed = False
        for val in self.values:
            if val.value == value:
                existed = True
                val = val.append(version)
            cell_history.append(val)
        # If the value did not exist in any prior version we need to add it to
        # the cell history.
        if not existed:
            ts = Timestamp(version=version)
            cell_history.append(TimestampedValue(value=value, timestamp=ts))
        return TimestampedCell(values=cell_history)


class TimestampedValue:
    """The timestamped value is a wrapper around a scalar cell value and the
    associated timestamp.
    """
    def __init__(self, value, timestamp):
        """Initialize the object properties.

        Parameters
        ----------
        value: scalar
            Scalar cell value.
        timestamp: histore.archive.timestamp.Timestamp
            Sequence of version numbers when the value was present in a dataset
            row cell.
        """
        self.value = value
        self.timestamp = timestamp

    def __repr__(self):
        """Unambiguous string representation of the timestamped value.

        Returns
        -------
        string
        """
        return '{} [{}]'.format(self.value, str(self.timestamp))

    def append(self, version):
        """Return a modified copy if the timestamped value where the given
        version has been appended to to timestamp.

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        histore.archive.value.TimestampedValue
        """
        return TimestampedValue(
            value=self.value,
            timestamp=self.timestamp.append(version)
        )
