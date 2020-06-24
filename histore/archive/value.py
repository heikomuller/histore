# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Values in a database archive. Represent cells in dataset rows as well as
information about the dataset schema (i.e., column name and position). Archived
values may either have multiple timestamped values, e.g., the different values
in a row cell over the history of the data set, or there is only one single
value, e.g., if the cell never changed over the history of the dataset.
"""

from abc import ABCMeta, abstractmethod

from histore.archive.provenance.value import UpdateValue
from histore.archive.timestamp import Timestamp


class ArchiveValue(metaclass=ABCMeta):
    """The archive value represents the history of a cell in a tabular dataset.
    """
    @abstractmethod
    def at_version(self, version, raise_error=True):  # pragma: no cover
        """Get cell value for the given version. Raises ValueError if the cell
        does not have a value for the given version and the raise error flag is
        set tot True. If the flag is false None is returned instead.

        Parameters
        ----------
        version: int
            Unique version identifier.
        raise_error: bool, default=True
            Flag that determines the behavior for cases where there is no value
            for the given version. If the flag is True an error is raised. If
            the flag is False None is returned.

        Returns
        -------
        scalar

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()

    def diff(self, original_version, new_version):
        """Get provenance information representing the difference for this
        value between the original version and a new version. If the value
        was the same for both versions the result is None.

        Parameters
        ----------
        original_version: int
            Unique identifier for the original version.
        new_version: int
            Unique identifier for the version that the original version is
            compared against.

        Returns
        -------
        histore.archive.provenance.value.UpdateValue
        """
        old_value = self.at_version(original_version, raise_error=False)
        new_value = self.at_version(new_version, raise_error=False)
        if old_value != new_value:
            return UpdateValue(old_value=old_value, new_value=new_value)
        # The value has not changed.
        return None

    @abstractmethod
    def extend(self, version, origin):  # pragma: no cover
        """Extend the timestamp of the value that was valid at the given source
        version with the new version identifier. If no value was valid at the
        given version of origin the value is returned unchanged.

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

    def is_multi_version(self):
        """Helper method to get the type of an archive value. Values can either
        be single version values or multi-version values.

        Returns
        -------
        bool
        """
        return not self.is_single_version()

    @abstractmethod
    def is_single_version(self):  # pragma: no cover
        """Helper method to get the type of an archive value. Values can either
        be single version values or multi-version values.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    @abstractmethod
    def merge(self, value, version):  # pragma: no cover
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


class SingleVersionValue(ArchiveValue):
    """Single archive value that has never changed over to history of the
    dataset that contains it.
    """
    def __init__(self, value, timestamp):
        """Initialize the archived value and the timestamp.

        Parameters
        ----------
        value: scalar
            Scalar value.
        timestamp: histore.archive.timestamp.Timestamp
            Timestamp of the archived value.
        """
        self.value = value
        self.timestamp = timestamp

    def __repr__(self):
        """Unambiguous string representation of the single version value.

        Returns
        -------
        string
        """
        return '({} [{}])'.format(self.value, str(self.timestamp))

    def at_version(self, version, raise_error=True):
        """Get value for the given version. If the given version is not
        included in the timestamp of the value an error is raised (if the raise
        error flag is True) or None is returned (otherwise).

        Parameters
        ----------
        version: int
            Unique version identifier.
        raise_error: bool, default=True
            Flag that determines the behavior for cases where there is no value
            for the given version. If the flag is True an error is raised. If
            the flag is False None is returned.

        Returns
        -------
        scalar

        Raises
        ------
        ValueError
        """
        if self.timestamp.contains(version):
            # Return value if it was present at the given version.
            return self.value
        elif raise_error:
            # Value was not present at the given version. Raise error if the
            # respective flag is True.
            raise ValueError('unknown version %d' % version)
        else:
            # Return None as default value for the given version.
            return None

    def extend(self, version, origin):
        """Extend the timestamp of the value with the given version identifier.

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
        # Return the object unchanged if the version of origin is not contained
        # in the current timestamp.
        if not self.timestamp.contains(origin):
            return self
        # Return a modified value that extends the validity of the timestamp
        # for the given version.
        return SingleVersionValue(
            value=self.value,
            timestamp=self.timestamp.append(version)
        )

    def is_single_version(self):
        """Return True for the single version value class.

        Returns
        -------
        bool
        """
        return True

    def merge(self, value, version):
        """Add value for the given version into the cell history. Depending on
        whether the value is different from previous cell value this either
        returns the current object itself with an extended timestamp or a new
        multi-valued object.

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
            # Extend the timestamp of the value.
            return SingleVersionValue(
                value=self.value,
                timestamp=self.timestamp.append(version)
            )
        # Return a multi-valued object.
        return MultiVersionValue(values=[
            TimestampedValue(value=self.value, timestamp=self.timestamp),
            TimestampedValue(value=value, timestamp=Timestamp(version=version))
        ])


class MultiVersionValue(ArchiveValue):
    """A multi-versiond archive value has multiple different values over the
    history of the dataset. Each value is associated with its own timestamp.
    """
    def __init__(self, values):
        """Initialize the list of timestamped values in the value history.

        Parameters
        ----------
        values: list(histore.archive.value.TimestampedValue)
            Values in the cell history.
        """
        self.values = values

    def __repr__(self):
        """Unambiguous string representation of the multi-versioned value.

        Returns
        -------
        string
        """
        return '({})'.format(', '.join(str(v) for v in self.values))

    def at_version(self, version, raise_error=True):
        """Get value for the given version. Raises ValueError if the value does
        not have an entry that was valid for the given version.

        Parameters
        ----------
        version: int
            Unique version identifier.
        raise_error: bool, default=True
            Flag that determines the behavior for cases where there is no value
            for the given version. If the flag is True an error is raised. If
            the flag is False None is returned.

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
        if raise_error:
            raise ValueError('unknown version %d' % version)
        return None

    def extend(self, version, origin):
        """Extend the timestamp of the value that was valid at the given source
        version with the new version identifier. If no value was valid at the
        given version of origin the value is returned unchanged.

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
                return MultiVersionValue(values=modified_values)
        # No value for the source version. Return the value unchanged.
        return self

    def is_single_version(self):
        """Return False for the single version value class.

        Returns
        -------
        bool
        """
        return False

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
        hisotre.archive.value.MultiVersionValue
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
        return MultiVersionValue(values=cell_history)


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
