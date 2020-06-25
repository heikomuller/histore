# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Classes to maintain information about dataset snapshots in an archive."""

import histore.util as util


class Snapshot(object):
    """Descriptor for archive snapshots. Contains all the meta-data about a
    single snapshot in a dataset archive.
    """
    def __init__(
        self, version, valid_time, transaction_time=None, description=None
    ):
        """Initialize the snapshot meta-data.

        Parameters
        ----------
        version: int
            Unique version identifier for the snapshot.
        valid_time: datetime.datetime
            Timestamp when the snapshot was first valid. A snapshot is valid
            until the valid time of the next snapshot in the archive.
        transaction_time: datetime.datetime, default=None
            Timestamp when the snapshot was inserted into the archive. If the
            timestamp is not given the current time is used.
        description: string, default=None
            Optional user-provided description for the snapshot.
        """
        self.version = version
        self.valid_time = valid_time
        if transaction_time is None:
            transaction_time = util.utc_now()
        self.transaction_time = transaction_time
        self.description = description if description is not None else ''

    def __repr__(self):
        """Unambiguous string representation of the archive snapshot
        descriptor.

        Returns
        -------
        string
        """
        return "<Snapshot (version={} description='{}' at={})>".format(
            self.version,
            self.description,
            str(util.to_localtime(self.transaction_time))
        )

    @property
    def created_at(self):
        """Shortcut for transaction timestamp.

        Returns
        -------
        datetime.datetime
        """
        return self.transaction_time


class SnapshotListing(object):
    """Listing of snapshot descriptors for snapshots in an archive. Ensures
    that the order of snapshots reflects the order of their version numbers
    and valid times.
    """
    def __init__(self, snapshots=None):
        """Initialize the list of snapshot descriptors. Raises a ValueError if
        the order of snapshots in the list does not reflect the order of their
        version numbers and valid times.

        Parameters
        ----------
        snapshots: histore.archive.snapshot.Snapshot, default=None
            List of snapshots descriptors for snapshots in an archive.

        Raises
        ------
        ValueError
        """
        self.snapshots = snapshots if snapshots is not None else list()
        # Validate the order of version numbers and valid times.
        if len(self.snapshots) > 1:
            prev = self.snapshots[0]
            for i in range(1, len(self.snapshots)):
                s = self.snapshots[i]
                if s.version <= prev.version:
                    msg = 'invalid version sequence{} ->{}'
                    raise ValueError(msg.format(prev.version, s.version))
                if s.valid_time < prev.valid_time:
                    msg = 'invalid time sequence {} -> {}'
                    raise ValueError(msg.format(prev.valid_time, s.valid_time))
                prev = s

    def __iter__(self):
        """Make the snapshot list iterable.

        Returns
        -------
        iterator
        """
        return iter(self.snapshots)

    def __len__(self):
        """Get number of snapshots in the listing.

        Returns
        -------
        int
        """
        return len(self.snapshots)

    def append(self, valid_time=None, description=None):
        """Add a new version to the given listing. This will return a modified
        version listing with the new snapshot as the last element.

        The snapshot version will be one greater that the last version. If the
        snapshot list is empty the first version will be zero. If the valid
        time is not given the transaction time is used as valid time.

        Raises a valueError if the new snapshot violates the order of valid
        times in the list.

        Parameters
        ----------
        valid_time: datetime.datetime, default=None
            Timestamp when the snapshot was first valid. A snapshot is valid
            until the valid time of the next snapshot in the archive.
        description: string, default=None
            Optional user-provided description for the snapshot.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing

        Raises
        ------
        ValueError
        """
        if not self.snapshots:
            version = 0
        else:
            version = self.snapshots[-1].version + 1
        transaction_time = util.utc_now()
        valid_time = valid_time if valid_time is not None else transaction_time
        s = Snapshot(
            version=version,
            valid_time=valid_time,
            transaction_time=transaction_time,
            description=description
        )
        return SnapshotListing(snapshots=self.snapshots + [s])

    def at_time(self, ts):
        """Get the snapshot that was valid at the given time. A snapshot is
        considered valid starting at its valit_time until the next timestamp.
        The last snapshot in the list is considered valid until infinity.

        Returns None if the given timestamp is before the valid time of the
        first snapshot.

        Parameters
        ----------
        ts: datetime.datetime
            Timestamp in UTC time.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        if not self.snapshots:
            # Return None if there are no snapshots yet.
            return None
        # Ensure that the first snapshot was valid at or before the given
        # timestamp.
        s1 = self.snapshots[0]
        if ts < s1.valid_time:
            # There is no snapshot at the given valid time.
            return None
        # Find the snapshot that was valid after the given timestamp and return
        # the previous snapshot.
        for i in range(1, len(self.snapshots)):
            s2 = self.snapshots[i]
            if ts < s2.valid_time:
                return s1
            s1 = s2
        # The last snapshot was valid at or before the given timestamp.
        return s1

    def has_version(self, version):
        """Check if the given version identifier references an existing
        snapshot in the listing.

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        bool
        """
        for s in self.snapshots:
            if s.version == version:
                return True
        return False

    def is_empty(self):
        """Shortcut to test if the list of snapshots is empty.

        Returns
        -------
        bool
        """
        return not self.snapshots

    def last_snapshot(self):
        """Shortcut to get the last snapshot in the list. The result is None if
        the listing is empty.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        if not self.snapshots:
            return None
        return self.snapshots[-1]
