# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Snapshots represent individual versions of an evolving dataset."""

import datetime


class Snapshot(object):
    """Each snapshot has a unique version number, a timestamp of creation (in
    UTC time), and an optional name.
    """
    def __init__(self, version, created_at=None, name=None, archive=None):
        """Initialize the snapshot. If the ctreated_at timestamp is None the
        current time will be used. If the name is None a string representation
        of the created_at timestamp is used. The archive object allows to
        retrieve the snapshot document from the archive.

        Parameters
        ----------
        version: int
            Unique snapshot version number
        create_at: datetime.datatime, optional
            Timestamp (in UTC time) when snapshot was inserted into archive
        name: string, optional
            Optional name describing the snapshot
        archive: histore.archive.base.Archive, optional
            Archive object containing the snapshot
        """
        self.version = version
        self.created_at = created_at if not created_at is None else datetime.datetime.utcnow()
        self.name = name if not name is None else self.created_at.isoformat()
        self.archive = archive

    @staticmethod
    def from_dict(doc, archive=None):
        """Create snapshot from dictionary serialization as returned by
        the .to_dict() method.

        Parameters
        ----------
        doc: dict
            Dictionary representation of snapshot.
        archive: histore.archive.base.Archive, optional
            Archive object containing the snapshot

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        return Snapshot(
            version=doc['version'],
            created_at=datetime.datetime.strptime(
                doc['createdAt'],
                '%Y-%m-%dT%H:%M:%S.%f'
            ),
            name=doc['name'],
            archive=archive
        )

    def get(self):
        """Get the snapshot document from the archive.

        Raises ValueError if the arhive object is not set.

        Returns
        -------
        dict
        """
        if self.archive is None:
            raise ValueError('snapshot archive not set')
        return self.archive.get(self.version)

    def to_dict(self):
        """Get dictionary serialization for snapshot.

        Returns
        -------
        dict
        """
        return {
            'version': self.version,
            'createdAt': self.created_at.isoformat(),
            'name': self.name
        }
