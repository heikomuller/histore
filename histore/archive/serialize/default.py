# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Default object serializer for dataset archives."""

from histore.archive.row import ArchiveRow
from histore.archive.schema import ArchiveColumn
from histore.archive.snapshot import Snapshot
from histore.archive.timestamp import TimeInterval, Timestamp
from histore.archive.value import (
    MultiVersionValue, SingleVersionValue, TimestampedValue
)
from histore.archive.serialize.base import ArchiveSerializer

import histore.util as util


class DefaultSerializer(ArchiveSerializer):
    """Implementation of the archive object serializer. This is the default
    serializer used by HISTORE.
    """
    def __init__(
        self, timestamp='t', pos='p', name='n', cells='c', value='v',
        key='k', rowid='r', colid='c', version='v', valid_time='vt',
        transaction_time='tt', description='d'
    ):
        """Initialize the labels for elements used in the serialized objects.
        By default short labels are used to reduce storage overhead.

        Parameters
        ----------
        timestamp: string, default='t'
            Element label for timestamp objects.
        pos: string, default='p'
            Element label for objects index position values.
        name: string, default='n'
            Element label for column names.
        cells: string, default='c'
            Element label for row cell values.
        value: string, default='v'
            Element label for timestamped values.
        key: string, default='k'
            Element label for row key values.
        rowid: string, default='r'
            Element label for arcive row identifier.
        colid: string, default='c'
            Element label for column identifier.
        version: string, default='v'
            Element label for snapshot version number.
        valid_time: string, default='vt'
            Element label for snapshot valid time.
        transaction_time: string, default='tt'
            Element label for snapshot transaction time.
        description: string, default='d'
            Element label for snapshot descriptions.
        """
        self.timestamp = timestamp
        self.pos = pos
        self.name = name
        self.cells = cells
        self.value = value
        self.key = key
        self.rowid = rowid
        self.colid = colid
        self.version = version
        self.valid_time = valid_time
        self.transaction_time = transaction_time
        self.description = description

    def deserialize_column(self, obj):
        """Get archive schema column instance from a serialized object.

        Parameters
        ----------
        obj: dict
            Serialized archive row object.

        Returns
        -------
        histore.archive.schema.ArchiveColumn
        """
        ts = self.deserialize_timestamp(obj[self.timestamp])
        return ArchiveColumn(
            identifier=obj[self.colid],
            name=self.deserialize_value(obj=obj[self.name], ts=ts),
            pos=self.deserialize_value(obj=obj[self.pos], ts=ts),
            timestamp=ts
        )

    def serialize_column(self, column):
        """Get serialization for an archive schema column. Creates a dictionary
        with elements for the column identifier, name, the index position, and
        the column timestamp.

        Parameters
        ----------
        column: histore.archive.schema.ArchiveColumn
            Archive column object that is being serialized.

        Returns
        -------
        dict
        """
        ts = column.timestamp
        return {
            self.colid: column.identifier,
            self.name: self.serialize_value(value=column.name, ts=ts),
            self.pos: self.serialize_value(value=column.pos, ts=ts),
            self.timestamp: self.serialize_timestamp(ts)
        }

    def deserialize_row(self, obj):
        """Get archive row instance from a serialized object. Expects a
        dictionary as created by the serialize_row method.

        Parameters
        ----------
        obj: dict
            Serialized archive row object.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        ts = self.deserialize_timestamp(obj[self.timestamp])
        rowid = obj[self.rowid]
        pos = self.deserialize_value(obj=obj[self.pos], ts=ts)
        cells = dict()
        for colid, value in obj[self.cells].items():
            cells[int(colid)] = self.deserialize_value(obj=value, ts=ts)
        if self.key in obj:
            key = obj[self.key]
            if isinstance(key, list):
                key = tuple(key)
        else:
            key = None
        return ArchiveRow(
            rowid=rowid,
            pos=pos,
            cells=cells,
            timestamp=ts,
            key=key
        )

    def serialize_row(self, row):
        """Get serialization for an archive row object.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Archive row object that is being serialized.

        Returns
        -------
        dict
        """
        ts = row.timestamp
        cells = dict()
        for colid, value in row.cells.items():
            cells[colid] = self.serialize_value(value=value, ts=ts)
        obj = {
            self.rowid: row.rowid,
            self.timestamp: self.serialize_timestamp(ts),
            self.pos: self.serialize_value(value=row.pos, ts=ts),
            self.cells: cells
        }
        if row.key != row.rowid:
            obj[self.key] = row.key
        return obj

    def deserialize_snapshot(self, obj):
        """Get snapshot descriptor instance from a serialized object.

        Parameters
        ----------
        obj: dict
            Serialized snapshot descriptor object.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        if self.description in obj:
            description = obj[self.description]
        else:
            description = None
        return Snapshot(
            version=obj[self.version],
            valid_time=util.to_datetime(obj[self.valid_time]),
            transaction_time=util.to_datetime(obj[self.transaction_time]),
            description=description
        )

    def serialize_snapshot(self, snapshot):
        """Get serialization for an archive snapshot descriptor.

        Parameters
        ----------
        snapshot: histore.archive.snapshot.Snapshot
            Archive snapshot descriptor that is being serialized.

        Returns
        -------
        dict
        """
        obj = {
            self.version: snapshot.version,
            self.valid_time: snapshot.valid_time.isoformat(),
            self.transaction_time: snapshot.transaction_time.isoformat()
        }
        if snapshot.description is not None:
            obj[self.description] = snapshot.description
        return obj

    def deserialize_timestamp(self, obj):
        """Get timestamp instance from serialization.

        Parameters
        ----------
        obj: list
            Serialized timestamp that is a list of 2-dimensional lists
            representing the time intervals.

        Returns
        -------
        histore.archive.timestamp.Timestamp
        """
        intervals = [TimeInterval(start=i[0], end=i[1]) for i in obj]
        return Timestamp(intervals=intervals)

    def serialize_timestamp(self, ts):
        """Get serialization for atimestamp object. A timestamp is serialized
        as a list of 2-dimensional lists that contain the start and end version
        of the time interval.

        Parameters
        ----------
        ts: histore.archive.timestamp.Timestamp
            Archive timestamp object is a list of time intervals.

        Returns
        -------
        list
        """
        return [[interval.start, interval.end] for interval in ts.intervals]

    def deserialize_value(self, obj, ts):
        """Get timestamped value object from a serialization object. Expects a
        dictionary for single version values or a list of dictionaries for
        multi-version values.

        Parameters
        ----------
        obj: list or dict
            Serialized timestamped values as generatred by the serialize_value
            method.
        ts: histore.archive.timestamp.Timestamp
            Timestamp of the parent object.

        Returns
        -------
        string, list, or dict
        """
        if isinstance(obj, dict):
            if self.timestamp in obj:
                ts = self.deserialize_timestamp(obj[self.timestamp])
            return SingleVersionValue(value=obj[self.value], timestamp=ts)
        elif isinstance(obj, list):
            values = []
            for v in obj:
                values.append(
                    TimestampedValue(
                        value=v[self.value],
                        timestamp=self.deserialize_timestamp(v[self.timestamp])
                    )
                )
            return MultiVersionValue(values=values)
        raise ValueError('invalid serialization object')

    def serialize_value(self, value, ts):
        """Get serialization for a timestamp value. Depending on whether the
        value is a single version value or a multi-version value either a
        dictionary (single version) or a list of dictionaries (multi-version)
        is returned.

        Parameters
        ----------
        value: histore.archive.value.ArchiveValue
            Timestamped value in a dataset archive.
        ts: histore.archive.timestamp.Timestamp
            Timestamp of the parent object.

        Returns
        -------
        list or dict
        """
        if value.is_single_version():
            # A single version value is a dictionary containing the timestamped
            # value and an optional timestamp (if different from the parent
            # timestamp).
            obj = dict()
            if not value.timestamp.is_equal(ts):
                obj[self.timestamp] = self.serialize_timestamp(value.timestamp)
            obj[self.value] = value.value
        else:
            # A multi-version value is serialized as a list of single version
            # values each with their own timestamp.
            obj = [{
                self.timestamp: self.serialize_timestamp(v.timestamp),
                self.value: v.value
            } for v in value.values]
        return obj