# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Compact object serializer for dataset archives. Modifies the default serializer
to save storage space by allowing for more compact serializations of timestamps
and single-version values.

Warning: This serializer CANNOT be used for datasets that have cell values that
are dictionaries.
"""

from typing import Any, List

from histore.archive.timestamp import TimeInterval, Timestamp
from histore.archive.value import ArchiveValue, MultiVersionValue, SingleVersionValue
from histore.archive.serialize.default import DefaultSerializer


class CompactSerializer(DefaultSerializer):
    """Implementation of the archive object serializer. This serializer is
    intended for producing more compact serializations compared to the default
    serializer used by HISTORE.

    The compact serializer takes advantage of some optimization opportunities
    over the default serializer. It inherits from the default serializer and
    modifies the way how timestamps and values are serialized.

    For timestamps intervals that start and end at the same version are
    serialized as a single integer instead of a pair of integers.

    Archive values that only have one version are not serialized as a dictionary
    but as the value itself. This has the disadvantage that values cannot be
    dictionaries as otherwise the deserialization will fail.
    """
    def __init__(
        self, timestamp='t', pos='p', name='n', cells='c', value='v',
        key='k', rowid='r', colid='c', version='v', valid_time='vt',
        transaction_time='tt', description='d', action='a'
    ):
        """Initialize the labels for elements used in the serialized objects.
        By default short labels are used to reduce storage overhead.

        Passing a label that starts with the reserved character '$' will result
        in a ValueError.

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
        action: string, default='a'
            Element label for snapshot actions.

        Raises
        ------
        ValueError
        """
        super(CompactSerializer, self).__init__(
            timestamp=timestamp,
            pos=pos,
            name=name,
            cells=cells,
            value=value,
            key=key,
            rowid=rowid,
            colid=colid,
            version=version,
            valid_time=valid_time,
            transaction_time=transaction_time,
            description=description,
            action=action
        )

    def deserialize_timestamp(self, obj: List) -> Timestamp:
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
        intervals = list()
        for i in obj:
            interval = TimeInterval(i) if isinstance(i, int) else TimeInterval(start=i[0], end=i[1])
            intervals.append(interval)
        return Timestamp(intervals=intervals)

    def serialize_timestamp(self, ts: Timestamp) -> List:
        """Get serialization for atimestamp object.

        A timestamp is serialized as a list of integers or lists that contain
        the start and end version of the time interval.

        Parameters
        ----------
        ts: histore.archive.timestamp.Timestamp
            Archive timestamp object is a list of time intervals.

        Returns
        -------
        list
        """
        return [[i.start, i.end] if i.start != i.end else i.start for i in ts.intervals]

    def deserialize_value(self, obj: Any, ts: Timestamp) -> ArchiveValue:
        """Get timestamped value object from a serialization object. Expects a
        dictionary for single version values or a list of dictionaries for
        multi-version values.

        Parameters
        ----------
        obj: any
            Serialized timestamped values as generatred by the serialize_value
            method.
        ts: histore.archive.timestamp.Timestamp
            Timestamp of the parent object.

        Returns
        -------
        histore.archive.value.ArchiveValue
        """
        if isinstance(obj, dict):
            return SingleVersionValue(
                value=obj[self.value],
                timestamp=self.deserialize_timestamp(obj[self.timestamp])
            )
        elif isinstance(obj, list):
            values = []
            for v in obj:
                values.append(
                    SingleVersionValue(
                        value=v[self.value],
                        timestamp=self.deserialize_timestamp(v[self.timestamp])
                    )
                )
            return MultiVersionValue(values=values)
        else:
            return SingleVersionValue(value=obj, timestamp=ts)

    def serialize_value(self, value: ArchiveValue, ts: Timestamp) -> Any:
        """Get serialization for a timestamp value.

        Depending on whether the value is a single version value or a
        multi-version value either (i) the value itself (single-value with no
        separate timestamp), (ii) a dictionary including the single version
        value and its timestamp, or (iii) or a list of dictionaries (multi-version)
        is returned.

        Parameters
        ----------
        value: histore.archive.value.ArchiveValue
            Timestamped value in a dataset archive.
        ts: histore.archive.timestamp.Timestamp
            Timestamp of the parent object.

        Returns
        -------
        any
        """
        if value.is_single_version():
            # A single version value is either serialized as a dictionary
            # containing the value and its timestamp (if it different from the
            # parent timestamp) or as the versioned value itself.
            if not value.timestamp.is_equal(ts):
                obj = dict()
                obj[self.timestamp] = self.serialize_timestamp(value.timestamp)
                obj[self.value] = value.value
            else:
                obj = value.value
        else:
            # A multi-version value is serialized as a list of single version
            # values each with their own timestamp.
            obj = [{
                self.timestamp: self.serialize_timestamp(v.timestamp),
                self.value: v.value
            } for v in value.values]
        return obj
