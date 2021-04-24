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

from typing import Any, List, Optional

from histore.archive.row import ArchiveRow
from histore.archive.timestamp import Timestamp
from histore.archive.value import ArchiveValue, MultiVersionValue, SingleVersionValue
from histore.archive.serialize.default import DefaultSerializer

import histore.key as anno


class CompactSerializer(DefaultSerializer):
    """Implementation of the archive object serializer. This serializer is
    intended for producing more compact serializations compared to the default
    serializer used by HISTORE.

    The compact serializer takes advantage of some optimization opportunities
    over the default serializer. It inherits from the default serializer and
    modifies the way how rows, timestamps and values are serialized.

    For rows the compact serializer takes advantage of the fact that each row
    has a fixed number of elements to it. The serializer therefore uses a list
    of objects instead of a dictionary. The elements in the list are:
    1) row identifier,
    2) row key
    3) row timestamp
    4) row position
    5) a timestamp-like serialization of the column identifier for the row cells, and
    6) a list of cell values corresponding to the list of column identifier in 5).

    For timestamps intervals that start and end at the same version are
    serialized as a single integer instead of a pair of integers.

    Archive values that only have one version are not serialized as a dictionary
    but as the value itself. This has the disadvantage that values cannot be
    dictionaries as otherwise the deserialization will fail.
    """
    def __init__(
        self, timestamp: Optional[str] = 't', pos: Optional[str] = 'p',
        name: Optional[str] = 'n', value: Optional[str] = 'v',
        colid: Optional[str] = 'c', version: Optional[str] = 'v',
        valid_time: Optional[str] = 'vt', transaction_time: Optional[str] = 'tt',
        description: Optional[str] = 'd', action: Optional[str] = 'a'
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
        value: string, default='v'
            Element label for timestamped values.
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
            value=value,
            colid=colid,
            version=version,
            valid_time=valid_time,
            transaction_time=transaction_time,
            description=description,
            action=action
        )

    def deserialize_row(self, obj: List) -> ArchiveRow:
        """Get archive row instance from a serialized object. Expects a
        dictionary as created by the serialize_row method.

        Parameters
        ----------
        obj: list
            Serialized archive row object.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        rowid = obj[0]
        key = obj[1]
        ts = self.deserialize_timestamp(obj[2])
        pos = self.deserialize_value(obj=obj[3], ts=ts)
        columns = obj[4]
        values = obj[5]
        cells = dict()
        validx = 0
        for interval in columns:
            if isinstance(interval, list):
                for i in range(interval[0], interval[1] + 1):
                    cells[i] = self.deserialize_value(obj=values[validx], ts=ts)
                    validx += 1
            else:
                cells[interval] = self.deserialize_value(obj=values[validx], ts=ts)
                validx += 1
        if isinstance(key, list) or isinstance(key, tuple):
            key = tuple([anno.to_key(k) for k in key])
        else:
            key = anno.to_key(key)
        return ArchiveRow(
            rowid=rowid,
            key=key,
            pos=pos,
            cells=cells,
            timestamp=ts
        )

    def serialize_row(self, row: ArchiveRow) -> List:
        """Get serialization for an archive row object.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Archive row object that is being serialized.

        Returns
        list
        dict
        """
        ts = row.timestamp
        columns = list()
        values = list()
        for colid, val in sorted(row.cells.items(), key=lambda x: x[0]):
            if columns:
                last_interval = columns[-1]
                if last_interval[1] == colid - 1:
                    last_interval[1] = colid
                else:
                    if last_interval[0] == last_interval[1]:
                        columns[-1] = last_interval[0]
                    columns.append([colid, colid])
            else:
                columns.append([colid, colid])
            values.append(self.serialize_value(value=val, ts=ts))
        return [
            row.rowid,
            row.key,
            self.serialize_timestamp(ts),
            self.serialize_value(value=row.pos, ts=ts),
            columns,
            values
        ]

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
            if value.has_timestamp and not value.timestamp.is_equal(ts):
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
