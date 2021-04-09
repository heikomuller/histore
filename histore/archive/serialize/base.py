# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class for object serialization and deserialization."""

from abc import ABCMeta, abstractmethod
from typing import Dict, Optional


class ArchiveSerializer(metaclass=ABCMeta):
    """The serializer is used the serialize and deserialize components of
    dataset archives. All serializations are either lists or dictionaries.
    """
    @abstractmethod
    def deserialize_column(self, obj):
        """Get archive schema column instance from a serialized object. The
        type of the object depends on the serializer implementation. It is
        assumed that the object has been serialized using the respective method
        from the same implementing class.

        Parameters
        ----------
        obj: string, list, or dict
            Serialized archive row object.

        Returns
        -------
        histore.archive.schema.ArchiveColumn
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def serialize_column(self, column):
        """Get serialization for an archive schema column.

        Parameters
        ----------
        column: histore.archive.schema.ArchiveColumn
            Archive column object that is being serialized.

        Returns
        -------
        string, list, or dict
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def deserialize_row(self, obj):
        """Get archive row instance from a serialized object. The type of the
        object depends on the serializer implementation. It is assumed that
        the object has been serialized using the respective method from
        the same implementing class.

        Parameters
        ----------
        obj: string, list, or dict
            Serialized archive row object.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def serialize_row(self, row):
        """Get serialization for an archive row object.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Archive row object that is being serialized.

        Returns
        -------
        string, list, or dict
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def deserialize_snapshot(self, obj):
        """Get snapshot descriptor instance from a serialized object. The type
        of the object depends on the serializer implementation. It is assumed
        that the object has been serialized using the respective method from
        the same implementing class.

        Parameters
        ----------
        obj: string, list, or dict
            Serialized snapshot descriptor object.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def serialize_snapshot(self, snapshot):
        """Get serialization for an archive snapshot descriptor.

        Parameters
        ----------
        snapshot: histore.archive.snapshot.Snapshot
            Archive snapshot descriptor that is being serialized.

        Returns
        -------
        string, list, or dict
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def deserialize_timestamp(self, obj):
        """Get timestamp instance from serialized object. The type of the
        object depends on the serializer implementation. It is assumed that
        the object has been serialized using the respective method from
        the same implementing class.

        Parameters
        ----------
        obj: string, list, or dict
            Serialized timestamp object.

        Returns
        -------
        histore.archive.timestamp.Timestamp
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def serialize_timestamp(self, ts):
        """Get serialization for a timestamp object.

        Parameters
        ----------
        ts: histore.archive.timestamp.Timestamp
            Archive timestamp object is a list of time intervals.

        Returns
        -------
        string, list, or dict
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def deserialize_value(self, obj, ts):
        """Get timestamped value object from a serialization object. The type
        of the object depends on the serializer implementation. It is assumed
        that the object has been serialized using the respective method from
        the same implementing class.

        Parameters
        ----------
        ts: histore.archive.timestamp.Timestamp
            Archive timestamp object is a list of time intervals.

        Returns
        -------
        string, list, or dict
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def serialize_value(self, value, ts):
        """Get serialization for a timestamp value.

        Parameters
        ----------
        value: histore.archive.value.ArchiveValue
            Timestamped value in a dataset archive.
        ts: histore.archive.timestamp.Timestamp
            Timestamp of the parent object.

        Returns
        -------
        string, list, or dict
        """
        raise NotImplementedError()  # pragma: no cover


# -- Helper Functions ---------------------------------------------------------

def SERIALIZER(clspath: str, kwargs: Optional[Dict] = None) -> Dict:
    """Helper to create serailizations for archive serializer specifications
    that are maintained in the archive descriptor.

    Parameters
    ----------
    clspath: str
        Full package path for a archive serializer class implementation.
    kwargs: dict, default=None
        Optional dictionary of keyword arguments that are passed to the
        class constructor.

    Returns
    -------
    dict
    """
    doc = {'clspath': clspath}
    if kwargs is not None:
        doc['kwargs'] = kwargs
    return doc


def COMPACT(
    timestamp: Optional[str] = 't', pos: Optional[str] = 'p',
    name: Optional[str] = 'n', value: Optional[str] = 'v',
    colid: Optional[str] = 'c', version: Optional[str] = 'v',
    valid_time: Optional[str] = 'vt', transaction_time: Optional[str] = 'tt',
    description: Optional[str] = 'd', action: Optional[str] = 'a'
):
    """Helper to create a specification for the compact serializer.

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

    Returns
    -------
    dict
    """
    return SERIALIZER(
        clspath='histore.archive.serialize.compact.CompactSerializer',
        kwargs={
            'timestamp': timestamp,
            'pos': pos,
            'name': name,
            'value': value,
            'colid': colid,
            'version': version,
            'valid_time': valid_time,
            'transaction_time': transaction_time,
            'description': description,
            'action': action
        }
    )


def DEFAULT(
    timestamp: Optional[str] = 't', pos: Optional[str] = 'p',
    name: Optional[str] = 'n', cells: Optional[str] = 'c',
    value: Optional[str] = 'v', key: Optional[str] = 'k',
    rowid: Optional[str] = 'r', colid: Optional[str] = 'c',
    version: Optional[str] = 'v', valid_time: Optional[str] = 'vt',
    transaction_time: Optional[str] = 'tt', description: Optional[str] = 'd',
    action: Optional[str] = 'a'
):
    """Helper to create a specification for the default serializer.

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

    Returns
    -------
    dict
    """
    return SERIALIZER(
        clspath='histore.archive.serialize.default.DefaultSerializer',
        kwargs={
            'timestamp': timestamp,
            'pos': pos,
            'name': name,
            'cells': cells,
            'value': value,
            'key': key,
            'rowid': rowid,
            'colid': colid,
            'version': version,
            'valid_time': valid_time,
            'transaction_time': transaction_time,
            'description': description,
            'action': action
        }
    )
