# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class for object serialization and deserialization."""

from abc import ABCMeta, abstractmethod


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
