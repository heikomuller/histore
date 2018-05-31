# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

from abc import abstractmethod

from histore.path import Path

"""Serializer are used to (1) convert archives into dictionaries, and (2) to
do the reverse and convert dictionaries into archives.
"""

"""Reserved keywords for archive serializations."""
LABEL_KEY = '@key'
LABEL_LABEL = '@label'
LABEL_META = '@meta'
LABEL_NODES = '@nodes'
LABEL_POSITIONS = '@pos'
LABEL_TIMESTAMP = '@t'
LABEL_VALUE = '@val'

KEYWORDS = [
    LABEL_META,
    LABEL_VALUE
]


class ArchiveSerializer(object):
    """Base class that defines the serializer interface methods for convering
    archives and form/to dictionaries.

    Maintains a list of reserved keywords that cannot be used as node labels.
    """
    def __init__(self, mapping=None):
        """Initialize the list of reserved keyword. The user can provide a
        mapping that overrides the default keyword (i.e., map default keywords
        to user-defined keywords).

        Raises ValueError if the given mapping dose not resutl in a 1:1 mapping
        of keywords.

        Parameters
        ----------
        mapping: dict(string:string)
            Expects a mapping from keywords defined in KEYWORDS to user-defined
            values.
        """
        # Initializa and complete the keyword list and mapping
        if not mapping is None:
            self.mapping = dict(mapping)
            for key in KEYWORDS:
                if not key in self.mapping:
                    self.mapping[key] = key
        else:
            self.mapping = dict()
            for key in KEYWORDS:
                self.mapping[key] = key
        # Ensure that keywwords in KEYWORDS are mapped to different elements
        targets = set()
        for key in KEYWORDS:
            target = self.mapping[key]
            if target in targets:
                raise ValueError('not a 1:1 mapping of keywords')
            targets.add(target)
        self.keywords = self.mapping.values()

    @abstractmethod
    def from_dict(self, doc):
        """Create an archive instance from a disctionary serialization.

        Parameters
        ----------
        doc: dict

        Returns
        -------
        histore.archive.base.Archive
        """
        raise NotImplementedError

    def to_dict(self, archive):
        """Get dictionary serialization for for given archive.

        Parameters
        ----------
        archive: histore.archive.base.Archive

        Returns
        -------
        dict
        """
        raise NotImplementedError


class DefaultArchiveSerializer(ArchiveSerializer):
    def __init__(self, mapping=None):
        """Initialize the list of reserved keyword in the super class.

        Parameters
        ----------
        mapping: dict(string:string)
            Expects a mapping from keywords defined in KEYWORDS to user-defined
            values.
        """
        super(DefaultArchiveSerializer, self).__init__(mapping=mapping)

    def to_dict(self, archive):
        """Get dictionary serialization for the given archive.

        Parameters
        ----------
        archive: histore.archive.base.Archive

        Returns
        -------
        dict
        """
        if not archive.root() is None:
            return self.element_to_dict(archive.root())
        else:
            return dict()

    def element_to_dict(self, node, timestamp=None):
        """Get dictionary serialization for an archive element node.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement
        timestamp: histore.timestamp.Timestamp, optional

        Returns
        -------
        dict
        """
        obj = {
            LABEL_LABEL: node.label
        }
        # Add timestamp only if the element has a local timestamp
        if timestamp is None or not node.timestamp.is_equal(timestamp):
            obj[LABEL_TIMESTAMP] = str(node.timestamp)
        # Add key and positions if the element is keyed by values
        if not node.key is None:
            obj[LABEL_KEY] = node.key
            obj[LABEL_POSITIONS] = [
                self.value_to_dict(pos, timestamp=node.timestamp) for pos in node.positions
            ]
        # Add children
        children = list()
        for child in node.children:
            if child.is_value():
                children.append(self.value_to_dict(child, timestamp=node.timestamp))
            else:
                children.append(self.element_to_dict(child, timestamp=node.timestamp))
        obj[LABEL_NODES] = children
        return obj

    def value_to_dict(self, node, timestamp=None):
        """Get dictionary representation for a value node.

        Parameters
        ----------
        node: histore.archive.node.ArchiveValue
        timestamp: histore.timestamp.Timestamp, optional

        Returns
        -------
        dict
        """
        obj = {self.mapping[LABEL_VALUE]: node.value}
        # Add timestamp only if the element has a local timestamp
        if timestamp is None or not node.timestamp.is_equal(timestamp):
            obj[LABEL_TIMESTAMP] = str(node.timestamp)
        return obj



class CompactArchiveSerializer(ArchiveSerializer):
    """Compact serializaion of archives. Raises ValueError's if reserved
    keywords are used as node labels.
    """
    def __init__(self, mapping=None, schema=None):
        """Initialize the list of reserved keyword in the super class. If the
        optinal schema is provided all key path values will be omitted in the
        serialized object.

        Parameters
        ----------
        mapping: dict(string:string)
            Expects a mapping from keywords defined in KEYWORDS to user-defined
            values.
        """
        super(CompactArchiveSerializer, self).__init__(mapping=mapping)
        self.key_paths = list()
        if not schema is None:
            for key in schema.keys():
                if hasattr(key, 'value_paths'):
                    for value_path in key.value_paths:
                        path = key.target_path.concat(value_path)
                        self.key_paths.append(path.to_key())

    def to_dict(self, archive):
        """Get dictionary serialization for the given archive.

        Parameters
        ----------
        archive: histore.archive.base.Archive

        Returns
        -------
        dict
        """
        if not archive.root() is None:
            return {archive.root().label : self.element_to_dict(archive.root())}
        else:
            return dict()

    def element_to_dict(self, node, timestamp=None, path=Path('')):
        """Get dictionary serialization for an archive element node.

        Parameters
        ----------
        node: histore.archive.node.ArchiveElement
        timestamp: histore.timestamp.Timestamp, optional

        Returns
        -------
        dict
        """
        if node.label in self.keywords:
            raise ValueError('node label \'' + node.label + '\' is a reserved keyword')
        meta = dict()
        # Add timestamp only if the element has a local timestamp
        if timestamp is None or not node.timestamp.is_equal(timestamp):
            meta[LABEL_TIMESTAMP] = str(node.timestamp)
        # Add key and positions if the element is keyed by values
        if not node.key is None:
            meta[LABEL_KEY] = node.key
            meta[LABEL_POSITIONS] = [
                self.value_to_dict(pos, timestamp=node.timestamp) for pos in node.positions
            ]
        obj = dict()
        if len(meta) > 0:
            obj[self.mapping[LABEL_META]] = meta
        # Add children
        for child in node.children:
            if child.is_value():
                if not self.mapping[LABEL_VALUE] in obj:
                    obj[self.mapping[LABEL_VALUE]] = list()
                obj[self.mapping[LABEL_VALUE]].append(self.value_to_dict(child, timestamp=node.timestamp))
            else:
                target_path = path.extend(child.label)
                if not target_path.to_key() in self.key_paths:
                    if not child.label in obj:
                        obj[child.label] = list()
                    obj[child.label].append(self.element_to_dict(child, timestamp=node.timestamp, path=target_path))
        return obj

    def value_to_dict(self, node, timestamp=None):
        """Get dictionary representation for a value node.

        Parameters
        ----------
        node: histore.archive.node.ArchiveValue
        timestamp: histore.timestamp.Timestamp, optional

        Returns
        -------
        dict or string
        """
        # Return simply the value if it does not have a timestamp. Otherwise,
        # return a dictionary with value and timestamp.
        if timestamp is None or not node.timestamp.is_equal(timestamp):
            return {
                self.mapping[LABEL_VALUE]: node.value,
                LABEL_TIMESTAMP: str(node.timestamp)
            }
        else:
            return node.value
