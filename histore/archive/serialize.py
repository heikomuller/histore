# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

from abc import abstractmethod

from histore.archive.base import Archive
from histore.archive.node import ArchiveElement, ArchiveValue
from histore.archive.snapshot import Snapshot
from histore.archive.store.mem import InMemoryArchiveStore
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.timestamp import Timestamp


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
    """Base class that defines the serializer interface methods for converting
    archives and form/to dictionaries.

    Maintains a list of reserved keywords that cannot be used as node labels.
    """
    def __init__(self, mapping=None):
        """Initialize the list of reserved keyword. The user can provide a
        mapping that overrides the default keyword (i.e., map default keywords
        to user-defined keywords).

        Raises ValueError if the given mapping dose not result in a 1:1 mapping
        of keywords.

        Parameters
        ----------
        mapping: dict(string:string)
            Expects a mapping from keywords defined in KEYWORDS to user-defined
            values.
        """
        # Initialize and complete the keyword list and mapping
        if not mapping is None:
            self.mapping = dict(mapping)
            for key in KEYWORDS:
                if not key in self.mapping:
                    self.mapping[key] = key
        else:
            self.mapping = dict()
            for key in KEYWORDS:
                self.mapping[key] = key
        # Ensure that keywords in KEYWORDS are mapped to different elements
        targets = set()
        for key in KEYWORDS:
            target = self.mapping[key]
            if target in targets:
                raise ValueError('not a 1:1 mapping of keywords')
            targets.add(target)
        self.keywords = self.mapping.values()

    @abstractmethod
    def from_dict(self, doc):
        """Create an archive instance from a dictionary serialization.

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
    """Compact serialization of archives. All key path values will be omitted in
    the serialized object. These values are maintained as part of the element
    keys and can therefore be reconstructed from them.
    """
    def __init__(self, mapping=None):
        """Initialize the list of reserved keyword in the super class.

        Raises ValueError's if reserved keywords are used as node labels.

        Parameters
        ----------
        mapping: dict(string:string)
            Expects a mapping from keywords defined in KEYWORDS to user-defined
            values.
        """
        super(DefaultArchiveSerializer, self).__init__(mapping=mapping)

    def from_dict(self, doc):
        """Create an archive instance from a dictionary serialization.

        Parameters
        ----------
        doc: dict

        Returns
        -------
        histore.archive.base.Archive
        """
        schema = DocumentSchema.from_dict(doc['schema'])
        data = doc['data']
        if len(data) == 1:
            key = data.keys()[0]
            root = self.element_from_dict(data[key], label=key, schema=schema)
        else:
            raise ValueError('invalid serialization for archive root \'' + str(data) + '\'')
        return Archive(
            schema=schema,
            snapshots=[Snapshot.from_dict(s) for s in doc['snapshots']],
            store=InMemoryArchiveStore(root=root)
        )

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
            key_paths = list()
            for key in archive.schema.keys():
                if key.is_keyed_by_path_values():
                    for value_path in key.value_paths:
                        path = key.target_path.concat(value_path)
                        key_paths.append(path.to_key())
            return {
                'schema': archive.schema.to_dict(),
                'snapshots': [s.to_dict() for s in archive.snapshots],
                'data': {
                    archive.root().label : self.element_to_dict(
                        node=archive.root(),
                        key_paths=key_paths
                    )
                }
            }
        else:
            return dict()

    def element_from_dict(self, doc, label, schema, path=Path(''), timestamp=None):
        """
        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        # Start by re-creating the node metadata (i.e., timestamp, key, index
        # positions) from the @meta dictionary (if present)
        t = timestamp
        key = None
        positions = None
        l_meta = self.mapping[LABEL_META]
        if l_meta in doc:
            meta = doc[l_meta]
            # Timestamp
            if LABEL_TIMESTAMP in meta:
                t = Timestamp.from_string(meta[LABEL_TIMESTAMP])
            # Key
            if LABEL_KEY in meta:
                key = meta[LABEL_KEY]
            # Positions
            if LABEL_POSITIONS in meta:
                positions = list()
                for el in meta[LABEL_POSITIONS]:
                    positions.append(self.value_from_dict(el, timestamp=t))
        # Create list of node children. Value nodes are identified by the @val
        # key. All other key values in the dictionary represent element nodes
        children = list()
        l_value = self.mapping[LABEL_VALUE]
        for l_child in doc:
            if l_child == l_meta:
                # Skip the meta data
                continue
            elif l_child == l_value:
                for el in doc[l_value]:
                    children.append(self.value_from_dict(el, timestamp=t))
            else:
                for el in doc[l_child]:
                    children.append(
                        self.element_from_dict(
                            el,
                            label=l_child,
                            schema=schema,
                            path=path.extend(l_child),
                            timestamp=t
                        )
                    )
        # Create the new archive node
        arch_node = ArchiveElement(
            label=label,
            timestamp=t,
            key=key,
            positions=positions,
            children=children,
            sort=True
        )
        # Add key path value nodes if this node is keyed by path values
        key_spec = schema.get(path)
        if not key_spec is None and key_spec.is_keyed_by_path_values():
            # Create an element node with single child value for each
            # key path value. The node inherits the timestamp from this
            # node.
            for i in range(len(key_spec.value_paths)):
                val_path = key_spec.value_paths[i]
                key_node = ArchiveElement(
                    label=val_path.last_element(),
                    timestamp=t,
                    children=[ArchiveValue(value=key[i], timestamp=t)]
                )
                add_child_node(arch_node, val_path.prefix(), key_node)
        return arch_node

    def element_to_dict(self, node, timestamp=None, path=Path(''), key_paths=list()):
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
                if not target_path.to_key() in key_paths:
                    if not child.label in obj:
                        obj[child.label] = list()
                    obj[child.label].append(
                        self.element_to_dict(
                            node=child,
                            timestamp=node.timestamp,
                            path=target_path,
                            key_paths=key_paths
                        )
                    )
        return obj

    def value_from_dict(self, doc, timestamp=None):
        """Create an instance of an archive value node from a dictionary
        representation as returned by .value_to_dict(). The provided doc
        parameter is either a dictionary or a scalar value (depending on
        whether the value node has its own timestamp or inherits the timestamp
        of the parent node).

        Raises ValueError if the given doc argument does not represent a valid
        serialization of an archive value node.

        Parameters
        ----------
        doc: dict or scalar
        timestamp: histore.timestamp.Timestamp, optional

        Returns
        -------
        dict or string
        """
        # If the document is a dictionary we expect two elements, the value @val
        # and the timestamp @t. Otherwise we assume that the document is a
        # scalar value
        if isinstance(doc, dict):
            if len(doc) == 2:
                # Get mapping for @val
                l_value = self.mapping[LABEL_VALUE]
                if l_value in doc and LABEL_TIMESTAMP in doc:
                    # The value node has a timestamp that is different from the
                    # parent node.
                    return ArchiveValue(
                        value=doc[l_value],
                        timestamp=Timestamp.from_string(doc[LABEL_TIMESTAMP])
                    )
        else:
            # Assume that doc is a scalar value. The timestamp of the value node
            # is the timestamp of the parent node.
            return ArchiveValue(value=doc, timestamp=timestamp)
        # If this part is reached the dictionary does not
        raise ValueError('invalid serialization for value node \'' + str(doc) + '\'')

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


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def add_child_node(parent, path, node):
    """Add the given node as a child of the node from the list nodes that is at
    the given path. Note that the parent node has to exist as we are only
    omitting the node that contains the key path value.

    Parameters
    ----------
    parent: histore.archive.node.ArchiveElement
    path: histore.path.Path
    node: histore.archive.node.ArchiveElement

    Returns
    -------
    bool
    """
    # If the path is empty we already found the parent and nodes is the list
    # of child nodes for that parent. Make sure to keep the nodes sorted.
    if path.is_empty():
        parent.add(node)
        parent.sort()
    else:
        for child in parent.children:
            if child.is_element() and child.label == path.first_element():
                # Add node recirsively and return
                return add_child_node(child, path.subpath(), node)
