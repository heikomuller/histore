# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Document serializer allow for document snapshots to be converted into
different formats when retrieved from an archive.
"""

from abc import abstractmethod


class DocumentSerializer(object):
    """Abstract class for serializer that convert a document (e.g., a snapshot
    retrieved from an archive) into a different representation (e.g., a
    dictionary).
    """
    @abstractmethod
    def serialize(self, doc):
        """Convert the given document into an implementation-specific
        representation.

        Parameters
        ----------
        doc: histore.document.base.Document

        Returns
        -------
        any
        """
        raise NotImplementedError


class DefaultDocumentSerializer(DocumentSerializer):
    """The default document serializer converts documents into dictionaries.
    Element nodes that have indexes are converted into list elements.
    """
    def convert(self, nodes):
        """Add the nodes in the given list to a result dictionary. Returns
        the result dictionary.

        Parameters
        ----------
        nodes: list(histore.document.node.Node)

        Returns
        -------
        dict
        """
        result = dict()
        # Keep lists of nodes that are indexed (i.e., become part of a list).
        el_lists = dict()
        for node in nodes:
            if node.list_index is None:
                if node.is_leaf():
                    result[node.label] = node.value
                else:
                    result[node.label] = self.convert(node.children)
            else:
                if not node.label in el_lists:
                    el_lists[node.label] = list()
                el_lists[node.label].append(node)
        # For each list, first sort the nodes based on their index. then
        # convert all nodes into one list.
        for key in el_lists:
            el_list = el_lists[key]
            el_list.sort(key=lambda node: node.list_index)
            result[key] = list()
            for node in el_list:
                if node.is_leaf():
                    result[key].append(node.value)
                elif len(node.children) > 0:
                    # Only create elements for nodes that have children. Otherwise,
                    # we would add an empty dictionary.
                    result[key].append(self.convert(node.children))
        return result

    def serialize(self, doc):
        """Convert the given document into a dictionary.

        Parameters
        ----------
        doc: histore.document.base.Document

        Returns
        -------
        dict
        """
        return self.convert(doc.nodes)
