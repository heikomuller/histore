# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Collection of helper methods for debugging purposes."""

import json
import StringIO
import yaml

from histore.archive.serialize import DefaultArchiveSerializer


def archive_node_to_json_string(node, schema=None):
    """Get nested structure of an archive node formated as a Json string.

    Parameters
    -------
    node: histore.archive.node.ArchiveNode
    schema: histore.schema.document.DocumentSchema

    Returns
    -------
    string
    """
    serializer = DefaultArchiveSerializer(schema=schema)
    return json.dumps(serializer.element_to_dict(node), indent=4, sort_keys=True)


def archive_node_to_yaml_string(node, schema=None):
    """Get nested structure of an archive node formated as a Yaml string.

    Parameters
    -------
    node: histore.archive.node.ArchiveNode
    schema: histore.schema.document.DocumentSchema

    Returns
    -------
    string
    """
    serializer = DefaultArchiveSerializer(schema=schema)
    stream = StringIO.StringIO()
    yaml.dump(serializer.element_to_dict(node), stream, default_flow_style=False)
    return stream.getvalue()


def print_archive(archive, indent='\t'):
    """Print the nested structure of an archive.

    Parameters
    ----------
    archive: histore.archive.base.Archive
    indent: string
    """
    print str(archive.root())
    for node in archive.root().children:
        print_archive_node(node, indent=indent, depth=1)


def print_archive_node(node, indent='\t', depth=0):
    """Print an archive node.

    Parameters
    ----------
    node: histore.archive.node.ArchiveNode
    indent: string
    depth:  int
    """
    print (indent * depth) + str(node)
    if not node.is_value():
        for child in node.children:
            print_archive_node(child, indent=indent, depth=depth+1)


def print_document(document, indent='\t'):
    """Print the nested structure of a document.

    Parameters
    ----------
    document: histore.document.base.Document
    indent: string
    """
    print 'Document'
    for node in document.nodes:
        print_document_node(node, indent=indent)


def print_document_node(node, indent='\t', depth=0):
    """Print document node recursively.

    Parameters
    ----------
    node: histore.document.node.Node
    indent: string
    depth: int
    """
    print (indent * depth) + str(node)
    if not node.is_leaf():
        for child in node.children:
            print_document_node(child, indent=indent, depth=depth+1)
