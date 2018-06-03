# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Simple in-memory store for archives."""

import gzip
import json
import os
import yaml

from histore.archive.serialize import DefaultArchiveSerializer
from histore.archive.snapshot import Snapshot
from histore.archive.store.base import ArchiveStore
from histore.schema.document import DocumentSchema


"""Identifier for different file formats."""
JSON = 'JSON'
YAML = 'YAML'

FILE_FORMATS = [JSON, YAML]

"""Keys for elements in the archive cache. The predefined sets CACHE_ALL and
CACHE_METADATA can be used cache all archive objects or only the meatdata
objects (snapshots and schema), respectively."""
LABEL_ROOT = 'root'
LABEL_SCHEMA = 'schema'
LABEL_SNAPSHOTS = 'snapshots'

CACHE_ALL = [LABEL_ROOT, LABEL_SCHEMA, LABEL_SNAPSHOTS]
CACHE_METADATA = [LABEL_SCHEMA, LABEL_SNAPSHOTS]


class PersistentArchiveStore(ArchiveStore):
    def __init__(self, filename, format=None, compressed=None, cache=CACHE_ALL):
        """Initialize archive from the given file. If format is not given an attempt
        is made to 'guess' the format from the file suffix. By default, JSON
        format is used.

        Raises ValueError if the given file does not exist or the given format
        identifier is invalid.

        Parameters
        ----------
        filename: string
        format: string, optional
        compressed: bool, optional
        cached: bool, optional
        """
        # Raise exception if file does not exist.
        if not os.path.isfile(filename):
            raise ValueError('file \'' + str(filename) + '\' does not exist')
        self.filename = filename
        # If compressed argument is not given try to guess based on file suffix.
        self.compressed = get_file_compression(filename, compressed=compressed)
        # Determine the file format
        self.format = get_file_format(filename, format=format)
        # Keep archive information in cache as defined by the optinonal cache
        # element list. The cache is a dictionary containing the the archive
        # components that were requested to be cached..
        if cache:
            root, snapshots, schema = self.read()
            self.cache = dict()
            if LABEL_ROOT in cache:
                self.cache[LABEL_ROOT] = root
            if LABEL_SCHEMA in cache:
                self.cache[LABEL_SCHEMA] = schema
            if LABEL_SNAPSHOTS in cache:
                self.cache[LABEL_SNAPSHOTS] = snapshots
        else:
            # Make sure to set the cache object to None to indicate that archive
            # information is not cached.
            self.cache = None

    @staticmethod
    def create(filename, schema, format=None, compressed=None, cache=CACHE_ALL, overwrite=False):
        """Create an archive file for an empty archive with a given schema.

        Raises a ValueError if the given file exists and the overwrite flag is
        False.

        Parameters
        ----------
        filename: string
        schema: histore.schema.document.DocumentSchema
        format: string, optional
        compressed: bool, optional
        cached: bool, optional

        Returns
        -------
        histore.archive.store.fs.PersistentArchiveStore
        """
        # Raise exception if file exists and is not to be overwritten
        if os.path.isfile(filename) and overwrite is False:
            raise ValueError('file \'' + str(filename) + '\' exists')
        # Get file compression flag
        compressed = get_file_compression(filename, compressed=compressed)
        # Determine the file format
        format = get_file_format(filename, format=format)
        # Write archive with only schema information to file.
        f = open_file(filename, 'w', compressed)
        write_document({LABEL_SCHEMA: schema.to_dict()}, f, format)
        f.close()
        # Return the persistent store object.
        return PersistentArchiveStore(
            filename,
            format=format,
            compressed=compressed,
            cache=cache
        )

    def get_root(self):
        """Get the root node for the archive that is maintained by this store.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        if not self.cache is None and LABEL_ROOT in self.cache:
            root = self.cache[LABEL_ROOT]
        else:
            root, snapshots, schema = self.read()
        return root

    def get_schema(self):
        """Get the current archive schema.

        Returns
        -------
        histore.schema.document.DocumentSchema
        """
        if not self.cache is None and LABEL_SCHEMA in self.cache:
            schema = self.cache[LABEL_SCHEMA]
        else:
            root, snapshots, schema = self.read()
        return schema

    def get_snapshots(self):
        """Get the current list of document snapshot handles.

        Returns
        -------
        list(histore.archive.snapshot.Snapshot)
        """
        if not self.cache is None and LABEL_SNAPSHOTS in self.cache:
            snapshots = self.cache[LABEL_SNAPSHOTS]
        else:
            root, snapshots, schema = self.read()
        return list(snapshots)

    def read(self):
        """Read the complete archive information. Returns a triple containing
        the archive root, the list of snapshots, and the archive schema.

        Returns
        -------
        histore.archive.node.ArchiveElement
        list(histore.archive.snapshot.Snapshot)
        histore.schema.document.DocumentSchema
        """
        # Read the archive information from file. This will raise an exception
        # if the file does not exist.
        f = open_file(self.filename, 'r', self.compressed)
        if self.format == JSON:
            doc = json.loads(f.read())
        else:
            doc = yaml.load(f.read())
        f.close()
        # The read dictionary is expected to contain at least the archive
        # schema.
        schema = DocumentSchema.from_dict(doc[LABEL_SCHEMA])
        # Snapshots and root are optional. However, they either are both
        # missing (in an empty archive) or are both present.
        if not LABEL_SNAPSHOTS in doc and not LABEL_ROOT in doc:
            root = None
            snapshots = list()
        else:
            root = DefaultArchiveSerializer(schema).from_dict(doc[LABEL_ROOT])
            snapshots = [Snapshot.from_dict(s) for s in doc[LABEL_SNAPSHOTS]]
        return root, snapshots, schema

    def write(self, root, snapshots, schema=None):
        """Relace the current archive information with an updated version
        (e.g., after adding a new snapshot to the archive). At this point the
        schema is not expected to be changed after the archive is created.
        However, the system is capable to manage changes to the schema if they
        only afect elements that have not been present in any of the previous
        document snapshots.

        Parameters
        ----------
        root: histore.archive.node.ArchiveElement
        snapshots: list(histore.archive.snapshot.Snapshot)
        schema: histore.schema.document.DocumentSchema
        """
        self.validate_snapshots(root, snapshots)
        if schema is None:
            schema = self.get_schema()
        doc = {
            LABEL_ROOT: DefaultArchiveSerializer(schema).to_dict(root),
            LABEL_SCHEMA: schema.to_dict(),
            LABEL_SNAPSHOTS: [s.to_dict() for s in snapshots],
        }
        # Write the archive information to file.
        f = open_file(self.filename, 'w', self.compressed)
        write_document(doc, f, self.format)
        f.close()
        # Keep update information in cache if cached
        if not self.cache is None:
            if LABEL_ROOT in self.cache:
                self.cache[LABEL_ROOT] = root
            if LABEL_SNAPSHOTS in self.cache:
                self.cache[LABEL_SNAPSHOTS] = snapshots
            if not schema is None and LABEL_SCHEMA in self.cache:
                self.cache[LABEL_SCHEMA] = schema

# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_file_compression(filename, compressed=None):
    """Get file compression flag. If compressed argument is not given try to
    guess based on file suffix.

    Parameters
    ----------
    filename: string
    compressed: bool, optional

    Returns
    -------
    bool
    """
    if compressed is None:
        return filename.lower().endswith('.gz')
    else:
        return compressed


def get_file_format(filename, format=None):
    """Get file format. If format argument is not given try to guess based on
    file suffix.

    Parameters
    ----------
    filename: string
    format: string, optional

    Returns
    -------
    string
    """
    if not format is None:
        if not format.upper() in FILE_FORMATS:
            raise ValueError('unknown file format \'' + str(format) + '\'')
        return format.upper()
    else:
        # Try to guess the file format from the file suffix
        if filename.lower().endswith('.gz'):
            suffix = filename.split('.')[-2].lower()
        else:
            suffix = filename.split('.')[-1].lower()
        if suffix in ['json']:
            return JSON
        elif suffix in ['yaml', 'yml']:
            return YAML
        else:
            return JSON

def open_file(filename, mode, compressed):
    """Open the given file. If the compressed flag is True it is assumed that
    the file is gzipped. The mode is expected to be either 'r' or 'w'.

    Parameters
    ----------
    filename: string
    mode: string
    compressed: bool, optional

    Returns
    -------
    FileObject
    """
    if compressed:
        return gzip.open(filename, mode + 'b')
    else:
        return open(filename, mode)

def write_document(doc, f, format):
    """Write the given document to file. The format determines whether the file
    is written in JSON or in YAML format.

    Parameters
    ----------
    filename: string
    format: string, optional
    """
    if format == JSON:
         json.dump(doc, f)
    else:
        yaml.dump(doc, f, default_flow_style=False)
