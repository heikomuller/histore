# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Simple in-memory store for archives."""

import gzip
import json
import yaml

from histore.archive.store.base import ArchiveStore


"""Identifier for different file formats."""
JSON = 'JSON'
YAML = 'YAML'

FILE_FORMATS = [JSON, YAML]


class PersistentArchiveStore(ArchiveStore):
    def __init__(self, filename, format=None):
        """Read archive from the given file. If format is not given an attempt
        is made to 'guess' the format from the file suffix. By default, JSON
        format is used.

        Raises ValueError if a given format identifier is invalid.

        Parameters
        ----------
        filename: string
        format: string
        """
        self.compressed = filename.endswith('.gz')
        if self.compressed:
            suffix = filename.split('.')[-2].lower()
        else:
            suffix = filename.split('.')[-1].lower()
        # Determine the file format
        if not format is None:
            self.format = format.upper()
            if not self.format in FILE_FORMATS:
                raise ValueError('unknown file format \'' + str(format) + '\'')
        elif suffix in ['json']:
            self.format = JSON
        elif suffix in ['yaml', 'yml']:
            self.format = YAML
        else:
            self.format = JSON
        # Read the archive information from file. This will raise an exception
        # if the file does not exist.
        if self.compressed:
            f = gzip.open(filename, 'rb')
        else:
            f = open(filename, 'r')
        if self.format == JSON:
            doc = json.loads(f.read())
        else:
            doc = yaml.load(f.read())
        f.close()

    def get_root(self):
        """Get the root node for the archive that is maintained by this store.

        Returns
        -------
        histore.archive.node.ArchiveElement
        """
        return self.root

    def read(self):
        """Read the complete archive information. Returns a triple containing
        the archive root, the list of snapshots, and the archive schema.

        Returns
        -------
        histore.archive.node.ArchiveElement
        list(histore.archive.snapshot.Snapshot)
        histore.schema.document.DocumentSchema
        """
        return (self.root, self.snapshots, self.schema)

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
