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

    @staticmethod
    def create(archive=None, filename, format=None, overwrite=False):
        """Create a new persistent archive on disk.

        Returns
        -------
        histore.archive
        """
        pass
