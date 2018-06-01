# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Simple in-memory store for archives."""

from histore.archive.store.base import ArchiveStore


"""Identifier for different file formats."""
JSON = 'JSON'
YAML = 'YAML'

FILE_FORMATS = [JSON, YAML]


class PersistentArchiveStore(ArchiveStore):

    @staticmethod
    def create(filename, format=None, schema=None, overwrite=False):
        """Create a new persistent archive on disk.

        Returns
        -------
        histore.archive
        """
        pass
