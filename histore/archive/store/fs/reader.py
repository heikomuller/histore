# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Reader for archive rows that are maintained as a Json array in a file on the
file system.
"""

from typing import Callable, Optional

from histore.archive.reader import ArchiveReader
from histore.archive.serialize.base import ArchiveSerializer
from histore.archive.serialize.default import DefaultSerializer
from histore.document.json.reader import JsonReader


class ArchiveFileReader(ArchiveReader):
    """Reader for rows in a dataset archive. Reads rows in ascending order of
    their identifier.
    """
    def __init__(
        self, filename: str, serializer: Optional[ArchiveSerializer] = None,
        compression: Optional[str] = None, decoder: Optional[Callable] = None
    ):
        """
        Parameters
        ----------
        filename: string
            Path to the output file.
        serializer: histore.archive.serialize.base.ArchiveSerializer, default=None
            Implementation of the archive serializer interface that is used to
            serialize rows that are written to file.
        compression: string, default=None
            String representing the compression mode for the output file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        """
        self.serializer = serializer if serializer else DefaultSerializer()
        # Create Json reader for the input file.
        self.reader = JsonReader(
            filename=filename,
            compression=compression,
            decoder=decoder
        )

    def close(self):
        """Release all reseources that are associated with the reader."""
        self.reader.close()

    def next(self):
        """Read the next row in the dataset archive. Returns None if the end of
        the archive rows has been reached.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        try:
            return self.serializer.deserialize_row(next(self.reader))
        except StopIteration:
            self.close()
            return None
