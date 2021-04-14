# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Writer for archives that are materialized as Json files on the file
system.
"""

from typing import Optional

import json


from histore.archive.serialize.base import ArchiveSerializer
from histore.archive.serialize.default import DefaultSerializer
from histore.archive.row import ArchiveRow
from histore.archive.writer import ArchiveWriter
from histore.document.json.writer import JsonWriter


class ArchiveFileWriter(ArchiveWriter):
    """Archive writer that outputs rows in an archive as Json serialized rows
    in a text file. Each row is stored in a separate line in the text file. The
    output file is a Json array. The first and the last row of the file open
    and close the array.
    """
    def __init__(
        self, filename: str, row_counter: Optional[int] = 0,
        serializer: Optional[ArchiveSerializer] = None,
        compression: Optional[str] = None,
        encoder: Optional[json.JSONEncoder] = None
    ):
        """Initialize the output file, row counter, and the serializer that is
        being used.

        Parameters
        ----------
        filename: string
            Path to the output file.
        row_counter: int, default=0
            Counter that is used to generate unique internal row identifier.
            The current value of the counter is the value for the next unique
            identifier.
        serializer: histore.archive.serialize.base.ArchiveSerializer, default=None
            Implementation of the archive serializer interface that is used to
            serialize rows that are written to file.
        compression: string, default=None
            String representing the compression mode for the output file.
        encoder: json.JSONEncoder, default=None
            Encoder used when writing archive rows as JSON objects to file.
        """
        super(ArchiveFileWriter, self).__init__(row_counter)
        # Use the default serializer if no serializer was given
        self.serializer = serializer if serializer else DefaultSerializer()
        # Create Json writer for the archive rows.
        self.writer = JsonWriter(
            filename=filename,
            compression=compression,
            encoder=encoder
        )

    def close(self):
        """Write the last row to the output file and close the output array and
        the output file.
        """
        self.writer.close()

    def write_archive_row(self, row: ArchiveRow):
        """Add the given row to the output file.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        self.write_buffer(row)

    def write_buffer(self, row: Optional[ArchiveRow] = None):
        """Write the archive row in the internal buffer to the output file.
        Replace the buffer with the given (next output row).

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow, default=None
            Next row in the output stream. This row will be kept in the
            internal buffer and the previous row is being written to the
            output file.
        """
        self.writer.write(self.serializer.serialize_row(row))
