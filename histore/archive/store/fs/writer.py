# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Writer for archives that are materialized as Json files on the file
system.
"""

import json
import numpy as np

from datetime import datetime, date, time

from histore.key.base import KeyValue
from histore.archive.serialize.default import DefaultSerializer
from histore.archive.writer import ArchiveWriter

import histore.util as util


class ArchiveFileWriter(ArchiveWriter):
    """Archive writer that outputs rows in an archive as Json serialized rows
    in a text file. Each row is stired ina separate line in the text file. The
    output file is a Json array. The first and the last row of the file open
    and close the array.
    """
    def __init__(
        self, filename, row_counter=0, serializer=None, compression=None,
        encoder=None
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
        serializer: histore.archive.serialize.ArchiveSerializer, default=None
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
        # Use the default JSONEncoder if no encoder is given
        self.encoder = encoder if encoder is not None else DefaultEncoder
        # Open output file for writing.
        self.fout = util.outputstream(filename, compression=compression)
        # Write buffer to keep track of the last row that is being written.
        self.buffer = None

    def close(self):
        """Write the last row to the output file and close the output array and
        the output file.
        """
        # Write last row in to output buffer.
        self.write_buffer()
        # Close Json array and the output file.
        self.fout.writeline(']')
        self.fout.close()

    def write_archive_row(self, row):
        """Add the given row to the output file.

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow
            Row in a new version of a dataset archive.
        """
        self.write_buffer(row)

    def write_buffer(self, row=None):
        """Write the archive row in the internal buffer to the output file.
        Replace the buffer with the given (next output row).

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow, default=None
            Next row in the output stream. This row will be kept in the
            internal buffer and the previous row is being written to the
            output file.
        """
        # Depending on whether there is a previous row in the buffer we either
        # output that row or open the output array.
        if self.buffer is not None:
            text = self.serializer.serialize_row(self.buffer)
            line = json.dumps(text, cls=self.encoder)
            if row is not None:
                line += ','
        else:
            line = '['
        self.fout.writeline(line)
        # Buffer the given row
        self.buffer = row


# -- Helper classes -----------------------------------------------------------

class DefaultEncoder(json.JSONEncoder):
    """Json encoder that handles numpy data types from pandas data frames and
    datetime objects.
    """
    def default(self, obj):
        """Convert numpy data types to default Python types Date objects are
        converted to dictionaries using the class an label (prefixed by the
        special character '$') and the ISO format string representation as the
        value.

        Parameters
        ----------
        obj: any
            Value that is being encoded.

        Returns
        -------
        any
        """
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime):
            return {'$datetime': obj.isoformat()}
        elif isinstance(obj, date):
            return {'$date': obj.isoformat()}
        elif isinstance(obj, time):
            return {'$time': obj.isoformat()}
        elif isinstance(obj, KeyValue):
            assert not obj.is_new()
            return obj.value
        else:
            return super(DefaultEncoder, self).default(obj)
