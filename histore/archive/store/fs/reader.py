# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Reader for archive rows that are maintained as a Json array in a file on the
file system.
"""

import json
import os

from histore.archive.reader import ArchiveReader
from histore.archive.serialize.default import DefaultSerializer

import histore.util as util


class ArchiveFileReader(ArchiveReader):
    """Reader for rows in a dataset archive. Reads rows in ascending order of
    their identifier.
    """
    def __init__(self, filename, serializer=None, compression=None):
        """
        Parameters
        ----------
        filename: string
            Path to the output file.
        serializer: histore.archive.serialize.ArchiveSerializer, default=None
            Implementation of the archive serializer interface that is used to
            serialize rows that are written to file.
        compression: string, default=None
            String representing the compression mode for the output file.
        """
        self.serializer = serializer if serializer else DefaultSerializer()
        # The archive is empty if the given file does not exists. In this case
        # the buffer will remain empty,
        self.buffer = None
        if os.path.isfile(filename):
            self.fin = util.inputstream(filename, compression=compression)
            # Read the first two lines in the output file. The first line is
            # expected to be '['. The second line is either ']' (for an empty
            # archive) or contain the first row.
            if self.fin.readline() != '[':
                raise ValueError('invalid input file {}'.format(filename))
            self.next()
        else:
            self.fin = None

    def has_next(self):
        """The next row for the reader is the current buffer value. If the
        buffer is empty there is no next row.

        Returns
        -------
        bool
        """
        return self.buffer is not None

    def next(self):
        """Read the next row in the dataset archive. Returns None if the end of
        the archive rows has been reached.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        # The end of the rows has been reached if the input stream is None.
        if not self.fin:
            return None
        # The return value is the current buffer value.
        result = self.buffer
        # Read the next line. If the line equals the closing array bracket the
        # end of the archive has been reached.
        line = self.fin.readline()
        if line == ']':
            # The end of the archive was reached. Close the input file.
            self.buffer = None
            self.fin.close()
            self.fin = None
        else:
            # Remove trailing comma for list array elements before the final
            # element.
            if line.endswith(','):
                line = line[:-1]
            self.buffer = self.serializer.deserialize_row(json.loads(line))
        # Return the previous buffer value as the result.
        return result
