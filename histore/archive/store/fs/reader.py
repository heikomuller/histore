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

from datetime import datetime
from dateutil.parser import isoparse

from histore.archive.reader import ArchiveReader
from histore.archive.serialize.default import DefaultSerializer

import histore.util as util


class ArchiveFileReader(ArchiveReader):
    """Reader for rows in a dataset archive. Reads rows in ascending order of
    their identifier.
    """
    def __init__(
        self, filename, serializer=None, compression=None, decoder=None
    ):
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
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        """
        self.serializer = serializer if serializer else DefaultSerializer()
        # Use the default decoder if None is given.
        self.decoder = decoder if decoder is not None else default_decoder
        # The archive is empty if the given file does not exists. In this case
        # the buffer will remain empty.
        self.buffer = None
        if os.path.isfile(filename):
            self.fin = util.inputstream(filename, compression=compression)
            # Read the first two lines in the output file. The first line is
            # expected to be '['. The second line is either ']' (for an empty
            # archive) or it contains the first row.
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
            # Decode and deserialize the archive row object.
            obj = json.loads(line, object_hook=self.decoder)
            self.buffer = self.serializer.deserialize_row(obj)
        # Return the previous buffer value as the result.
        return result


# -- Helper Functions ---------------------------------------------------------

def default_decoder(obj):
    """Default Json object decoder. Accounts for date types that have been
    encoded as dictionaries.

    Parameters
    ----------
    obj: dict
        Json object that is being encountered by the Json reader

    Returns
    -------
    dict
    """
    if '$datetime' in obj:
        return isoparse(obj['$datetime'])
    elif '$date' in obj:
        return isoparse(obj['$date']).date()
    elif '$time' in obj:
        val = obj['$time']
        if '.' in val:
            return datetime.strptime(val, '%H:%M:%S.%f').time()
        else:
            return datetime.strptime(val, '%H:%M:%S').time()
    return obj
