# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Reader for lists of objects that are maintained as a serialized Json array
in a file on the file system.
"""

from datetime import datetime
from dateutil.parser import isoparse
from typing import Any, Callable, Dict, Optional

import json
import os

import histore.util as util


class JsonReader(object):
    """Reader for rows in a Json file."""
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the input file and the Json decoder.

        Parameters
        ----------
        filename: string
            Path to the output file.
        compression: string, default=None
            String representing the compression mode for the output file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        """
        self.decoder = decoder if decoder is not None else default_decoder
        self.buffer = None
        if os.path.isfile(filename):
            self.fin = util.inputstream(filename, compression=compression)
            # Read the first two lines in the input file. The first line is
            # expected to be '['. The second line is either ']' (for an empty
            # file) or it contains the first object.
            if self.fin.readline() != '[':
                raise ValueError('invalid input file {}'.format(filename))
            self.next()
        else:
            self.fin = None

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        self.close()
        return False

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def __next__(self):
        """Return next row from JSON reader. Raise a StopIteration error when
        the end of the file is reached.
        """
        if not self.has_next():
            raise StopIteration()
        return self.next()

    def close(self):
        """Release all reseources that are associated with the reader."""
        self.buffer = None
        if self.fin is not None:
            self.fin.close()
        self.fin = None

    def has_next(self) -> bool:
        """The next row for the reader is the current buffer value.

        If the buffer is empty the end of the file has been reached.

        Returns
        -------
        bool
        """
        return self.buffer is not None

    def next(self) -> Any:
        """Read the next row in the file.

        Returns None if the end of the file has been reached.

        Returns
        -------
        any
        """
        # The return value is the current buffer value.
        result = self.buffer
        # Read the next line. If the line equals the closing array bracket the
        # end of the archive has been reached.
        line = self.fin.readline()
        if line == ']':
            # The end of the archive was reached. Close the input file.
            self.close()
        else:
            # Remove trailing comma for list array elements before the final
            # element.
            if line.endswith(','):
                line = line[:-1]
            # Decode and deserialize the read object.
            self.buffer = json.loads(line, object_hook=self.decoder)
        # Return the previous buffer value as the result.
        return result


# -- Helper Functions ---------------------------------------------------------

def default_decoder(obj: Dict) -> Any:
    """Default Json object decoder. Accounts for date types that have been
    encoded as dictionaries.

    Parameters
    ----------
    obj: dict
        Json object that is being encountered by the Json reader.

    Returns
    -------
    any
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
