# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Writer for documents that are materialized as Json files on the file system."""

from __future__ import annotations
from datetime import datetime, date, time
from typing import Any, Optional

import json
import numpy as np


from histore.key import KeyValue

import histore.util as util


class JsonWriter(object):
    """Writer for objects that are stored as Json serialized rows in a text
    file. Each row is stored in a separate line in the text file. The
    output file is a Json array. The first and the last row of the file open
    and close the array.
    """
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        encoder: Optional[json.JSONEncoder] = None
    ):
        """Initialize the output file, row counter, and the serializer that is
        being used.

        Parameters
        ----------
        filename: string
            Path to the output file.
        compression: string, default=None
            String representing the compression mode for the output file.
        encoder: json.JSONEncoder, default=None
            Encoder used when writing archive rows as JSON objects to file.
        """
        # Use the default JSONEncoder if no encoder is given
        self.encoder = encoder  # if encoder is not None else DefaultEncoder
        # Open output file for writing.
        self.fout = util.outputstream(filename, compression=compression)

    def __enter__(self) -> JsonWriter:
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        """Close the document iterator when the context manager exits."""
        self.close()
        return False

    def close(self):
        """Write the last row to the output file and close the output array and
        the output file.
        """
        self.fout.close()

    def write(self, row: Optional[Any] = None):
        """Write the archive row in the internal buffer to the output file.
        Replace the buffer with the given (next output row).

        Parameters
        ----------
        row: histore.archive.row.ArchiveRow, default=None
            Next row in the output stream. This row will be kept in the
            internal buffer and the previous row is being written to the
            output file.
        """
        self.fout.write('{}\n'.format(json.dumps(row, cls=self.encoder)))


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
