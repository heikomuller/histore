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
from typing import Any, Callable, Dict, Optional, Tuple

import json
import os

from histore.document.base import DataRow, DocumentIterator, RowIndex

import histore.util as util


class JsonReader(object):
    """Abstract reader for rows in a Json file. Returns rows that are Json
    objects. The reader is agnostice to the format of these rows. The format is
    implementation dependent and different sub-classes will implement their own
    deserialization function.
    """
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the input file, the row deserializer, and the Json
        decoder.

        Parameters
        ----------
        filename: string
            Path to the input file.
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
            next(self)
        else:
            self.fin = None

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def close(self):
        """Release all reseources that are associated with the reader."""
        self.buffer = None
        if self.fin is not None:
            self.fin.close()
        self.fin = None

    def __next__(self) -> Any:
        """Read the next row in the file.

        Raises a StopIteration error if an attempts is made to read past the
        end of the file.

        Returns
        -------
        any
        """
        # The return value is the current buffer value.
        result = self.buffer
        # Read the next line. If the line equals the closing array bracket the
        # end of the archive has been reached. If the reader has been closed
        # the file handle is None and an AttributeError will occur.
        try:
            line = self.fin.readline()
        except AttributeError:
            # Raise error to signal that the end of the file has been reached.
            raise StopIteration()
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


class JsonIterator(DocumentIterator):
    """Document iterator for documents that have been serialized as Json
    documents. The iterator expects a file that contains a list if lists (rows).
    The first row is expected to contain the document schema. Data rows are
    expected to be triples of row position, row index, and cell values.
    """
    def __init__(
        self, filename: str, compression: Optional[str] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the input file, the row deserializer, and the Json
        decoder.

        Parameters
        ----------
        filename: string
            Path to the data file.
        compression: string, default=None
            String representing the compression mode for the output file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file. If not
            given, the default decoder will be used.
        """
        self.reader = JsonReader(
            filename=filename,
            compression=compression,
            decoder=decoder
        )
        # Skip the column name row.
        try:
            next(self.reader)
        except StopIteration:
            pass

    def close(self):
        """Close the associated Json reader."""
        self.reader.close()

    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the document.

        Returns the row position, row index and the list of cell values for each
        of the document columns. Raises a StopIteration error if an attempt is
        made to read past the end of the document.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        rowpos, rowidx, values = next(self.reader)
        return rowpos, rowidx, values


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
