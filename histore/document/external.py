# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Abstract class for external documents that maintain data in a file on the
local file system.
"""

from typing import List

import os
import pandas as pd

from histore.document.base import Document
from histore.document.schema import Schema


class ExternalDocument(Document):
    """External document that maintains data in a file in disk."""
    def __init__(self, columns: Schema, filename: str, delete_on_close: bool):
        """Initialize the document schema and the reference to the data file.

        Parameters
        ----------
        columns: list of string
            List of column names.
        filename: string
            Path to the data file.
        delete_on_close: bool
            If True, delete the file when the document is closed.
        """
        super(ExternalDocument, self).__init__(columns=columns)
        self.filename = filename
        self.delete_on_close = delete_on_close

    def close(self):
        """Delete the file if the ``delete_on_close`` Flag is True."""
        if self.delete_on_close:
            os.unlink(self.filename)

    def read_df(self) -> pd.DataFrame:
        """Create data frame from the document rows.

        Returns
        -------
        pd.DataFrame
        """
        data, index = list(), list()
        for rowid, row in self.iterrows():
            index.append(rowid)
            data.append(row)
        return pd.DataFrame(data=data, index=index, columns=self.columns, dtype=object)

    def sorted(self, keys: List[int]) -> Document:
        """Sort the document rows based on the values in the key columns.

        Key columns are specified by their index position. Returns a new
        document.

        Parameters
        ----------
        keys: list of int
            Index position of sort columns.

        Returns
        -------
        histore.document.base.Document
        """
        raise NotImplementedError()  # pragma: no cover
