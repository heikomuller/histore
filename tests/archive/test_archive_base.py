# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for base functionality and edge cases of the archive base module."""

import os
import pandas as pd
import pytest

from histore.archive.base import to_document
from histore.document.base import Document
from histore.document.csv.base import CSVFile


DIR = os.path.dirname(os.path.realpath(__file__))
FILENAME = os.path.join(DIR, '../.files/agencies.csv')


@pytest.mark.parametrize(
    'doc', [
        pd.DataFrame(data=[[1, 2], [3, 4]], columns=['A', 'B']),
        FILENAME,
        CSVFile(filename=FILENAME)
    ]
)
def test_input_to_document(doc):
    """Test converting inputs to documents."""
    doc = to_document(doc=doc)
    assert isinstance(doc, Document)
