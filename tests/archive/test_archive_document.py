# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the to_document helper function."""

import os
import pandas as pd
import pytest

from histore.archive.base import to_document
from histore.document.csv.base import CSVFile
from histore.document.csv.read import SimpleCSVDocument
from histore.document.mem.dataframe import DataFrameDocument
from histore.document.schema import Column
from histore.tests.base import DataFrameStream


DIR = os.path.dirname(os.path.realpath(__file__))
CSV_FILE = os.path.join(DIR, '../.files/agencies.csv')


@pytest.mark.parametrize(
    'input_document',
    [
        SimpleCSVDocument(CSVFile(CSV_FILE)),
        DataFrameDocument(pd.read_csv(CSV_FILE)),
        DataFrameStream(df=pd.read_csv(CSV_FILE))
    ]
)
@pytest.mark.parametrize('max_size', [(100 / (1024 * 1024)), (16 * 1024 * 1024)])
def test_read_document_sorted(input_document, max_size):
    """Read document with primary key that needs to be sorted."""
    doc = to_document(doc=input_document, keys=[1, 0], sorted=False, max_size=max_size)
    assert doc.columns == ['agency', 'borough', 'state']
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    keys = [(r.values[1], r.values[0]) for r in doc.reader(schema=schema)]
    assert keys[:3] == [('BK', '311'), ('BK', 'DOB'), ('BK', 'DOE')]
    assert keys[-3:] == [('BX', 'FDNY'), ('MN', 'FDNY'), ('MN', 'NYPD')]


@pytest.mark.parametrize(
    'input_document',
    [
        SimpleCSVDocument(CSVFile(CSV_FILE)),
        DataFrameDocument(pd.read_csv(CSV_FILE)),
        DataFrameStream(df=pd.read_csv(CSV_FILE))
    ]
)
def test_read_document_unsorted(input_document):
    """Read document in original order."""
    doc = to_document(doc=input_document, keys=[1, 0], sorted=True)
    assert doc.columns == ['agency', 'borough', 'state']
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    keys = [(r.values[1], r.values[0]) for r in doc.reader(schema=schema)]
    assert keys[:3] == [('BK', '311'), ('BK', 'NYPD'), ('BK', 'FDNY')]
    assert keys[-3:] == [('MN', 'FDNY'), ('BX', 'DOE'), ('BX', 'FDNY')]
