# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for sorting data frame documents."""

import os
import pandas as pd
import pytest

from histore.document.df import DataFrameDocument

DIR = os.path.dirname(os.path.realpath(__file__))
DATAFILE = os.path.join(DIR, '../.files/agencies.csv')


@pytest.fixture
def dataset():
    """Get data frame for test purposes."""
    return pd.read_csv(DATAFILE)


def test_sort_by_agency(dataset):
    """Test sorting by a single attribute."""
    doc = DataFrameDocument(df=dataset).sorted(keys=[0])
    assert doc.columns == ['agency', 'borough', 'state']
    with doc.open() as reader:
        names = [values[0] for _, _, values in reader]
    assert names == ['311', 'DOB', 'DOE', 'DOE', 'DSNY', 'FDNY', 'FDNY', 'FDNY', 'NYPD', 'NYPD']


def test_sort_by_borough_and_name(dataset):
    """Test sorting by multiple attributes."""
    doc = DataFrameDocument(df=dataset).sorted(keys=[1, 0])
    assert doc.columns == ['agency', 'borough', 'state']
    with doc.open() as reader:
        names = [values[0] for _, _, values in reader]
    assert names == ['311', 'DOB', 'DOE', 'DSNY', 'FDNY', 'NYPD', 'DOE', 'FDNY', 'FDNY', 'NYPD']
