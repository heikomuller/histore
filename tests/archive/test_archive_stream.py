# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the dataset snapshot stream."""

import pandas as pd
import pytest

from histore.archive.base import Archive
from histore.document.mem.dataframe import DataFrameDocument
from histore.tests.base import DataFrameStream


SNAPSHOT_1 = [['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]]
SNAPSHOT_2 = [['Alice', 32], ['Bob', 44], ['Claire', 27], ['Dave', 23]]
SNAPSHOT_3 = [['Dave', 33], ['Eve', 25], ['Claire', 27], ['Bob', 44], ['Alice', 32]]
SNAPSHOT_4 = [['Alice', 32], ['Eve', 25], ['Claire', 27], ['Bob', 44]]


@pytest.mark.parametrize('doccls', [DataFrameDocument, DataFrameStream])
def test_load_unkeyed_archive(doccls):
    """Test committing snaposhots into an unkeyed archive."""
    # -- Setup - Create archive in main memory --------------------------------
    archive = Archive()
    for data in [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3, SNAPSHOT_4]:
        doc = doccls(df=pd.DataFrame(data=data, columns=['Name', 'Age']))
        archive.commit(doc)
    # -- Stream first snapshot ------------------------------------------------
    stream = archive.stream(version=0)
    assert stream.columns == ['Name', 'Age']
    with stream.open() as s:
        assert [values for _, values in s] == SNAPSHOT_1
    with archive.stream(version=1).open() as s:
        assert [values for _, values in s] == SNAPSHOT_2
    with archive.stream(version=2).open() as s:
        assert [values for _, values in s] == SNAPSHOT_3
    with archive.stream().open() as s:
        assert [values for _, values in s] == SNAPSHOT_4
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(ValueError):
        archive.stream(version=5)
