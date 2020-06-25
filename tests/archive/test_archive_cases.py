# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for special (error) cases for the archive object."""

import pandas as pd
import pytest

from histore.archive.base import Archive
from histore.document.mem.dataframe import DataFrameDocument


def test_special_checkout_cases():
    """Test various special (error) cases for commit and checkout."""
    archive = Archive()
    # Version 0
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 1], ['Claire', 1], ['Alice', 2]],
        columns=['Name', 'Index']
    )
    s = archive.commit(DataFrameDocument(df))
    assert s.version == 0
    # Last version is checked out by default.
    df = archive.checkout(version=0)
    assert list(df.index) == [0, 1, 2, 3]
    df = archive.checkout()
    assert list(df.index) == [0, 1, 2, 3]
    # Partial merge with given origin.
    df = pd.DataFrame(data=[['Alice', 1]], columns=df.columns)
    s = archive.commit(df, origin=0)
    assert s.version == 1
    # Checkout an unknown version.
    with pytest.raises(ValueError):
        archive.checkout(version=10)


def test_partial_commit_to_empty_archive():
    """Test error when committing a partial snapshot to an empty archive."""
    df = pd.DataFrame(data=[[1]])
    with pytest.raises(ValueError):
        Archive().commit(df, partial=True)
