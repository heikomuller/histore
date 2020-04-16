# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the snapshot reader."""

import pandas as pd
import pytest

from histore.snapshot.base import Snapshot


def test_read_dataframe():
    """Test reading simple data frames."""
    # All row identifier are -1
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=['A', 'B', 'C'])
    reader = Snapshot(df=df, columns=[1, 2]).reader()
    rowcount = 0
    while reader.has_next():
        row = reader.next()
        assert row.identifier == -1
        assert row.pos == rowcount
        assert row.values[1] == ((rowcount + 1) * 2) - 1
        rowcount += 1
    assert rowcount == 3
    # Positive row identifier
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=[1, 2, 3])
    reader = Snapshot(df=df, columns=[1, 2]).reader()
    rowcount = 0
    while reader.has_next():
        row = reader.next()
        assert row.identifier == rowcount + 1
        assert row.pos == rowcount
        assert row.values[1] == ((rowcount + 1) * 2) - 1
        rowcount += 1
    assert rowcount == 3
    # Mixed row identifier
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=[1, 2, 'A'])
    reader = Snapshot(df=df, columns=[1, 2]).reader()
    rowids = list()
    while reader.has_next():
        row = reader.next()
        rowids.append(row.identifier)
    assert rowids == [1, 2, -1]
    # Error for non-unique row identifier
    df = pd.DataFrame(data=[[1, 2], [3, 4], [5, 6]], index=[1, 2, 1])
    with pytest.raises(ValueError):
        Snapshot(df=df, columns=[1, 2])
