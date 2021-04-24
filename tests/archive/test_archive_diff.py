# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive reader and archive writer of the file system
archive store.
"""

import pandas as pd

from histore.archive.base import Archive


def test_archive_diff(tmpdir):
    """Test difference between two archive snapshots."""
    # Create archive in main memory
    archive = Archive()
    # First snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]],
        columns=['Name', 'Age']
    )
    s1 = archive.commit(df)
    # Second snapshot
    df = pd.DataFrame(
        data=[
            [32, 'Alice', 180],
            [44, 'Bob', 175],
            [27, 'Claire', 167],
            [23, 'Dave', 175]
        ],
        index=[0, 1, 2, 3],
        columns=['Age', 'Name', 'Height']
    )
    s2 = archive.commit(df)
    # Third snapshot
    df = pd.DataFrame(
        data=[
            [44, 'Bob', 176],
            [32, 'Alice', 180],
            [23, 'Dave', 175]
        ],
        index=[1, 0, 3],
        columns=['Height', 'Age', 'Name']
    )
    s3 = archive.commit(df)
    # Fourth snapshot
    df = pd.DataFrame(
        data=[
            [44, 'Bob', 176],
            [33, 'Alice', 180],
            [23, 'Dave', 175]
        ],
        index=[1, 0, 3],
        columns=['Size', 'Age', 'Name']
    )
    s4 = archive.commit(df, renamed=[('Height', 'Size')])
    # Difference between s1 and s2
    diff = archive.diff(s1.version, s2.version)
    assert len(diff.schema().delete()) == 0
    assert len(diff.schema().insert()) == 1
    assert len(diff.schema().update()) == 2
    assert len(diff.rows().delete()) == 0
    assert len(diff.rows().insert()) == 0
    assert len(diff.rows().update()) == 4
    diff = archive.diff(1, 2)
    # Ensure that we can print provenance information without error.
    diff.describe()
    # Ensure that we can print individual provenance items without an error.
    for row in diff.rows().update():
        for cell in row.cells.values():
            assert str(cell) is not None
    # Difference between s2 and s3
    diff = archive.diff(s2.version, s3.version)
    assert len(diff.schema().delete()) == 0
    assert len(diff.schema().insert()) == 0
    assert len(diff.schema().update()) == 3
    assert len(diff.rows().delete()) == 1
    assert len(diff.rows().insert()) == 0
    assert len(diff.rows().update()) == 3
    diff.describe()
    # Difference between s3 and s4
    diff = archive.diff(s3.version, s4.version)
    assert len(diff.schema().delete()) == 0
    assert len(diff.schema().insert()) == 0
    assert len(diff.schema().update()) == 1
    assert len(diff.rows().delete()) == 0
    assert len(diff.rows().insert()) == 0
    assert len(diff.rows().update()) == 1
    diff.describe()
