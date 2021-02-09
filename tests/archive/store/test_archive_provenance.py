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


def test_archive_read_write(tmpdir):
    """Test writing an exsiting archive that uses row indices as keys to file
    and then reading it.
    """
    # Create archive in main memory
    archive = Archive()
    # First snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
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
    archive.commit(df)
    diff = archive.diff(0, 1)
    assert len(diff.schema().delete()) == 0
    assert len(diff.schema().insert()) == 1
    assert len(diff.schema().update()) == 2
    assert len(diff.rows().delete()) == 0
    assert len(diff.rows().insert()) == 0
    assert len(diff.rows().update()) == 4
    # Ensure that we can print provenance information without error.
    diff.describe()
    # Ensuer that we can print individual provenance items without an error.
    for row in diff.rows().update():
        for cell in row.cells.values():
            print(str(cell))
