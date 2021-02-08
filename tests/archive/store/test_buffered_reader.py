# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the buffered archive reader."""

import pandas as pd

from histore.archive.base import Archive


def test_archive_reader_iterate(tmpdir):
    """Test reading rows in an archive using the row iterator provided by the
    archive reader.
    """
    # Create archive in main memory
    archive = Archive()
    # First snapshot
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    reader = archive.reader()
    for row in reader:
        assert row.rowid >= 0
