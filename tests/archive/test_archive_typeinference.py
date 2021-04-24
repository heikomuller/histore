# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests to ensure that type inference is switched off when checking out
a dataset snapshot as a data frame (issue #24).
"""

import pandas as pd

from histore.archive.base import Archive
from histore.document.df import DataFrameDocument


def test_data_frame_checkout():
    """Test commit and checkout for data frame with values of mixed type."""
    archive = Archive()
    # Version 0
    df = pd.DataFrame(
        data=[['Alice', 1], ['Bob', 1], [1, 1.4]],
        columns=['Name', 'Val'],
        dtype=object
    )
    archive.commit(DataFrameDocument(df))
    df = archive.checkout(version=0)
    assert isinstance(df.iloc[0, 0], str)
    assert isinstance(df.iloc[2, 0], int)
    assert isinstance(df.iloc[0, 1], int)
    assert isinstance(df.iloc[2, 1], float)
