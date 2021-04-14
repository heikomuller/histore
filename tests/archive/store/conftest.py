# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Fixtures for archive store unit tests."""

import pandas as pd
import pytest


@pytest.fixture
def empty_dataset():
    """Get an empty dataset with two columns."""
    return pd.DataFrame(data=[], columns=['Name', 'Age'])
