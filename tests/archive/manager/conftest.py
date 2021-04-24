# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Fixtures for archive manager unit tests."""

import pandas as pd
import pytest


@pytest.fixture
def dataset():
    """Get a simple data frame with SSN and Name columns."""
    return pd.DataFrame(
        data=[['1234', 'Alice'], ['2345', 'Bob']],
        columns=['SSN', 'Name']
    )
