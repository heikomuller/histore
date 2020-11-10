# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

import pandas as pd
import pytest

from histore.document.schema import Column


@pytest.fixture
def dupcols():
    """Get a simple data frame with duplicate column names."""
    data = [
        ['Alice', 23, 180],
        ['Bob', 32, 179],
        ['Claudia', 37, 184],
        ['Dave', 45, 176],
        ['Eileen', 29, 168],
        ['Frank', 34, 198],
        ['Gertrud', 44, 177]
    ]
    columns = [Column(0, 'Name', 0), Column(1, 'A', 1), Column(2, 'A', 2)]
    return pd.DataFrame(data=data, columns=columns)


@pytest.fixture
def employees():
    """Get a simple data frame with the name, age, and salary of seven
    employees.
    """
    data = [
        ['Alice', 23, 60000],
        ['Bob', 32, ''],
        ['Claudia', 37, '21k'],
        ['Dave', None, 34567],
        ['Eileen', 29, 34598.87],
        ['Frank', 34, '23'],
        ['Gertrud', 44, '120050.5']
    ]
    columns = [Column(0, 'Name'), Column(1, 'Age'), Column(2, 'Salary')]
    return pd.DataFrame(data=data, columns=columns)
