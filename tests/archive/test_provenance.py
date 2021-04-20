# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for the default provenance descriptor and provenance object edge
cases.
"""

import pytest

from histore.archive.provenance.base import Provenance, ProvOp
from histore.archive.provenance.column import UpdateColumn
from histore.archive.provenance.row import UpdateRow
from histore.archive.provenance.value import UpdateValue


def test_empty_provenance_trace():
    """Test printing details about an empty provenance trace using the default
    provenance descriptor.
    """
    Provenance().describe()


def test_invalid_provenance_type():
    """Test error when instantiating provenance operator with an invalid
    provenance type.
    """
    with pytest.raises(ValueError):
        ProvOp(type='UNKNOWN', key=0)


def test_invalid_update_column():
    """Test error when instantiating update column without name and position
    arguments.
    """
    prov = UpdateColumn(key=0, name='A')
    assert prov.is_update()
    with pytest.raises(ValueError):
        UpdateColumn(key=0)


def test_invalid_update_row():
    """Test error when instantiating update row without cells and position
    arguments.
    """
    prov = UpdateRow(key=0, position=UpdateValue(0, 1))
    assert prov.is_update()
    with pytest.raises(ValueError):
        UpdateRow(key=0)
