# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the configuration helper functions."""

import os
import pytest

import histore.config as config


@pytest.mark.parametrize(
    'var,func,value,default_is_none',
    [
        (config.ENV_HISTORE_BASEDIR, config.BASEDIR, 'ABC', False),
        (config.ENV_HISTORE_DBCONNECT, config.DBCONNECT, 'XYZ', True),
        (config.ENV_HISTORE_SORTBUFFER, config.SORTBUFFER, 33.4, False)
    ]
)
def test_get_config(var, func, value, default_is_none):
    """Test helper functon to get different configuration parameters from the
    respective environment variables.
    """
    # -- Setup ----------------------------------------------------------------
    if var in os.environ:
        del os.environ[var]
    # -- Test default value ---------------------------------------------------
    assert (func() is None) == default_is_none
    # -- Test getting value from variable -------------------------------------
    os.environ[var] = str(value)
    assert func() == value
    # -- Cleanup --------------------------------------------------------------
    del os.environ[var]
