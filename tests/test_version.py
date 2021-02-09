# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for version information."""

import histore.version as version


def test_version():
    """For completeness, assert that the version information variable is
    defined.
    """
    assert version.__version__ is not None
