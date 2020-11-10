# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for the default provenance descriptor."""

from histore.archive.provenance.base import Provenance


def test_empty_provenance_trace():
    """Test printing details about an empty provenance trace using the default
    provenance descriptor.
    """
    Provenance().describe()
