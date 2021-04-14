# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for the input document snapshot descriptor."""

from histore.document.snapshot import InputDescriptor

import histore.util as util


def test_input_descriptor():
    """Test creating instance of the input descriptor."""
    descriptor = InputDescriptor()
    assert descriptor.valid_time is None
    assert descriptor.description == ''
    assert descriptor.action is None
    now = util.utc_now()
    descriptor = InputDescriptor(valid_time=now)
    assert descriptor.valid_time == now
    assert descriptor.description == ''
    assert descriptor.action is None
    descriptor = InputDescriptor(valid_time=now, description='abc', action=dict({'x': 1}))
    assert descriptor.valid_time == now
    assert descriptor.description == 'abc'
    assert descriptor.action == dict({'x': 1})
