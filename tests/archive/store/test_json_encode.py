# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for the default JSON encoder and decoder."""

import json
import numpy as np

from datetime import datetime, time

from histore.archive.store.fs.reader import default_decoder
from histore.archive.store.fs.writer import DefaultEncoder


def test_datetime_objects():
    """Test encoding and decoding date time objects."""
    dt = datetime.now()
    d = dt.date()
    t = dt.time()
    doc = {'a': 'X', 'b': dt, 'c': d, 'd': t, 'e': time(11, 15)}
    obj = json.dumps(doc, cls=DefaultEncoder)
    doc = json.loads(obj, object_hook=default_decoder)
    assert doc['a'] == 'X'
    assert doc['b'] == dt
    assert doc['c'] == d
    assert doc['d'] == t
    assert doc['e'] == time(11, 15)


def test_numpy_objects():
    """Test encoding numpy objects."""
    doc = {'a': 'X', 'b': np.int(1), 'c': np.float(1.2), 'd': np.array([1, 2])}
    doc = json.loads(json.dumps(doc, cls=DefaultEncoder))
    assert doc['a'] == 'X'
    assert doc['b'] == 1
    assert doc['c'] == 1.2
    assert doc['d'] == [1, 2]
