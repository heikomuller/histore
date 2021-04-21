# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for reading and writing serialized Json documents."""

from datetime import datetime, time

import json
import numpy as np
import os
import pytest

from histore.document.json.reader import JsonReader, default_decoder
from histore.document.json.writer import JsonWriter, DefaultEncoder
from histore.key import NumberKey

import histore.util as util


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


def test_key_objects():
    """Test encoding and decoding date time objects."""
    doc = {'a': NumberKey(42)}
    obj = json.dumps(doc, cls=DefaultEncoder)
    doc = json.loads(obj, object_hook=default_decoder)
    assert doc['a'] == 42


def test_numpy_objects():
    """Test encoding numpy objects."""
    doc = {'a': 'X', 'b': np.int64(1), 'c': np.float64(1.2), 'd': np.array([1, 2])}
    doc = json.loads(json.dumps(doc, cls=DefaultEncoder))
    assert doc['a'] == 'X'
    assert doc['b'] == 1
    assert doc['c'] == 1.2
    assert doc['d'] == [1, 2]
    # Edge cases --------------------------------------------------------------
    encoder = DefaultEncoder()
    assert encoder.default(np.float64(1.2)) == 1.2
    assert encoder.default('abc') is None


def test_read_empty_file(tmpdir):
    """Test reading from an non existing file."""
    filename = os.path.join(tmpdir, 'output.json')
    reader = JsonReader(filename=filename)
    with pytest.raises(StopIteration):
        next(reader)
    reader.close()


def test_read_from_invalid_file(tmpdir):
    """Test reading from an non existing file."""
    filename = os.path.join(tmpdir, 'output.json')
    with open(filename, 'wt') as f:
        f.write('{}\n')
    with pytest.raises(ValueError):
        JsonReader(filename=filename)


@pytest.mark.parametrize('compression', ['gzip', None])
def test_read_write_document(compression, tmpdir):
    """Test reading and writing a Json document."""
    filename = os.path.join(tmpdir, 'output.json')
    input = [
        ['Alice', 23, 178.67, util.to_datetime('2020-01-15')],
        ['Bob', 43, 165.30, util.to_datetime('2020-03-31')]
    ]
    with JsonWriter(filename=filename, compression=compression) as writer:
        for row in input:
            writer.write(row)
    rows = [row for row in JsonReader(filename=filename, compression=compression)]
    assert rows == input
