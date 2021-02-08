# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for utility functions."""

import os
import pytest

import histore.util as util


def test_createdir(tmpdir):
    """Test the create directory function."""
    dir1 = util.createdir(os.path.join(str(tmpdir), 'mydir1'))
    assert os.path.basename(dir1) == 'mydir1'
    dir2 = util.createdir(os.path.join(str(tmpdir), 'mydir2'), abs=True)
    assert dir2 == os.path.abspath(dir2)


def test_cleardir(tmpdir):
    """Test removing all files from a directory."""
    # Setup: Create one file and one directory with another file.
    file_a = os.path.join(tmpdir, 'A.txt')
    with open(file_a, 'w'):
        pass
    dir_b = os.path.join(tmpdir, 'B')
    os.makedirs(dir_b)
    file_c = os.path.join(dir_b, 'C.txt')
    with open(file_c, 'w'):
        pass
    dir_c = os.path.join(dir_b, 'C')
    os.makedirs(dir_c)
    file_e = os.path.join(dir_c, 'E.txt')
    with open(file_e, 'w'):
        pass
    assert os.path.isfile(file_a)
    assert os.path.isfile(file_c)
    assert os.path.isfile(file_e)
    util.cleardir(dir_b)
    assert os.path.isfile(file_a)
    assert not os.path.isfile(file_c)
    assert not os.path.isfile(file_e)


def test_datetime():
    """Tests for datatime conversion functions."""
    dt = util.to_localtime(util.utc_now())
    assert util.to_datetime(dt.isoformat()) == dt
    # Error for invalid date format
    with pytest.raises(ValueError):
        util.to_datetime('Not a valid date')


def test_iostream():
    """Test error cases for IO streams."""
    with pytest.raises(ValueError):
        util.inputstream('/dev/null', compression='unknown')
    with pytest.raises(ValueError):
        util.outputstream('/dev/null', compression='unknown')
