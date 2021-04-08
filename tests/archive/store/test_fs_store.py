# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for special cases in the archive file store."""

from pathlib import Path

import os
import pytest

from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.fs.reader import ArchiveFileReader


def test_init_store(tmpdir):
    """Test initializing the file store."""
    Path(os.path.join(tmpdir, 'rows.dat')).touch()
    Path(os.path.join(tmpdir, 'b.json')).touch()
    ArchiveFileStore(basedir=tmpdir, replace=True)


def test_read_invalid_file(tmpdir):
    """Test error when attempting to read an invalid file."""
    filename = os.path.join(tmpdir, 'rows.dat')
    with open(filename, 'wt') as f:
        f.write('invalid\n')
    with pytest.raises(ValueError):
        ArchiveFileReader(filename=filename)
