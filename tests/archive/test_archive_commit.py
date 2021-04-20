# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for committing (and checking out) archive snapshots with and
without primary keys.
"""

import numpy as np
import pandas as pd
import pytest

from histore.archive.base import Archive
from histore.archive.timestamp import Timestamp, TimeInterval
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore


# -- Datasets -----------------------------------------------------------------

DF1 = pd.DataFrame(
    data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]],
    columns=['Name', 'Age'],
    dtype=object
)
DF2 = pd.DataFrame(
    data=[['Alice', 32], ['Bob', 44], ['Claire', 27], ['Dave', 23]],
    index=[0, 1, 2, 3],
    columns=['Name', 'Age'],
    dtype=object
)
DF3 = pd.DataFrame(
    data=[['Dave', 33], ['Claire', 27], ['Bob', 44], ['Alice', 32]],
    index=[3, 2, 1, 0],
    columns=['Name', 'Age'],
    dtype=object
)
DF4 = pd.DataFrame(
    data=[['Alice', 32], ['Eve', 25], ['Claire', 27], ['Bob', 44]],
    index=[0, None, 2, 1],
    columns=['Name', 'Age'],
    dtype=object
)
DF4mod = pd.DataFrame(
    data=[['Alice', 32], ['Eve', 25], ['Claire', 27], ['Bob', 44]],
    index=[0, 4, 2, 1],
    columns=['Name', 'Age'],
    dtype=object
)


# -- Unit tests ---------------------------------------------------------------

@pytest.mark.parametrize('validate', [True, False])
def test_commit_keyed_persistent(validate, tmpdir):
    """Test commit to keyed persistent archive."""
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=True, primary_key=[0, 1]))
    s1 = archive.commit(DF1, sorted=False, validate=validate)
    s2 = archive.commit(DF2, sorted=False, validate=validate)
    s3 = archive.commit(DF3, sorted=False, validate=validate)
    archive.commit(DF4, sorted=False, validate=validate)
    reader = archive.reader()
    validate_keyed_rowindex_archive(reader)
    # Ensure that we can check out the original data frames.
    np.array_equal(archive.checkout(version=s1.version).values, DF1.values)
    np.array_equal(archive.checkout(version=s2.version).values, DF2.values)
    np.array_equal(archive.checkout(version=s3.version).values, DF3.values)
    np.array_equal(archive.checkout().values, DF4.values)
    # -- Error case for checkout ----------------------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=1000)


@pytest.mark.parametrize('validate', [True, False])
def test_commit_keyed_volatile(validate):
    """Test commit to keyed volatile archive."""
    archive = Archive(store=VolatileArchiveStore(primary_key=[0, 1]))
    s1 = archive.commit(DF1, sorted=False, validate=validate)
    s2 = archive.commit(DF2, sorted=False, validate=validate)
    s3 = archive.commit(DF3, sorted=False, validate=validate)
    archive.commit(DF4, sorted=False, validate=validate)
    reader = archive.reader()
    validate_keyed_rowindex_archive(reader)
    # Ensure that we can check out the original data frames.
    np.array_equal(archive.checkout(version=s1.version).values, DF1.values)
    np.array_equal(archive.checkout(version=s2.version).values, DF2.values)
    np.array_equal(archive.checkout(version=s3.version).values, DF3.values)
    np.array_equal(archive.checkout().values, DF4.values)
    # -- Error case for checkout ----------------------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=1000)


@pytest.mark.parametrize('validate', [True, False])
def test_commit_unkeyed_persistent(validate, tmpdir):
    """Test commit to un-keyed persistent archive."""
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=True))
    s1 = archive.commit(DF1, validate=validate)
    s2 = archive.commit(DF2, validate=validate)
    s3 = archive.commit(DF3, validate=validate)
    archive.commit(DF4, validate=validate)
    reader = archive.reader()
    validate_unkeyed_rowindex_archive(reader)
    pd.testing.assert_frame_equal(archive.checkout(version=s1.version), DF1)
    pd.testing.assert_frame_equal(archive.checkout(version=s2.version), DF2)
    pd.testing.assert_frame_equal(archive.checkout(version=s3.version), DF3)
    pd.testing.assert_frame_equal(archive.checkout(), DF4mod)
    # -- Error case for checkout ----------------------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=1000)


@pytest.mark.parametrize('validate', [True, False])
def test_commit_unkeyed_volatile(validate):
    """Test commit to un-keyed volatile archive."""
    archive = Archive(store=VolatileArchiveStore())
    s1 = archive.commit(DF1, validate=validate)
    s2 = archive.commit(DF2, validate=validate)
    s3 = archive.commit(DF3, validate=validate)
    archive.commit(DF4, validate=validate)
    reader = archive.reader()
    validate_unkeyed_rowindex_archive(reader)
    pd.testing.assert_frame_equal(archive.checkout(version=s1.version), DF1)
    pd.testing.assert_frame_equal(archive.checkout(version=s2.version), DF2)
    pd.testing.assert_frame_equal(archive.checkout(version=s3.version), DF3)
    pd.testing.assert_frame_equal(archive.checkout(), DF4mod)
    # -- Error case for checkout ----------------------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=1000)


# -- Helper functions ---------------------------------------------------------

def validate_keyed_rowindex_archive(reader):
    keys = list()
    while reader.has_next():
        row = reader.next()
        keys.append(tuple([k.value for k in row.key]))
    assert keys == [
        ('Alice', 23),
        ('Alice', 32),
        ('Bob', 44),
        ('Bob', 45),
        ('Claire', 27),
        ('Dave', 23),
        ('Dave', 33),
        ('Eve', 25)
    ]


def validate_unkeyed_rowindex_archive(reader):
    rows = {row.rowid: row for row in reader}
    assert len(rows) == 5
    ts = Timestamp(intervals=TimeInterval(0, 3))
    for i in range(3):
        assert rows[i].timestamp.is_equal(ts)
    assert rows[3].timestamp.is_equal(Timestamp(intervals=TimeInterval(0, 2)))
    assert rows[4].timestamp.is_equal(Timestamp(version=3))
    assert rows[0].cells[0].value == 'Alice'
    assert rows[1].cells[0].value == 'Bob'
    assert rows[2].cells[0].value == 'Claire'
    names = [v.value for v in rows[3].cells[0].values]
    assert 'Alice' in names
    assert 'Dave' in names
    assert rows[4].cells[0].value == 'Eve'
