# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for rollback of archive snapshots."""

import numpy as np
import pandas as pd
import pytest

from histore.archive.base import Archive
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore


# -- Datasets -----------------------------------------------------------------

DF1 = pd.DataFrame(
    data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Alice', 23]],
    columns=['Name', 'Age'],
    dtype=object
)
DF2 = pd.DataFrame(
    data=[['Alice', 32, 170], ['Bob', 44, 180], ['Claire', 27, 160], ['Dave', 23, 190]],
    index=[0, 1, 2, 3],
    columns=['Name', 'Age', 'Height'],
    dtype=object
)
DF3 = pd.DataFrame(
    data=[['Dave', 33], ['Claire', 27], ['Bob', 44], ['Alice', 32]],
    index=[3, 2, 1, 0],
    columns=['Name', 'Age'],
    dtype=object
)


# -- Unit tests ---------------------------------------------------------------

def test_rollback_keyed_persistent(tmpdir):
    """Test rollback to keyed persistent archive."""
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=True, primary_key=[0, 1]))
    s1 = archive.commit(DF1, sorted=False, validate=True)
    s2 = archive.commit(DF2, sorted=False, validate=True)
    s3 = archive.commit(DF3, sorted=False, validate=True)
    # Ensure that we can check out the original data frames.
    np.array_equal(archive.checkout().values, DF3.values)
    archive.rollback(version=s2.version)
    np.array_equal(archive.checkout().values, DF2.values)
    archive.rollback(version=s1.version)
    np.array_equal(archive.checkout().values, DF1.values)
    # -- Error case for checkout and rollback ---------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=s3.version)
    with pytest.raises(ValueError):
        archive.rollback(version=s3.version)


@pytest.mark.parametrize('validate', [True, False])
def test_rollback_keyed_volatile(validate):
    """Test rollback to keyed volatile archive."""
    archive = Archive(store=VolatileArchiveStore(primary_key=[0, 1]))
    s1 = archive.commit(DF1, sorted=False, validate=True)
    s2 = archive.commit(DF2, sorted=False, validate=True)
    s3 = archive.commit(DF3, sorted=False, validate=True)
    # Ensure that we can check out the original data frames.
    np.array_equal(archive.checkout().values, DF3.values)
    archive.rollback(version=s2.version)
    np.array_equal(archive.checkout().values, DF2.values)
    archive.rollback(version=s1.version)
    np.array_equal(archive.checkout().values, DF1.values)
    # -- Error case for checkout and rollback ---------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=s3.version)
    with pytest.raises(ValueError):
        archive.rollback(version=s3.version)


@pytest.mark.parametrize('validate', [True, False])
def test_commit_unkeyed_persistent(validate, tmpdir):
    """Test commit to un-keyed persistent archive."""
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=True))
    s1 = archive.commit(DF1)
    s2 = archive.commit(DF2)
    s3 = archive.commit(DF3)
    pd.testing.assert_frame_equal(archive.checkout(), DF3)
    archive.rollback(version=s2.version)
    pd.testing.assert_frame_equal(archive.checkout(), DF2)
    archive.rollback(version=s1.version)
    pd.testing.assert_frame_equal(archive.checkout(), DF1)
    # -- Error case for checkout and rollback ---------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=s3.version)
    with pytest.raises(ValueError):
        archive.rollback(version=s3.version)


@pytest.mark.parametrize('validate', [True, False])
def test_commit_unkeyed_volatile(validate):
    """Test commit to un-keyed volatile archive."""
    archive = Archive(store=VolatileArchiveStore())
    s1 = archive.commit(DF1)
    s2 = archive.commit(DF2)
    s3 = archive.commit(DF3)
    pd.testing.assert_frame_equal(archive.checkout(), DF3)
    archive.rollback(version=s2.version)
    pd.testing.assert_frame_equal(archive.checkout(), DF2)
    archive.rollback(version=s1.version)
    pd.testing.assert_frame_equal(archive.checkout(), DF1)
    # -- Error case for checkout and rollback ---------------------------------
    with pytest.raises(ValueError):
        archive.checkout(version=s3.version)
    with pytest.raises(ValueError):
        archive.rollback(version=s3.version)
