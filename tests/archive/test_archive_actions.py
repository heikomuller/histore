# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests to ensure that snapshot actions are mintained properly by the
different archive stores.
"""

import pandas as pd

from histore.archive.base import Archive
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore
from histore.document.base import InputDescriptor

import histore.util as util


def test_persistent_archive_metadata(tmpdir):
    """Test persisting archive snapshot metadata."""
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=True, primary_key=[0, 1]))
    # Commit snapshot with addtional metadata.
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    ts = util.utc_now()
    action = {'command': 'X', 'time': ts}
    s = archive.commit(
        doc=df,
        descriptor=InputDescriptor(
            valid_time=ts,
            description='First',
            action=action
        )
    )
    assert s.valid_time == ts
    assert s.description == 'First'
    assert s.action == action
    # Ensure that we can access snapshot metadata correctly after re-creating
    # the archive.
    archive = Archive(store=ArchiveFileStore(basedir=tmpdir, replace=False, primary_key=[0, 1]))
    s = archive.snapshots().last_snapshot()
    assert s.valid_time == ts
    assert s.description == 'First'
    assert s.action == action


def test_volatile_archive_metadata():
    """Test archive snapshot metadata for a volatile archive."""
    archive = Archive(store=VolatileArchiveStore(primary_key=[0, 1]))
    # Commit snapshot with addtional metadata.
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    ts = util.utc_now()
    action = {'command': 'X', 'time': ts}
    s = archive.commit(
        doc=df,
        descriptor=InputDescriptor(
            valid_time=ts,
            description='First',
            action=action
        )
    )
    assert s.valid_time == ts
    assert s.description == 'First'
    assert s.action == action
