# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests to ensure that snapshot actions are mintained properly by the
different archive stores.
"""

import os
import pandas as pd

from histore.archive.base import PersistentArchive, VolatileArchive
from histore.document.snapshot import InputDescriptor

import histore.util as util


def test_persistent_archive_metadata(empty_dataset, tmpdir):
    """Test persisting archive snapshot metadata."""
    archive = PersistentArchive(
        basedir=os.path.join(str(tmpdir), 'archive'),
        replace=True,
        primary_key='Name',
        doc=empty_dataset
    )
    # Commit snapshot with addtional metadata.
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    ts = util.utc_now()
    action = {'command': 'X', 'time': ts}
    s = archive.commit(
        doc=df,
        snapshot=InputDescriptor(
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
    archive = PersistentArchive(
        basedir=os.path.join(str(tmpdir), 'archive'),
        replace=False,
        primary_key=[0]
    )
    s = archive.snapshots().last_snapshot()
    assert s.valid_time == ts
    assert s.description == 'First'
    assert s.action == action


def test_volatile_archive_metadata(empty_dataset, tmpdir):
    """Test archive snapshot metadata for a volatile archive."""
    archive = VolatileArchive(
        primary_key='Name',
        doc=empty_dataset
    )
    # Commit snapshot with addtional metadata.
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    ts = util.utc_now()
    action = {'command': 'X', 'time': ts}
    s = archive.commit(
        doc=df,
        snapshot=InputDescriptor(
            valid_time=ts,
            description='First',
            action=action
        )
    )
    assert s.valid_time == ts
    assert s.description == 'First'
    assert s.action == action
