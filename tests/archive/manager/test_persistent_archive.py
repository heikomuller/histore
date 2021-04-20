# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the persistent archive."""

import os
import pandas as pd
import pytest

from histore.archive.manager.persist import PersistentArchiveManager
from histore.document.base import InputDescriptor
from histore.document.csv.base import CSVFile


DIR = os.path.dirname(os.path.realpath(__file__))
WATERSHED_1 = os.path.join(DIR, '../../.files/y43c-5n92.tsv.gz')


@pytest.mark.parametrize(
    'doc',
    [
        pd.read_csv(WATERSHED_1, compression='gzip', delimiter='\t'),
        CSVFile(WATERSHED_1),
        WATERSHED_1
    ]
)
@pytest.mark.parametrize('replace', [True, False])
def test_watershed_archive(doc, replace, tmpdir):
    """Test merging snapshots of the NYC Watershed data into an archive."""
    manager = PersistentArchiveManager(basedir=tmpdir, create=True)
    descriptor = manager.create(
        name='watershed',
        doc=doc,
        primary_key=['Site', 'Date'],
        snapshot=InputDescriptor(description='First snapshot', action={'type': 'LOAD'}),
        description='Watershed Archive'
    )
    archive = manager.get(descriptor.identifier())
    s = archive.snapshots().last_snapshot()
    diff = archive.diff(s.version - 1, s.version)
    assert len(diff.schema().insert()) == 10
    assert len(diff.rows().insert()) == 1793
    # -- Recreate the archive instance.
    manager = PersistentArchiveManager(basedir=tmpdir, create=False)
    archive = manager.get(descriptor.identifier())
    s = archive.snapshots().last_snapshot()
    assert s.description == 'First snapshot'
    assert s.action == {'type': 'LOAD'}
    s = archive.commit(doc)
    diff = archive.diff(s.version - 1, s.version)
    assert len(diff.schema().insert()) == 0
    assert len(diff.rows().insert()) == 0
