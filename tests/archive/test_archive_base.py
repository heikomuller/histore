# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for base functionality and edge cases of the archive base module."""

import os
import pandas as pd
import pytest

from histore.archive.base import Archive, PersistentArchive, to_document
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore
from histore.document.base import Document
from histore.document.csv.base import CSVFile


DIR = os.path.dirname(os.path.realpath(__file__))
FILENAME = os.path.join(DIR, '../.files/agencies.csv')


@pytest.mark.parametrize(
    'doc', [
        pd.DataFrame(data=[[1, 2], [3, 4]], columns=['A', 'B']),
        FILENAME,
        CSVFile(filename=FILENAME)
    ]
)
def test_input_to_document(doc):
    """Test converting inputs to documents."""
    doc = to_document(doc=doc)
    assert isinstance(doc, Document)


def test_create_persistent_archive(tmpdir):
    """Test creating a persistent archive with a first dataset snapshot."""
    # -- Setup ----------------------------------------------------------------
    df = pd.DataFrame(data=[['Alice', 23], ['Bob', 32]], columns=['Name', 'Age'])
    # -- Initialize with first snapshot.
    PersistentArchive(store=ArchiveFileStore(basedir=tmpdir, replace=True))
    PersistentArchive(store=ArchiveFileStore(basedir=tmpdir, replace=True), doc=df)
    PersistentArchive(basedir=tmpdir, create=True, doc=df)
    PersistentArchive(basedir=tmpdir, create=True, doc=df, primary_key=['Name'])
    # -- Error cases ----------------------------------------------------------
    #
    # Cannot have document with primary key and archive store.
    with pytest.raises(ValueError):
        PersistentArchive(store=ArchiveFileStore(basedir=tmpdir, replace=True), doc=df, primary_key=['Name'])
    # Cannot haveprimary key without document.
    with pytest.raises(ValueError):
        PersistentArchive(basedir=tmpdir, create=True, primary_key=['Name'])


def test_create_volatile_archive():
    """Test creating a volatile archive with a first dataset snapshot."""
    # -- Setup ----------------------------------------------------------------
    df = pd.DataFrame(data=[['Alice', 23], ['Bob', 32]], columns=['Name', 'Age'])
    # -- Initialize with first snapshot.
    Archive(store=VolatileArchiveStore(), doc=df)
    Archive(doc=df)
    Archive(doc=df, primary_key=['Name'])
    # -- Error cases ----------------------------------------------------------
    #
    # Cannot have document with primary key and archive store.
    with pytest.raises(ValueError):
        Archive(store=VolatileArchiveStore(), doc=df, primary_key=['Name'])
    # Cannot haveprimary key without document.
    with pytest.raises(ValueError):
        Archive(primary_key=['Name'])
