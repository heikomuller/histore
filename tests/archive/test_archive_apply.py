# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the archive apply method."""

import pandas as pd

from histore.archive.base import Archive
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore
from histore.document.schema import Column
from histore.document.operator import DatasetOperator


class FuncOp(DatasetOperator):
    def __init__(self, columns, func, description=None, action=None):
        super(FuncOp, self).__init__(
            columns=columns,
            description=description,
            action=action
        )
        self.func = func

    def handle(self, rowid, row):
        return self.func(row)


DATASET = pd.DataFrame(
    data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
    columns=['Name', 'Age']
)


def test_archive_apply(tmpdir):
    """Test applying a sequence of operators on an archive."""
    # -- Setup ----------------------------------------------------------------
    archive = Archive()
    archive.commit(DATASET)
    # -- Increment age --------------------------------------------------------

    def inc_age(row):
        row[1] = int(row[1]) + 1
        return row

    archive.apply(FuncOp(columns=archive.schema().at_version(0), func=inc_age))
    assert list(archive.checkout()['Age']) == [33, 46, 28, 24]
    # -- Insert height --------------------------------------------------------
    map = {'Alice': 180, 'Bob': 175, 'Claire': 170, 'Dave': 165}

    def ins_height(row):
        return row[:1] + [map[row[0]]] + row[1:]

    columns = archive.schema().at_version(1)
    columns = columns[:1] + ['Height'] + columns[1:]
    archive.apply(FuncOp(columns=columns, func=ins_height))
    df = archive.checkout()
    assert list(df['Age']) == [33, 46, 28, 24]
    assert list(df['Height']) == [180, 175, 170, 165]
    # -- Rename column Name and remove Claire ---------------------------------

    def rm_claire(row):
        return row if row[0] != 'Claire' else None

    columns = archive.schema().at_version(2)
    columns = [Column(colid=columns[0].colid, name='Person')] + columns[1:]
    archive.apply(FuncOp(columns=columns, func=rm_claire))
    df = archive.checkout()
    assert list(df['Person']) == ['Alice', 'Bob', 'Dave']
    # -- Names to upper case --------------------------------------------------

    def name_upper(row):
        return [row[0].upper()] + row[1:]

    columns = archive.schema().at_version(3)
    archive.apply(FuncOp(columns=columns, func=name_upper))
    df = archive.checkout()
    assert list(df['Person']) == ['ALICE', 'BOB', 'DAVE']
    # -- Make first snapshot the new current snapshot -------------------------

    def pass_through(row):
        return row

    columns = archive.schema().at_version(0)
    archive.apply(FuncOp(columns=columns, func=pass_through), origin=0)
    df = archive.checkout()
    assert list(df['Name']) == ['Alice', 'Bob', 'Claire', 'Dave']
    assert list(df['Age']) == [32, 45, 27, 23]
    # -- Run all transformation steps as a single pipeline --------------------
    archive = Archive()
    df = pd.DataFrame(
        data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
        columns=['Name', 'Age']
    )
    archive.commit(df)
    operators = list()
    columns = archive.schema().at_version(0)
    operators.append(FuncOp(columns=columns, func=inc_age))
    columns = columns[:1] + ['Height'] + columns[1:]
    operators.append(FuncOp(columns=columns, func=ins_height))
    columns = [Column(colid=columns[0].colid, name='Person')] + columns[1:]
    operators.append(FuncOp(columns=columns, func=rm_claire))
    operators.append(FuncOp(columns=columns, func=name_upper))
    # Repeat the full process for both, the volatile archive and a persistent
    # archive.
    for store in [VolatileArchiveStore(), ArchiveFileStore(basedir=tmpdir, replace=True)]:
        archive = Archive(store=store)
        archive.commit(DATASET)
        snapshots = archive.apply(operators=operators, validate=True)
        df = archive.checkout(version=snapshots[0].version)
        assert list(df['Age']) == [33, 46, 28, 24]
        df = archive.checkout(version=snapshots[1].version)
        assert list(df['Age']) == [33, 46, 28, 24]
        assert list(df['Height']) == [180, 175, 170, 165]
        df = archive.checkout(version=snapshots[2].version)
        assert list(df['Person']) == ['Alice', 'Bob', 'Dave']
        df = archive.checkout(version=snapshots[3].version)
        assert list(df['Person']) == ['ALICE', 'BOB', 'DAVE']
