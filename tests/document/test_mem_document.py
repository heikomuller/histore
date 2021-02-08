# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the in-memory document and document reader."""

import pytest

from histore.key.base import NumberKey, StringKey
from histore.document.mem.base import InMemoryDocument
from histore.document.schema import Column
from histore.tests.base import ListReader


COLS = [Column(0, 'Name'), Column(1, 'Age')]
ROWS = [
    ['Alice', 23],
    ['Bob', 35],
    ['Dave', 28],
    ['Claire', 33]
]

NAMES_SORTED = ['Alice', 'Bob', 'Claire', 'Dave']
POS_SORTED = [0, 1, 3, 2]


def test_array_document_reader():
    """Test the reader for an in-memory document."""
    # Read rows in given order.
    readorder = [(i, NumberKey(i), i) for i in range(len(ROWS))]
    doc = InMemoryDocument(columns=COLS, rows=ROWS, readorder=readorder)
    reader = doc.reader(schema=COLS)
    readindex = 0
    while reader.has_next():
        row = reader.next()
        assert row.pos == readindex
        assert row.key.value == readindex
        assert row.values[0] == ROWS[readindex][0]
        assert row.values[1] == ROWS[readindex][1]
        assert str(row).startswith('<DocumentRow(')
        readindex += 1
    assert readindex == 4
    # Read rows in reverse order.
    readorder = [(i, NumberKey(i), 4 - i) for i in range(len(ROWS))]
    doc = InMemoryDocument(columns=COLS, rows=ROWS, readorder=readorder)
    reader = doc.reader(schema=COLS)
    readindex = 0
    while reader.has_next():
        row = reader.next()
        assert row.pos == 4 - readindex
        assert row.key.value == readindex
        assert row.values[0] == ROWS[readindex][0]
        assert row.values[1] == ROWS[readindex][1]
        readindex += 1
    assert readindex == 4
    # Read rows keyed by their name
    readorder = [(i, StringKey(ROWS[i][0]), i) for i in range(len(ROWS))]
    readorder = sorted(readorder, key=lambda x: x[1])
    assert [r[1].value for r in readorder] == NAMES_SORTED
    doc = InMemoryDocument(columns=COLS, rows=ROWS, readorder=readorder)
    reader = doc.reader(schema=[Column(0, 'Name'), Column(1, 'Age')])
    readindex = 0
    while reader.has_next():
        row = reader.next()
        assert row.pos == POS_SORTED[readindex]
        assert row.values[0] == NAMES_SORTED[readindex]
        readindex += 1
    assert readindex == 4
    # Error cases
    with pytest.raises(ValueError):
        doc.reader(schema=[Column(0, 'Name')])


def test_partial_array_document_reader():
    """Test the reader for a partial in-memory document."""
    # Read rows in given order.
    origorder = [(StringKey(ROWS[i][0]), i) for i in range(len(ROWS))]
    origorder = sorted(origorder, key=lambda t: t[0])
    # Test 1: One inserted tuple has name which is greater that any existing
    # name. List reader will terminate before document reader.
    rows = [ROWS[2], ['Bea', 21], ['Eve', 48], ROWS[0]]
    readorder = [(i, StringKey(rows[i][0]), i) for i in range(len(rows))]
    readorder = sorted(readorder, key=lambda x: x[1])
    doc = InMemoryDocument(columns=COLS, rows=rows, readorder=readorder)
    doc = doc.partial(reader=ListReader(values=origorder))
    reader = doc.reader(schema=COLS)
    names, positions = list(), list()
    while reader.has_next():
        row = reader.next()
        names .append(row.values[0])
        positions.append(row.pos)
    assert names == ['Alice', 'Bea', 'Dave', 'Eve']
    assert positions == [0, 4, 2, 5]
    # Test 2: No inserted tuple has name which is greater that any existing
    # name. Document reader will terminate before list reader.
    origorder = list()
    for row in sorted(ROWS, key=lambda r: r[0]):
        origorder.append((StringKey(row[0]), len(origorder)))
    rows = [ROWS[1], ['Bea', 21], ROWS[0]]
    readorder = [(i, StringKey(rows[i][0]), i) for i in range(len(rows))]
    readorder = sorted(readorder, key=lambda x: x[1])
    doc = InMemoryDocument(columns=COLS, rows=rows, readorder=readorder)
    doc = doc.partial(reader=ListReader(values=origorder))
    reader = doc.reader(schema=COLS)
    names, positions = list(), list()
    while reader.has_next():
        row = reader.next()
        names .append(row.values[0])
        positions.append(row.pos)
    assert names == ['Alice', 'Bea', 'Bob']
    assert positions == [0, 4, 1]
