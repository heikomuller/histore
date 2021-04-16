# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for the different document reader."""

import pytest

from histore.document.mem import InMemoryDocument
from histore.document.reader import AnnotatedDocumentReader, DefaultDocumentReader
from histore.document.schema import to_schema
from histore.key import NewRow, NumberKey, StringKey


@pytest.fixture
def document():
    """Get a document for test purposes."""
    return InMemoryDocument(
        columns=to_schema(['Name', 'Age']),
        rows=[
            (0, 0, ['Alice', 23]),
            (1, (1, 2), ['Bob', 32]),
            (2, 3, ['Claire', 27]),
            (3, -1, ['Dave', 45])
        ]
    )


def test_annotated_reader_single_key(document):
    """Test the annotated document reader with a single key attribute."""
    columns = [c.colid for c in document.columns]
    with document.open() as iterator:
        reader = AnnotatedDocumentReader(iterator=iterator, columns=columns, keys=[0])
        keys = list()
        row = reader.next()
        while row:
            keys.append(row.key)
            row = reader.next()
        reader.close()
    assert keys == [StringKey('Alice'), StringKey('Bob'), StringKey('Claire'), StringKey('Dave')]


def test_annotated_reader_multi_key(document):
    """Test the annotated document reader with a single key attribute."""
    columns = [c.colid for c in document.columns]
    with document.open() as iterator:
        reader = AnnotatedDocumentReader(iterator=iterator, columns=columns, keys=[0, 1])
        keys = list()
        row = reader.next()
        while row:
            keys.append(row.key)
            row = reader.next()
        reader.close()
    assert keys == [
        (StringKey('Alice'), NumberKey(23)),
        (StringKey('Bob'), NumberKey(32)),
        (StringKey('Claire'), NumberKey(27)),
        (StringKey('Dave'), NumberKey(45))
    ]


def test_default_reader(document):
    """Test the default document reader."""
    columns = [c.colid for c in document.columns]
    with document.open() as iterator:
        reader = DefaultDocumentReader(iterator=iterator, columns=columns)
        keys = list()
        row = reader.next()
        while row:
            keys.append(row.key)
            row = reader.next()
        reader.close()
    assert keys == [NumberKey(0), (NumberKey(1), NumberKey(2)), NumberKey(3), NewRow()]
