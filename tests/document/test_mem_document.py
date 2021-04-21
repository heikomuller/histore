# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the in-memory document and document reader."""

import pytest

from histore.document.mem import InMemoryDocument, Schema


@pytest.fixture
def document():
    """Get a document for test purposes."""
    return InMemoryDocument(
        columns=['Name', 'Age', 'Dept'],
        rows=[
            (0, 0, ['Alice', 23, 'R&D']),
            (1, 1, ['Bob', 35, 'Finance']),
            (2, 3, ['Dave', 28, 'Sales']),
            (3, 2, ['Claire', 33, 'Finance'])
        ]
    )


def test_document_iterrows(document):
    """Test the reader for an in-memory document."""
    rows = list()
    for rowidx, row in document.iterrows():
        rows.append((rowidx, row))
    assert rows == [
        (0, ['Alice', 23, 'R&D']),
        (1, ['Bob', 35, 'Finance']),
        (3, ['Dave', 28, 'Sales']),
        (2, ['Claire', 33, 'Finance'])
    ]


def test_empty_document():
    """Test the reader for an empty document."""
    doc = Schema(columns=['Name', 'Age'])
    assert doc.columns == ['Name', 'Age']
    rows = list()
    for rowidx, row in doc.iterrows():
        rows.append((rowidx, row))
    assert rows == []


def test_error_read_after_close(document):
    """Error when reading closed document."""
    for rowidx, row in document.iterrows():
        pass
    document.close()
    with document.open() as reader:
        with pytest.raises(StopIteration):
            next(reader)
    document.close()


def test_to_df(document):
    """Test reading the document as a data frame."""
    df = document.to_df()
    assert df.shape == (4, 3)
    assert list(df.index) == [0, 1, 3, 2]


def test_sort_document(document):
    """Test sorting the document."""
    doc = document.sorted(keys=[0])
    with document.open() as reader:
        names = [r[0] for _, _, r in reader]
        assert names == ['Alice', 'Bob', 'Dave', 'Claire']
    with doc.open() as reader:
        names = [r[0] for _, _, r in reader]
        assert names == ['Alice', 'Bob', 'Claire', 'Dave']
    doc = document.sorted(keys=[2, 0])
    with doc.open() as reader:
        names = [r[0] for _, _, r in reader]
        assert names == ['Bob', 'Claire', 'Alice', 'Dave']
