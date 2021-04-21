# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for Json documents."""

import os
import pytest

from histore.document.json.base import JsonDocument
from histore.document.json.writer import JsonWriter


@pytest.fixture
def json_file(tmpdir):
    filename = os.path.join(tmpdir, 'data.json')
    input = [
        [0, 'alice', ['Alice', 23]],
        [1, 'bob', ['Bob', 43]],
        [2, 'claire', ['Claire', 32]]
    ]
    writer = JsonWriter(filename=filename)
    writer.write(['Name', 'Age'])
    for row in input:
        writer.write(row)
    writer.close()
    return filename


DOCUMENT = [(0, 'alice', ['Alice', 23]), (1, 'bob', ['Bob', 43]), (2, 'claire', ['Claire', 32])]


def test_delete_on_close(json_file):
    """Test deleting the input file when closing the document."""
    # By default, the file is not deleted when closing the document.
    doc = JsonDocument(filename=json_file)
    with doc.open() as reader:
        rows = [r for r in reader]
    assert rows == DOCUMENT
    doc.close()
    assert os.path.isfile(json_file)
    # When the delete_on_close flag is True the document will be deleted when
    # closing the document.
    doc = JsonDocument(filename=json_file, delete_on_close=True)
    with doc.open() as reader:
        rows = [r for r in reader]
    assert rows == DOCUMENT
    doc.close()
    assert not os.path.isfile(json_file)


def test_document_iterator(json_file):
    """Test iterating over rows in a dataset file."""
    with JsonDocument(filename=json_file).open() as reader:
        rows = [r for r in reader]
    assert rows == DOCUMENT


def test_document_iterrows(json_file):
    """Test iterating over document rows using the iterrows() method."""
    doc = JsonDocument(filename=json_file)
    rows = [(rowid, values) for rowid, values in doc.iterrows()]
    assert rows == [('alice', ['Alice', 23]), ('bob', ['Bob', 43]), ('claire', ['Claire', 32])]


def test_empty_document(tmpdir):
    """Test opening a document for a non-existing file."""
    filename = os.path.join(tmpdir, 'nodata.json')
    doc = JsonDocument(filename=filename)
    assert doc.columns == []
    with doc.open() as reader:
        rows = [r for r in reader]
    assert rows == []


def test_read_data_frame(json_file):
    """Test reading the default JSON document as as data frame."""
    df = JsonDocument(filename=json_file).read_df()
    assert list(df.columns) == ['Name', 'Age']
    assert df.shape == (3, 2)


def test_read_document(json_file):
    """Test reading a serialized Json document."""
    doc = JsonDocument(filename=json_file)
    with doc.open() as reader:
        rows = [row for row in reader]
    assert rows == DOCUMENT
