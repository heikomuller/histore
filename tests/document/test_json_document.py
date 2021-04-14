# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for Json documents."""

import os
import pytest

from histore.document.json.base import JsonDocument, JsonDocumentReader
from histore.document.json.reader import JsonReader
from histore.document.json.writer import JsonWriter
from histore.document.schema import Column
from histore.tests.base import ListReader


@pytest.fixture
def json_file(tmpdir):
    filename = os.path.join(tmpdir, 'output.json')
    input = [
        ['Alice', 23],
        ['Bob', 43],
        ['Claire', 32]
    ]
    writer = JsonWriter(filename=filename)
    writer.write(['Name', 'Age'])
    for i, row in enumerate(input):
        writer.write([i, row])
    writer.close()
    return filename


def test_document_iterator(json_file):
    """Test iterating over rows in a dataset file."""
    with JsonDocument(filename=json_file).reader() as reader:
        rows = [r for r in reader]
    assert len(rows) == 3


def test_empty_document(tmpdir):
    """Test opening a document for a non-existing file."""
    filename = os.path.join(tmpdir, 'output.json')
    doc = JsonDocument(filename=filename)
    assert doc.columns == []


def test_read_data_frame(json_file):
    """Test reading a partial document."""
    df = JsonDocument(filename=json_file).read_df()
    assert df.shape == (3, 2)


def test_read_invalid_document(json_file):
    """Test error case where number of schema columns does not match the number
    of cells in a data row.
    """
    with pytest.raises(ValueError):
        JsonDocumentReader(
            reader=JsonReader(json_file),
            schema=[Column(colid=0, name='Name')]
        )


def test_read_partial_document(json_file):
    """Test reading a partial document."""
    doc = JsonDocument(filename=json_file).partial(ListReader(values=[]))
    rows = list()
    reader = doc.reader()
    while reader.has_next():
        rows.append(reader.next().values)
    assert rows == [{0: 'Alice', 1: 23}, {0: 'Bob', 1: 43}, {0: 'Claire', 1: 32}]


def test_read_document(json_file):
    """Test reading a serialized Json document."""
    doc = JsonDocument(filename=json_file)
    rows = list()
    reader = doc.reader()
    while reader.has_next():
        rows.append(reader.next().values)
    assert rows == [{0: 'Alice', 1: 23}, {0: 'Bob', 1: 43}, {0: 'Claire', 1: 32}]
