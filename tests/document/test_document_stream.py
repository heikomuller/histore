# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the stream function of input documents."""

import os

from histore.document.base import DocumentConsumer
from histore.document.csv.base import CSVFile
from histore.document.csv.read import SimpleCSVDocument, SortedCSVDocument
from histore.document.mem.base import InMemoryDocument
from histore.document.row import DocumentRow
from histore.document.schema import to_schema
from histore.key.base import NumberKey


"""Input files for testing."""
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../.files')
CSV_FILE = os.path.join(DIR, 'agencies.csv')


# -- Helper classes -----------------------------------------------------------

class RowCollector(DocumentConsumer):
    """Stream consumer for test purposes. Ensures that all rows have the
    appropriate number of columns and snapshot version.
    """
    def __init__(self, columns: int, version: int):
        self.columns = columns
        self.version = version
        self.rows = set()

    def consume_document_row(self, row: DocumentRow, version: int):
        """Add a given document row to a new archive version.

        Parameters
        ----------
        row: histore.document.row.DocumentRow
            Row from an input stream (snapshot) that is being added to the
            archive snapshot for the given version.
        version: int
            Unique identifier for the new snapshot version.
        """
        assert version == self.version
        assert len(row.values) == self.columns
        key = tuple([v.value for v in row.key]) if isinstance(row.key, tuple) else row.key.value
        assert key not in self.rows
        self.rows.add(key)


def test_csv_document_stream():
    """Test reading a CSV document as a stream."""
    stream = SimpleCSVDocument(CSVFile(filename=CSV_FILE))
    stream.stream_to_archive(
        schema=to_schema(['agency', 'borough', 'state']),
        version=0,
        consumer=RowCollector(columns=3, version=0)
    )


def test_sorted_csv_document_stream():
    """Test reading a sorted CSV document as a stream."""
    stream = SortedCSVDocument(
        filename=CSV_FILE,
        columns=['agency', 'borough', 'state'],
        primary_key=list(range(3))
    )
    stream.stream_to_archive(
        schema=to_schema(['agency', 'borough', 'state']),
        version=0,
        consumer=RowCollector(columns=3, version=0)
    )


def test_in_memory_document_stream():
    """Test reading a in-memory document as a stream."""
    stream = InMemoryDocument(
        columns=['A', 'B'],
        rows=[[1, 2], [3, 4]],
        readorder=[(1, NumberKey(3), 0), (0, NumberKey(2), 1)]
    )
    stream.stream_to_archive(
        schema=to_schema(['A', 'B']),
        version=0,
        consumer=RowCollector(columns=2, version=0)
    )
