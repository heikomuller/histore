# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the in-memory document and document reader."""

import os
import pytest

from histore.document.csv.base import CSVFile
from histore.document.csv.read import SimpleCSVDocument, SortedCSVDocument
from histore.document.schema import Column
from histore.tests.base import ListReader


DIR = os.path.dirname(os.path.realpath(__file__))
# Reports Substantiated by Gender of Victim and Type of Abuse
GZIP_FILE = os.path.join(DIR, '../.files/etnx-8aft.tsv.gz')
INVALID_FILE = os.path.join(DIR, '../.files/invalid.tsv')


def test_read_keyed_file(tmpdir):
    """Test read CSV file where the rows are keyed by a primary key."""
    filename = os.path.join(tmpdir, 'sorted.csv')
    with open(filename, 'wt') as f:
        f.write('Alice,23\n')
        f.write('Bob,32\n')
    doc = SortedCSVDocument(filename=filename, columns=['Name', 'Age'], primary_key=[0])
    reader = doc.reader()
    keys = list()
    while reader.has_next():
        keys.append(reader.next().key.value)
    assert keys == ['Alice', 'Bob']
    reader.close()
    doc = SortedCSVDocument(filename=filename, columns=['Name', 'Age'], primary_key=[1, 0])
    reader = doc.reader()
    keys = list()
    while reader.has_next():
        keys.append(tuple([k.value for k in reader.next().key]))
    assert keys == [('23', 'Alice'), ('32', 'Bob')]
    doc.close()
    assert not os.path.isfile(filename)


def test_read_keyed_file_invalid(tmpdir):
    """Test read CSV file where the rows are keyed by a primary key but the
    number of cell values does not match the number of columns in the schema.
    """
    filename = os.path.join(tmpdir, 'sorted.csv')
    # -- Error cases (invalid file) -------------------------------------------
    with open(filename, 'wt') as f:
        f.write('Alice\n')
        f.write('Bob\n')
    doc = SortedCSVDocument(filename=filename, columns=['Name', 'Age'], primary_key=[0])
    with pytest.raises(ValueError):
        doc.reader()


def test_read_keyed_file_partial(tmpdir):
    """Test partial read for CSV file where the rows are keyed by a primary
    key.
    """
    filename = os.path.join(tmpdir, 'sorted.csv')
    with open(filename, 'wt') as f:
        f.write('Alice,23\n')
        f.write('Bob,32\n')
    doc = SortedCSVDocument(filename=filename, columns=['Name', 'Age'], primary_key=[0])
    reader = doc.partial(reader=ListReader(values=[])).reader()
    keys = list()
    while reader.has_next():
        keys.append(reader.next().key.value)
    assert keys == ['Alice', 'Bob']


def test_read_unkeyed_file():
    """Read a csv file without primary key."""
    # Compressed file.
    doc = SimpleCSVDocument(CSVFile(filename=GZIP_FILE, delim='\t', compressed=True))
    assert doc.columns == [
        'Calendar Year',
        'Gender',
        'Physical Abuse',
        'Sexual Abuse',
        'Risk of Sexual Abuse',
        'Risk of Harm',
        'Emotional/Neglect'
    ]
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    reader = doc.reader(schema=schema)
    years = list()
    origorder = list()
    while reader.has_next():
        row = reader.next()
        years.append(row.values[0])
        origorder.append((row.key, 9 - len(origorder)))
    reader.close()
    assert reader.next() is None
    YEARS = [
        '2018',
        '2018',
        '2017',
        '2017',
        '2016',
        '2016',
        '2015',
        '2015',
        '2014',
        '2014'
    ]
    assert years == YEARS
    # Test partial document.
    doc = doc.partial(reader=ListReader(values=origorder))
    reader = doc.reader(schema=schema)
    positions = list()
    while reader.has_next():
        row = reader.next()
        positions.append(row.pos)
    assert positions == list(range(10))[::-1]
    doc.close()
    # Override column names.
    NAMES = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7']
    doc = SimpleCSVDocument(
        CSVFile(
            filename=GZIP_FILE,
            delim='\t',
            compressed=True,
            header=NAMES
        ),
    )
    assert doc.columns == NAMES
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    reader = doc.reader(schema=schema)
    assert len(list(reader)) == 11
    # Ignore header (considered as a data row).
    doc = SimpleCSVDocument(
        CSVFile(
            filename=GZIP_FILE,
            delim='\t',
            compressed=True,
            header=NAMES
        )
    )
    assert doc.columns == NAMES
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    reader = doc.reader(schema=schema)
    assert len(list(reader)) == 11
    doc = SimpleCSVDocument(CSVFile(filename=GZIP_FILE, delim='\t', compressed=True))
    reader = doc.reader()
    reader.close()
    assert reader.next() is None
    # -- Error cases ----------------------------------------------------------
    doc = SimpleCSVDocument(CSVFile(filename=INVALID_FILE, delim='\t'))
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    assert len(schema) == 7
    reader = doc.reader(schema=schema)
    with pytest.raises(ValueError):
        reader.next()
