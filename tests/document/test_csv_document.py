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
from histore.document.csv.read import open_document
from histore.document.schema import Column
from histore.tests.base import ListReader


DIR = os.path.dirname(os.path.realpath(__file__))
# Reports Substantiated by Gender of Victim and Type of Abuse
GZIP_FILE = os.path.join(DIR, '../.files/etnx-8aft.tsv.gz')
INVALID_FILE = os.path.join(DIR, '../.files/invalid.tsv')


def test_read_unkeyed_file():
    """Read a csv file without primary key."""
    # Compressed file.
    doc = open_document(CSVFile(filename=GZIP_FILE, delim='\t', compressed=True))
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
        origorder.append((row.key, 9-len(origorder)))
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
    doc = open_document(
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
    doc = open_document(
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
    # -- Error cases ----------------------------------------------------------
    # 2) Invalid file
    doc = open_document(CSVFile(filename=INVALID_FILE, delim='\t'))
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    assert len(schema) == 7
    reader = doc.reader(schema=schema)
    with pytest.raises(ValueError):
        reader.next()


def test_read_keyed_external_document():
    """Read a csv file with primary key that is sorted on disk."""
    doc = open_document(
        CSVFile(
            filename=GZIP_FILE,
            delim='\t',
            compressed=True
        ),
        primary_key=['Calendar Year', 'Gender'],
        max_size=300/(1024*1024)
    )
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
    keys = list()
    origorder = list()
    while reader.has_next():
        row = reader.next()
        origorder.append((row.key, 9-len(origorder)))
        keys.append((row.values[0], row.values[1]))
    KEYS = [
        ('2014', 'Female'),
        ('2014', 'Male'),
        ('2015', 'Female'),
        ('2015', 'Male'),
        ('2016', 'Female'),
        ('2016', 'Male'),
        ('2017', 'Female'),
        ('2017', 'Male'),
        ('2018', 'Female'),
        ('2018', 'Male')
    ]
    assert keys == KEYS
    # Get partial reader from extenal file reader.
    doc = doc.partial(reader=ListReader(values=origorder))
    reader = doc.reader(schema=schema)
    positions = list()
    while reader.has_next():
        row = reader.next()
        positions.append(row.pos)
    assert positions == list(range(10))[::-1]
    doc.close()
    # Invalid document.
    doc = open_document(
        CSVFile(
            filename=INVALID_FILE,
            delim='\t'
        ),
        primary_key=['Calendar Year'],
        max_size=1/(1024*1024)
    )
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
    with pytest.raises(ValueError):
        reader.next()
    doc.close()


def test_read_keyed_mem_document():
    """Read a csv file with primary key that fits in main-memory."""
    doc = open_document(
        CSVFile(
            filename=GZIP_FILE,
            delim='\t',
            compressed='gzip'
        ),
        primary_key=['Calendar Year', 'Gender']
    )
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
    while reader.has_next():
        row = reader.next()
        years.append(row.values[0])
    YEARS = [
        '2014',
        '2014',
        '2015',
        '2015',
        '2016',
        '2016',
        '2017',
        '2017',
        '2018',
        '2018'
    ]
    assert years == YEARS
