# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the in-memory document and document reader."""

import os
import pytest

from histore.document.csv.base import read_csv
from histore.document.schema import Column
from histore.tests.base import ListReader


DIR = os.path.dirname(os.path.realpath(__file__))
# Reports Substantiated by Gender of Victim and Type of Abuse
GZIP_FILE = os.path.join(DIR, '../.files/etnx-8aft.tsv.gz')
INVALID_FILE = os.path.join(DIR, '../.files/invalid.tsv')


def test_read_unkeyed_file():
    """Read a csv file without primary key."""
    # Compressed file.
    doc = read_csv(filename=GZIP_FILE, delimiter='\t', compression='gzip')
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
    years = []
    origorder = []
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
    positions = []
    while reader.has_next():
        row = reader.next()
        positions.append(row.pos)
    assert positions == list(range(10))[::-1]
    doc.close()
    # Override column names.
    NAMES = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7']
    doc = read_csv(
        filename=GZIP_FILE,
        delimiter='\t',
        compression='gzip',
        columns=NAMES
    )
    assert doc.columns == NAMES
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    reader = doc.reader(schema=schema)
    assert len(list(reader)) == 10
    # Ignore header (considered as a data row).
    doc = read_csv(
        filename=GZIP_FILE,
        delimiter='\t',
        compression='gzip',
        has_header=False,
        columns=NAMES
    )
    assert doc.columns == NAMES
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    reader = doc.reader(schema=schema)
    assert len(list(reader)) == 11
    # -- Error cases ----------------------------------------------------------
    # 1) Invalid compression format.
    with pytest.raises(ValueError):
        read_csv(filename=GZIP_FILE, compression='unknown')
    # 2) Invalid file
    doc = read_csv(filename=INVALID_FILE, delimiter='\t')
    schema = [Column(colid=i, name=name) for i, name in enumerate(doc.columns)]
    assert len(schema) == 7
    reader = doc.reader(schema=schema)
    with pytest.raises(ValueError):
        reader.next()
