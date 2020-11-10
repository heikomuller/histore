# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for sorting CSV documents."""

import csv
import os

from histore.document.csv.base import CSVFile

import histore.config as config
import histore.document.csv.sort as sort


DIR = os.path.dirname(os.path.realpath(__file__))
# Reports Substantiated by Gender of Victim and Type of Abuse
DATAFILE = os.path.join(DIR, '../.files/etnx-8aft.tsv.gz')


def test_split_file():
    """Test splitting a csv file into blocks."""
    # Document fits into main memory.
    PK = [0, 1]
    with CSVFile(DATAFILE).open() as reader:
        buffer, filenames = sort.split(reader, sortkey=PK, buffer_size=1)
    assert len(buffer) == 10
    assert len(filenames) == 0
    # Split the file into blocks of two rows each.
    os.environ[config.ENV_HISTORE_SORTBUFFER] = str(200/(1024*1024))
    with CSVFile(DATAFILE).open() as reader:
        buffer, filenames = sort.split(reader, sortkey=PK)
    assert len(buffer) == 0
    assert len(filenames) == 5
    fout = sort.mergesort(buffer=buffer, filenames=filenames, sortkey=PK)
    validate_sorted(fout)
    # Cleanup
    for f in filenames:
        if f != fout:
            assert not os.path.exists(f)
        else:
            os.remove(f)
    del os.environ[config.ENV_HISTORE_SORTBUFFER]
    # Split into three files with one row in the buffer.
    with CSVFile(DATAFILE).open() as reader:
        buffer, filenames = sort.split(reader, sortkey=PK, buffer_size=300/(1024*1024))
    assert len(buffer) == 1
    assert len(filenames) == 3
    fout = sort.mergesort(buffer=buffer, filenames=filenames, sortkey=PK)
    validate_sorted(fout)
    # Cleanup
    os.remove(fout)


def validate_sorted(filename):
    """Validate the the rows in the given CSV file are sorted by year and
    gender."""
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
    keys = []
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            keys.append((row[0], row[1]))
    assert keys == KEYS
