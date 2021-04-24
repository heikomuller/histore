# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the in-memory document and document reader."""

import os

from histore.document.csv.base import CSVFile


DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../.files')
CSV_FILE = os.path.join(DIR, 'agencies.csv')
GZIP_FILE = os.path.join(DIR, 'etnx-8aft.tsv.gz')


def test_custom_header(tmpdir):
    """Test reading files without header row."""
    filename = os.path.join(tmpdir, 'data.csv')
    # -- Default assumes header information is present.
    with CSVFile(filename=filename, delim='|').writer() as writer:
        writer.write(['A', 'B'])
        writer.write(['1', '2'])
    doc = CSVFile(filename=filename, delim='|')
    assert doc.columns == ['A', 'B']
    with doc.open() as reader:
        rows = [row for _, _, row in reader]
    assert rows == [['1', '2']]
    # -- Passing a custom header means that two data rows are read.
    doc = CSVFile(filename=filename, delim='|', header=['X', 'Y'])
    assert doc.columns == ['X', 'Y']
    with doc.open() as reader:
        rows = [row for _, _, row in reader]
    assert rows == [['A', 'B'], ['1', '2']]


def test_document_iterator():
    """Test iterating over the document rows."""
    doc = CSVFile(filename=CSV_FILE)
    with doc.open() as reader:
        row_count = sum([1 for _ in reader])
    assert row_count == 10


def test_delete_file_on_close(tmpdir):
    """Test deleting a file when closing the document."""
    filename = os.path.join(tmpdir, 'data.csv')
    doc = CSVFile(filename=filename)
    with doc.writer() as writer:
        writer.write(['A', 'B'])
        writer.write(['1', '2'])
    doc.close()
    assert os.path.isfile(filename)
    doc = CSVFile(filename=filename, delete_on_close='|')
    assert doc.columns == ['A', 'B']
    with doc.open() as reader:
        rows = [row for _, _, row in reader]
    doc.close()
    assert rows == [['1', '2']]
    assert not os.path.isfile(filename)


def test_read_csv_file():
    """Test reading default CSV file."""
    doc = CSVFile(filename=CSV_FILE)
    assert doc.columns == ['agency', 'borough', 'state']
    with doc.open() as reader:
        rows = [row for row in reader]
    assert rows == [
        (0, 0, ['311', 'BK', 'NY']),
        (1, 1, ['NYPD', 'BK', 'NY']),
        (2, 2, ['FDNY', 'BK', 'NY']),
        (3, 3, ['DOE', 'BK', 'NY']),
        (4, 4, ['DOB', 'BK', 'NY']),
        (5, 5, ['DSNY', 'BK', 'NY']),
        (6, 6, ['NYPD', 'MN', 'NY']),
        (7, 7, ['FDNY', 'MN', 'NY']),
        (8, 8, ['DOE', 'BX', 'NY']),
        (9, 9, ['FDNY', 'BX', 'NJ'])
    ]


def test_read_data_frame():
    """Test reading a the tab-delimited document as as data frame."""
    df = CSVFile(filename=GZIP_FILE, compressed=True).to_df()
    assert list(df.columns) == [
        'Calendar Year',
        'Gender',
        'Physical Abuse',
        'Sexual Abuse',
        'Risk of Sexual Abuse',
        'Risk of Harm',
        'Emotional/Neglect'
    ]
    assert df.shape == (10, 7)


def test_read_tsv_file():
    """Test reading compressed, tab-delimited CSV."""
    doc = CSVFile(filename=GZIP_FILE, compressed=True)
    assert doc.columns == [
        'Calendar Year',
        'Gender',
        'Physical Abuse',
        'Sexual Abuse',
        'Risk of Sexual Abuse',
        'Risk of Harm',
        'Emotional/Neglect'
    ]
    with doc.open() as reader:
        rows = [row for row in reader]
    assert rows == [
        (0, 0, ['2018', 'Male', '98', '60', '21', '293', '29']),
        (1, 1, ['2018', 'Female', '69', '306', '54', '253', '29']),
        (2, 2, ['2017', 'Male', '89', '62', '47', '267', '17']),
        (3, 3, ['2017', 'Female', '70', '286', '36', '244', '18']),
        (4, 4, ['2016', 'Male', '76', '52', '47', '212', '17']),
        (5, 5, ['2016', 'Female', '57', '259', '68', '216', '20']),
        (6, 6, ['2015', 'Male', '105', '63', '61', '212', '24']),
        (7, 7, ['2015', 'Female', '66', '275', '89', '207', '27']),
        (8, 8, ['2014', 'Male', '88', '95', '82', '196', '14']),
        (9, 9, ['2014', 'Female', '73', '285', '134', '177', '17'])
    ]
