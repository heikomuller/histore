# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for CSV file streams."""

import os
import pytest

from histore.document.csv.base import CSVFile


"""Input files for testing."""
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../.files')
CSV_FILE = os.path.join(DIR, '311.csv')
GZIP_FILE = os.path.join(DIR, 'etnx-8aft.tsv.gz')


def test_none_converter(tmpdir):
    """Test converting encodings of None to None when reading and writing a
    CSV file.
    """
    # Create a CSV file with three rows where None is encoded as n.
    tmpfile = os.path.join(tmpdir, 'myfile.txt')
    with open(tmpfile, 'w') as f:
        f.write('A,B\n')
        f.write('1,n\n')
        f.write('n,1\n')
        f.write('n,n\n')
    # Read the file to ensure that 'n' is replaced by None
    file = CSVFile(tmpfile, none_is='n')
    with file.open() as f:
        rows = [r for _, r in f]
    assert rows == [['1', None], [None, '1'], [None, None]]
    # Write the file and replace None with '-'
    with file.write(header=['A', 'B'], none_as='-') as f:
        for row in rows:
            f.write(row)
    with open(tmpfile, 'r') as f:
        lines = [line.strip() for line in f]
    assert lines == ['A,B', '1,-', '-,1', '-,-']


@pytest.mark.parametrize(
    'filename,header,rows',
    [
        (CSV_FILE, ['descriptor', 'borough', 'city', 'street'], 99),
        (
            GZIP_FILE,
            [
                'Calendar Year',
                'Gender',
                'Physical Abuse',
                'Sexual Abuse',
                'Risk of Sexual Abuse',
                'Risk of Harm',
                'Emotional/Neglect'
            ],
            10
        )
    ]
)
def test_read_csv_file(filename, header, rows):
    """Test reading an exising plain CSV and tab-delimited, compressed CSV file."""
    dataset = CSVFile(filename)
    assert dataset.columns == header
    rowcount = 0
    for _, row in dataset.iterrows():
        assert len(row) == len(header)
        rowcount += 1
    assert rowcount == rows


@pytest.mark.parametrize('compressed', [True, False])
def test_read_file_without_header(compressed, tmpdir):
    """Read a CSV file that does not have a header."""
    # -- Setup ----------------------------------------------------------------
    # Write rows (without header) for 311 data file to a temporary text file.
    tmpfile = os.path.join(tmpdir, 'myfile.txt')
    header = ['A', 'B', 'C', 'D']
    file = CSVFile(tmpfile, header=header, compressed=compressed)
    write_count = 0
    with file.write() as writer:
        with CSVFile(CSV_FILE).open() as f:
            for _, row in f:
                writer.write(row)
                write_count += 1
    # -- Read the created CSV file with user-provided header ------------------
    file = CSVFile(tmpfile, compressed=compressed)
    assert file.columns == header
    read_count = len(read_file(file))
    assert read_count == write_count


def test_skip_lines():
    """Test skipping lines in a CSV file."""
    # If no header is given the first non-skipped line will be the header.
    file = CSVFile(filename=CSV_FILE, skip_lines=1)
    assert file.columns == ['No Parking, Standing, Stopping', 'MANHATTAN', 'NEW YORK', 'BARCLAY STREET']
    assert len(read_file(file)) == 98
    # Test skipping lines with given header
    file = CSVFile(filename=CSV_FILE, header=['A', 'B', 'C', 'D'], skip_lines=2)
    assert file.columns == ['A', 'B', 'C', 'D']
    rows = read_file(file)
    assert len(rows) == 98
    assert rows[0] == ['No Parking, Standing, Stopping', 'BROOKLYN', 'BROOKLYN', 'EBONY COURT']


def test_write_file(tmpdir):
    """Test writing CSV file."""
    filename = os.path.join(tmpdir, 'myfile.csv')
    file = CSVFile(filename, write=True)
    writer = file.write(header=['A', 'B'])
    writer.write([1, 2])
    writer.close()
    # Calling close a second time has no effect.
    writer.close()
    file = CSVFile(filename)
    assert file.columns == ['A', 'B']
    reader = file.open()
    assert next(reader) == (0, ['1', '2'])
    reader.close()
    # Ensure that closing a reader multiple times has no effect.
    reader.close()
    # Write without header
    file = CSVFile(filename, write=True)
    writer = file.write()
    writer.write([1, 2])
    writer.close()
    file = CSVFile(filename)
    assert file.columns == ['1', '2']


# -- Helper Funcitons ---------------------------------------------------------

def read_file(file):
    """Read all lines in aCSV file. Assert that the length of each row matches
    the length of the file header. Returns a list with the read rows.
    """
    header = file.columns
    rows = list()
    with file.open() as f:
        for rowid, row in f:
            assert len(row) == len(header)
            rows.append(row)
    return rows
