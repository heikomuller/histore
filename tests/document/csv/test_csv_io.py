# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for CSV file streams."""

from pathlib import Path
from typing import List, Optional

import os
import pytest

from histore.document.csv.reader import CSVReader
from histore.document.csv.writer import CSVWriter


"""Input files for testing."""
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../.files')
GZIP_FILE = os.path.join(DIR, 'etnx-8aft.tsv.gz')


def create_file(
    filename: str, header: List[str], rows: List[List[str]],
    compressed: Optional[bool] = None, none_as: Optional[str] = None
) -> str:
    """Create CSV file for the given data."""
    with CSVWriter(filename=filename, compressed=compressed, none_as=none_as) as writer:
        writer.write(header)
        for row in rows:
            writer.write(row)
    return filename


def read_file(
    filename: str, compressed: Optional[bool] = None,
    none_is: Optional[str] = None
) -> List[List[str]]:
    """Read all lines in aCSV file. Assert that the length of each row matches
    the length of the file header. Returns a list with the read rows.
    """
    reader = CSVReader(filename=filename, compressed=compressed, none_is=none_is)
    rows = [row for row in reader]
    return rows


def test_close_writer(tmpdir):
    """Test closing a CSV writer multiple times."""
    filename = os.path.join(tmpdir, 'myfile.csv')
    writer = CSVWriter(filename=filename)
    writer.write(['A', 'B'])
    writer.close()
    writer.close()


@pytest.mark.parametrize('compressed', [True, False])
def test_compressed_readwrite(compressed, tmpdir):
    """Test reading and writing compressed and un-compressed files."""
    tmpfile = create_file(
        filename=os.path.join(tmpdir, 'myfile.csv'),
        header=['A', 'B'],
        rows=[[1, 'a'], [2, 'b'], [3, 'c']],
        compressed=compressed
    )
    doc = read_file(filename=tmpfile, compressed=compressed)
    assert doc == [['A', 'B'], ['1', 'a'], ['2', 'b'], ['3', 'c']]


def test_none_converter(tmpdir):
    """Test converting encodings of None to None when reading and writing a
    CSV file.
    """
    # Create a CSV file with three rows where None is encoded as n.
    tmpfile = create_file(
        filename=os.path.join(tmpdir, 'myfile.txt'),
        header=['A', 'B'],
        rows=[[1, 'a'], [2, None], [None, 'c']],
        none_as='\\N'
    )
    doc = read_file(filename=tmpfile)
    assert doc == [['A', 'B'], ['1', 'a'], ['2', '\\N'], ['\\N', 'c']]
    doc = read_file(filename=tmpfile, none_is='\\N')
    assert doc == [['A', 'B'], ['1', 'a'], ['2', None], [None, 'c']]


def test_read_empty_file(tmpdir):
    """Test reading an empty file."""
    filename = os.path.join(tmpdir, 'empty.tsv')
    Path(filename).touch()
    assert read_file(filename) == []


def test_read_tsv_file():
    """Test reading a compressed, tab-delimited CSV file."""
    reader = CSVReader(filename=GZIP_FILE, compressed=True, delim='\t')
    rows = [row for row in reader]
    assert len(rows) == 11
    assert rows[0] == [
        'Calendar Year',
        'Gender',
        'Physical Abuse',
        'Sexual Abuse',
        'Risk of Sexual Abuse',
        'Risk of Harm',
        'Emotional/Neglect'
    ]
    assert rows[10] == ['2014', 'Female', '73', '285', '134', '177', '17']
