# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit test for sorting CSV documents."""

from tempfile import NamedTemporaryFile as TempFile

import os
import pytest

from histore.document.csv.base import CSVFile
from histore.document.json.base import JsonDocument
from histore.document.json.writer import JsonWriter
from histore.document.sort import SortEngine

import histore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
DATAFILE = os.path.join(DIR, '../.files/etnx-8aft.tsv.gz')


@pytest.fixture
def json_file(tmpdir):
    doc = CSVFile(DATAFILE)
    filename = os.path.join(tmpdir, 'data.json')
    with JsonWriter(filename=filename) as writer:
        writer.write(doc.columns)
        with doc.open() as reader:
            for pos, idx, values in reader:
                values = [int(values[0]), values[1], int(values[2])]
                writer.write((pos, idx, values))
    return filename


@pytest.mark.parametrize('buffersize', [20, 200, 16 * 1024 * 1024])
def test_sort_csv_file(buffersize):
    """Test sorting a CSV file."""
    doc = CSVFile(DATAFILE)
    columns = doc.columns
    doc = doc.sorted(keys=[1, 0], buffersize=buffersize)
    with doc.open() as reader:
        rows = [r for r in reader]
    assert doc.columns == columns
    assert rows == [
        (9, 9, ['2014', 'Female', '73', '285', '134', '177', '17']),
        (7, 7, ['2015', 'Female', '66', '275', '89', '207', '27']),
        (5, 5, ['2016', 'Female', '57', '259', '68', '216', '20']),
        (3, 3, ['2017', 'Female', '70', '286', '36', '244', '18']),
        (1, 1, ['2018', 'Female', '69', '306', '54', '253', '29']),
        (8, 8, ['2014', 'Male', '88', '95', '82', '196', '14']),
        (6, 6, ['2015', 'Male', '105', '63', '61', '212', '24']),
        (4, 4, ['2016', 'Male', '76', '52', '47', '212', '17']),
        (2, 2, ['2017', 'Male', '89', '62', '47', '267', '17']),
        (0, 0, ['2018', 'Male', '98', '60', '21', '293', '29'])
    ]


def test_sort_csv_without_buffer():
    """Test edge case where the merge step for the sort algorithm receives
    an empty buffer.
    """
    doc = CSVFile(DATAFILE)
    columns = doc.columns
    sort = SortEngine(buffersize=200)
    with doc.open() as reader:
        buffer, files = sort._split(reader=reader, columns=columns, keys=[1, 0])
        buffer.sort(key=lambda row: util.keyvalue(row[2], [1, 0]))
        with TempFile(delete=False, mode='w', newline='') as f_out:
            with JsonWriter(filename=f_out, encoder=sort.encoder) as writer:
                writer.write(columns)
                for row in buffer:
                    writer.write(row)
            files.append(f_out.name)
    filename = sort._mergesort(buffer=[], filenames=files, columns=columns, keys=[1, 0])
    doc = JsonDocument(
        filename=filename,
        encoder=sort.encoder,
        decoder=sort.decoder,
        delete_on_close=True
    )
    with doc.open() as reader:
        rows = [r for r in reader]
    assert doc.columns == columns
    assert rows == [
        (9, 9, ['2014', 'Female', '73', '285', '134', '177', '17']),
        (7, 7, ['2015', 'Female', '66', '275', '89', '207', '27']),
        (5, 5, ['2016', 'Female', '57', '259', '68', '216', '20']),
        (3, 3, ['2017', 'Female', '70', '286', '36', '244', '18']),
        (1, 1, ['2018', 'Female', '69', '306', '54', '253', '29']),
        (8, 8, ['2014', 'Male', '88', '95', '82', '196', '14']),
        (6, 6, ['2015', 'Male', '105', '63', '61', '212', '24']),
        (4, 4, ['2016', 'Male', '76', '52', '47', '212', '17']),
        (2, 2, ['2017', 'Male', '89', '62', '47', '267', '17']),
        (0, 0, ['2018', 'Male', '98', '60', '21', '293', '29'])
    ]


@pytest.mark.parametrize('buffersize', [200, 16 * 1024 * 1024])
def test_sort_json_document(buffersize, json_file, tmpdir):
    """Test sorting a Json document with integer and string values."""
    doc = JsonDocument(json_file)
    columns = doc.columns
    doc = doc.sorted(keys=[1, 0], buffersize=buffersize)
    with doc.open() as reader:
        rows = [r for r in reader]
    assert doc.columns == columns
    assert rows == [
        (9, 9, [2014, 'Female', 73]),
        (7, 7, [2015, 'Female', 66]),
        (5, 5, [2016, 'Female', 57]),
        (3, 3, [2017, 'Female', 70]),
        (1, 1, [2018, 'Female', 69]),
        (8, 8, [2014, 'Male', 88]),
        (6, 6, [2015, 'Male', 105]),
        (4, 4, [2016, 'Male', 76]),
        (2, 2, [2017, 'Male', 89]),
        (0, 0, [2018, 'Male', 98])
    ]
