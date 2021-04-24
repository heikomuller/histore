import sys
import time

from histore.archive.serialize.compact import CompactSerializer
from histore.archive.store.fs.reader import ArchiveFileReader
from histore.document.json.reader import JsonReader


def read_archive_file(filename):
    start_time = time.perf_counter()
    f = ArchiveFileReader(filename=filename, serializer=CompactSerializer())
    rows = 0
    row = f.next()
    while row is not None:
        rows += 1
        row = f.next()
    end_time = time.perf_counter()
    print('Read archive with {} rows in {:0.4f} sec.'.format(rows, (end_time - start_time)))


def read_json_file(filename):
    start_time = time.perf_counter()
    f = JsonReader(filename=filename)
    rows = 0
    for obj in f:
        rows += 1
    end_time = time.perf_counter()
    print('Read document with {} rows in {:0.4f} sec.'.format(rows, (end_time - start_time)))


if __name__ == '__main__':
    read_archive_file(sys.argv[1])
