# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Convert an existing archive JSON file from the format that was used by
histore prior to 0.4.0 into the new format.
"""

import sys


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) != 2:
        print('usage: <input-file> <output-file>')
        sys.exit(-1)
    file_in, file_out = args
    with open(file_in, 'rt') as fin:
        line = next(fin)
        with open(file_out, 'wt') as fout:
            while True:
                line = next(fin).strip()
                if line == ']':
                    break
                if line.endswith(','):
                    line = line[:-1]
                fout.write('{}\n'.format(line))
