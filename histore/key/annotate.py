# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper methods for generating a read order list for documents that are
keyed by a primary key or by list of row index values.
"""

from typing import Iterable, List, Union

from histore.key.base import to_key


def pk_readorder(rows: Iterable, keys: Union[int, List[int]]) -> List:
    """Create a read order list for a document that derives row identifier from
    cell values using a list of primary key colums.

    Returns a list of 3-tuples with (row position, key, row position). The
    returned list is sorted by the key values.

    Parameters
    ----------
    rows: list
        List of document rows.
    keys: list(int)
        List of index positions for primary key columns.

    Returns
    -------
    list

    Raises
    ------
    ValueError
    """
    # For performance reasons we use two different implementations here
    # that distinguish between primary keys with a single column or with
    # multiple columns.
    readorder = list()
    if len(keys) == 1:
        keycol = keys[0]
        for values in rows:
            key = to_key(values[keycol])
            pos = len(readorder)
            readorder.append((pos, key, pos))
    else:
        for values in rows:
            key = tuple([to_key(values[c]) for c in keys])
            pos = len(readorder)
            readorder.append((pos, key, pos))
    return sorted(readorder, key=lambda r: r[1])
