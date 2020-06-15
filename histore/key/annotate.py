# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper methods for generating a read order list for documents that are
keyed by a primary key or by list of row index values.
"""

import numpy as np

from histore.key.base import NewRow, NumberKey, to_key


def pk_readorder(columns, rows, primary_key):
    """Create a read order list for a document that derives row identifier from
    cell values using a list of primary key colums.

    Returns a list of 3-tuples with (row position, key, row position). The
    returned list is sorted by the key values.

    Parameters
    ----------
    columns: list
        List of column names. The number of values in each row is expected
        to be the same as the number of columns and the order of values in
        each row is expected to correspond to their respective column in
        this list.
    rows: list
        List of document rows. Each row in the list is an array of the same
        length as the number of columns in the document.
    primary_key: string or list
        Name(s) of columns in the data frame schema from which the row keys
        are generated. If the primary key is not given the row index in the
        data frame is used as the row identifier. In the latter case all
        row identifier are expected to be integer or converted to integer.
        Negative values are assumed to represent new rows that will receive
        a new row identifier during merge.

    Returns
    -------
    list

    Raises
    ------
    ValueError
    """
    # Generate mapping for the document schema from column names to index
    # position.
    schema = dict()
    for colidx in range(len(columns)):
        name = columns[colidx]
        if name in schema:
            if isinstance(schema[name], list):
                schema[name].append(colidx)
            else:
                schema[name] = [schema[name], colidx]
        else:
            schema[name] = colidx
    # Map columns in the primary key to index position for cell values.
    pk = list()
    if type(primary_key) not in [list, tuple]:
        # For convenience we ensure that we have a list of columns.
        primary_key = [primary_key]
    for name in primary_key:
        colidx = schema.get(name)
        if colidx is None:
            raise ValueError('unknown column {}'.format(name))
        elif isinstance(colidx, list):
            msg = 'primary key includes duplicate name {}'
            raise ValueError(msg.format(name))
        pk.append(colidx)
    # For performance reasons we use two different implementations here
    # that distinguish between primary keys with a single column or with
    # multiple columns.
    readorder = list()
    if len(pk) == 1:
        keycol = pk[0]
        for values in rows:
            key = to_key(values[keycol])
            pos = len(readorder)
            readorder.append((pos, key, pos))
    else:
        for values in rows:
            key = tuple([to_key(values[c]) for c in pk])
            pos = len(readorder)
            readorder.append((pos, key, pos))
    return sorted(readorder, key=lambda r: r[1])


def rowindex_readorder(index):
    """Create a read order list from a given list of row index values. The
    index list, for example, corresponds to the row index of a pandas data
    frame.

    Index values are considered as primary key values for document rows. Index
    values that are not positive integers are treated as new rows.

    Returns a list of 3-tuples with (row position, key, row position). The row
    position represents the index position of a row in the given list. The key
    is the value in the index at the poistion (or a new row identifier if the
    original value was negative integer or not of type integer). The returned
    list is sorted by the key values.

    Parameters
    ----------
    index: list
        List of row index values.

    Returns
    -------
    list
    """
    # Ensure that all row identifier are integers. Generate identifier for
    # those rows that have a non-integer index, None, or a negative value
    # as their index. These rows are considered new rows.
    readorder = list()
    for ridx in index:
        pos = len(readorder)
        is_int = isinstance(ridx, int) or isinstance(ridx, np.integer)
        if not is_int or ridx < 0:
            key = NewRow(identifier=pos)
        else:
            key = NumberKey(value=ridx)
        readorder.append((pos, key, pos))
    return sorted(readorder, key=lambda r: r[1])
