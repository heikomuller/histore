# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

import pandas as pd
import pytest

from histore.document.schema import Column, as_list, select_clause


@pytest.mark.parametrize(
    'arg,result',
    [
        (1, [1]),
        ([2, 3], [2, 3]),
        ((1, 2), [1, 2]),
        (pd.DataFrame(data=[[1], [2]], index=[0, 1], columns=['A']).index, [0, 1])
    ]
)
def test_as_list(arg, result):
    """Test the as_list conversion function."""
    assert as_list(arg) == result


def test_select_clause_error():
    """Test error for invalid select clause types."""
    with pytest.raises(ValueError):
        select_clause(schema=['A', 'B'], columns=[3.4])


@pytest.mark.parametrize(
    'columns,names,positions',
    [
        ('Name', ['Name'], [0]),
        ('Age', ['Age'], [1]),
        (['Salary', 'Age'], ['Salary', 'Age'], [2, 1]),
        ([2, 'Age'], ['Salary', 'Age'], [2, 1]),
        (('Salary', 'Age'), ['Salary', 'Age'], [2, 1]),
        ((0, 1), ['Name', 'Age'], [0, 1]),
        (['Name', 'Age', 'Salary'], ['Name', 'Age', 'Salary'], [0, 1, 2])
    ]
)
def test_select_clause_without_duplicates(columns, names, positions, employees):
    """Test the select clause method that extracts a list of column names and
    their index positions frm a data frame schema.
    """
    assert select_clause(employees.columns, columns) == (names, positions)


def test_select_clause_with_duplicates(dupcols):
    """Test the select clause method for a data frame schema with duplicate
    column names.
    """
    # -- Use only the columns name (returns the first 'A' column) -------------
    columns = ['Name', 'A']
    colnames, colidxs = select_clause(dupcols.columns, columns)
    assert colnames == ['Name', 'A']
    assert colidxs == [0, 1]
    # -- Use column objects (returns the second 'A' column) -------------------
    columns = [dupcols.columns[0], dupcols.columns[2]]
    colnames, colidxs = select_clause(dupcols.columns, columns)
    assert colnames == ['Name', 'A']
    assert colidxs == [0, 2]
    # -- Error if the column index is out of range ----------------------------
    columns = [dupcols.columns[0], Column(colid=10, name='A', colidx=20)]
    with pytest.raises(ValueError):
        select_clause(dupcols.columns, columns)
    # -- Error if the column index and the name don't match -------------------
    columns = [dupcols.columns[0], Column(colid=10, name='B', colidx=2)]
    with pytest.raises(ValueError):
        select_clause(dupcols.columns, columns)
