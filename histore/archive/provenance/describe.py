# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Descriptor classes that can be used to print summary information for a set
of provenance operators.
"""

from abc import ABCMeta, abstractmethod


class ProvDescriptor(metaclass=ABCMeta):
    """Abstract provenance summary printer."""
    @abstractmethod
    def describe(self, provenance):  # pragma: no cover
        """Print summary of the maintained provenance information."""
        raise NotImplementedError()


class DefaultDescribe(ProvDescriptor):
    """Default provenance summary printer. Outputs counts for inserted,
    deleted, and updated objects.
    """
    def describe(self, provenance):
        """Print summary of the maintained provenance information."""
        print('Inserted: {}'.format(len(provenance.insert())))
        print('Deleted : {}'.format(len(provenance.delete())))
        print('Updated : {}'.format(len(provenance.update())))


class RowProvDescribe(ProvDescriptor):
    """Print provenance summary information about changed rows."""
    def describe(self, provenance):
        """Print summary of the maintained provenance information."""
        updated_rows, updated_values, moved_rows = 0, 0, 0
        for upd_row in provenance.update():
            if upd_row.updated_position():
                moved_rows += 1
            if upd_row.updated_cells():
                updated_rows += 1
                updated_values += len(upd_row.updated_cells())
        print('Inserted Rows    : {}'.format(len(provenance.insert())))
        print('Deleted Rows     : {}'.format(len(provenance.delete())))
        print('Moved Rows       : {}'.format(moved_rows))
        print('Updated Rows     : {}'.format(updated_rows))
        print('Updated Values   : {}'.format(updated_values))


class SchemaProvDescribe(ProvDescriptor):
    """Print provenance summary information about schema changes."""
    def describe(self, provenance):
        """Print summary of the maintained provenance information."""
        moved_cols, renamed_cols = 0, 0
        for upd_row in provenance.update():
            if upd_row.updated_position():
                moved_cols += 1
            if upd_row.updated_name():
                renamed_cols += 1
        print('Inserted Columns : {}'.format(len(provenance.insert())))
        print('Deleted Columns  : {}'.format(len(provenance.delete())))
        print('Moved Columns    : {}'.format(moved_cols))
        print('Renamed Columns  : {}'.format(renamed_cols))
