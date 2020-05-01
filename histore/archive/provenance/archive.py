# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Provenance information about differences between two snapshots in a dataset
archive.
"""

from histore.archive.provenance.base import Provenance
from histore.archive.provenance.describe import (
    RowProvDescribe, SchemaProvDescribe
)


PROV_ROWS = 'ROWS'
PROV_SCHEMA = 'SCHEMA'


class SnapshotDiff(object):
    """The snapsshot difference object maintains provenance information about
    the difference betweeen two dataset snapshots. The changes are divided into
    changes to the dataset schema and the dataset rows.
    """
    def __init__(self, schema=None, rows=None):
        """Initialize provenance information about chages the dataset schema
        and row changes.

        Parameters
        ----------
        schema: histore.archive.provenance.base.Provenance
        rows: histore.archive.provenance.base.Provenance
        """
        if schema is None:
            schema = Provenance(descriptor=SchemaProvDescribe())
        if rows is None:
            rows = Provenance(descriptor=RowProvDescribe())
        self.prov = {
            PROV_SCHEMA: schema,
            PROV_ROWS: rows
        }

    def describe(self):
        """Print summary of the maintained provenance information."""
        print('Schema Changes\n==============')
        self.schema().describe()
        print('\nData Changes\n============')
        self.rows().describe()

    def rows(self):
        """Set of row objects that have changed between the two snapshots.

        Returns
        -------
        histore.archive.provenance.base.Provenance
        """
        return self.prov[PROV_ROWS]

    def schema(self):
        """Set of schema objects that have changed between the two snapshots.

        Returns
        -------
        histore.archive.provenance.base.Provenance
        """
        return self.prov[PROV_SCHEMA]
