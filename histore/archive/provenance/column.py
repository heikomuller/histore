# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Class representing provenance information for dataset columns in an archive.
Column provenance information is represented as operators that describe the
differences in a single dataset column between two dataset snapshots.
"""

import histore.archive.provenance.base as prov


class DeleteColumn(prov.ProvOp):
    """Provenance class representing the column deletion operator."""
    def __init__(self, key):
        """Initialize the operator type in the super class and the column
        identifier.

        Parameters
        ----------
        key: int
            Unique column identifier.
        """
        super(DeleteColumn, self).__init__(type=prov.PROV_DELETE, key=key)


class InsertColumn(prov.ProvOp):
    """Provenance class representing the column insert operator."""
    def __init__(self, key):
        """Initialize the operator type in the super class and the column
        identifier.

        Parameters
        ----------
        key: int
            Unique column identifier.
        """
        super(InsertColumn, self).__init__(type=prov.PROV_INSERT, key=key)


class UpdateColumn(prov.ProvOp):
    """Provenance class representing the column update operator. The column
    update operator maintains additional information about potential changes in
    the column position and the column name.
    """
    def __init__(self, key, name=None, position=None):
        """Initialize information about the parts of the column that have
        changed. This includes changes to the column name and the column
        position. Both types of changes are optional but at least one of them
        has to be given.

        Parameters
        ----------
        key: int
            Unique column identifier.
        name: histore.archive.provenance.value.UpdateValue, default=None
            Updated value representing the old and new column name.
        position: histore.archive.provenance.value.UpdateValue, default=None
            Updated value representing the old and new position of the column
            in the dataset schema.

        Raises
        ------
        ValueError
        """
        # Raise an error if both arguments are None
        if not name and not position:
            raise ValueError('no column changes given')
        super(UpdateColumn, self).__init__(type=prov.PROV_UPDATE, key=key)
        self.name = name
        self.position = position

    def updated_name(self):
        """Get updated column name.

        Returns
        -------
        dict
        """
        return self.name

    def updated_position(self):
        """Get updated column position.

        Returns
        -------
        histore.archive.provenance.value.UpdateValue
        """
        return self.position
