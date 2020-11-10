# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Classes representing provenance information for dataset rows in an archive.
Row provenance information is represented as operators that describe the
differences in a single dataset row between two dataset snapshots.
"""

import histore.archive.provenance.base as prov


class DeleteRow(prov.ProvOp):
    """Provenance class representing the row deletion operator."""
    def __init__(self, key):
        """Initialize the operator type in the super class and the row key.

        Parameters
        ----------
        key: scalar or tuple
            Unique row key.
        """
        super(DeleteRow, self).__init__(type=prov.PROV_DELETE, key=key)


class InsertRow(prov.ProvOp):
    """Provenance class representing the row insert operator."""
    def __init__(self, key):
        """Initialize the operator type in the super class and the row key.

        Parameters
        ----------
        key: scalar or tuple
            Unique row key.
        """
        super(InsertRow, self).__init__(type=prov.PROV_INSERT, key=key)


class UpdateRow(prov.ProvOp):
    """Provenance class representing the row update operator. The row update
    operator maintains additional information about potential changes in the
    row position and the row cells.
    """
    def __init__(self, key, cells=None, position=None):
        """Initialize information about the parts of the row that have changed.
        This includes changes to cell values and the row position. Both types
        of changes are optional but at least one of them has to be given.

        Parameters
        ----------
        key: scalar or tuple
            Unique row key.
        cells: dict, default=None
            Dictionary mapping column identifier to changed cell values.
        position: histore.archive.provenance.value.UpdateValue, default=None
            Updated value representing the old and new position of the row in
            the dataset.

        Raises
        ------
        ValueError
        """
        # Raise an error if both arguments are None
        if not cells and not position:
            raise ValueError('no row changes given')
        super(UpdateRow, self).__init__(type=prov.PROV_UPDATE, key=key)
        self.cells = cells
        self.position = position

    def updated_cells(self):
        """Get dictionary mapping column identifier to updated cell values.

        Returns
        -------
        dict
        """
        return self.cells

    def updated_position(self):
        """Get updated row position.

        Returns
        -------
        histore.archive.provenance.value.UpdateValue
        """
        return self.position
