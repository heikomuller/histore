# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Rows in an archived dataset. Each row has a timestamp, the index positions
of the row in the history of the dataset, and the cell values in the row
history.
"""

from histore.archive.value import OmnipresentCell
from histore.archive.timestamp import Timestamp


class ArchiveRow(object):
    """Each row in an archived dataset has a unique row identifier. The row
    maintains the history of index positions that it had, and the history of
    all cell values.
    """
    def __init__(self, identifier, index, cells, timestamp):
        """Initialize the row identifier and the objects that maintain history
        information about the row index positions and cell values.

        Parameters
        ----------
        identifier: int
            Unique row identifier
        index: histore.archive.value.ArchiveValue
            Index positions for the row in the history of the dataset.
        cells: dict(int, histore.archive.value.ArchiveValue)
            Mapping of column identifier to cell values over the history of the
            dataset.
        timestamp: histore.archive.timestamp.Timestamp
            Sequence of version from the dataset history in which the row was
            present.
        """
        self.identifier = identifier
        self.cells = cells
        self.index = index
        self.timestamp = timestamp

    def at_version(self, version, columns):
        """Get cell values and the index position for the row in the given
        version of the dataset. Returns the index position of the row and a
        list of cell values for all columns in the given list. The order of
        returned values matches the order of columns in the argument.

        Parameters
        ----------
        version: int
            Unique version identifier.
        columns: list(int)
            List of columns for which values are returned at the given version.

        Returns
        -------
        int, dict
        """
        values = list()
        for colid in columns:
            values.append(self.cells[colid].at_version(version=version))
        return self.index.at_version(version=version), values

    def merge(self, values, pos, version, unchanged=None, origin=None):
        """Create a new version of the dataset row for a given set of cell
        values and a specified index position. The set of extend cells
        identifies those columns that are not present in the value dictionary
        but that remain valid in the new version with respect to the given
        source version (origin).

        Values is a dictionary that maps column identifier to cell values in
        the new row. Columns that are not present in the dictionary the have
        either be deleted or remain unchanged. In the latter case, these
        columns are included in the set of unchanged columns. Columns that
        are unchanged are keep the value that they had in the given source
        version (origin).

        Parameters
        ----------
        values: dict
            Cell values in the new row. The dictionary is a mapping from column
            identifier to cell values.
        pos: int
            Index position of the row in the dataset snapshot.
        version: int
            Identifier of the new row version.
        unchanged: set, optional
            Set of identifier for columns whose cell values are not included in
            the values dictionary but that remain unchanged with respect to the
            specified source version (origin).
        origin: int, optional
            Version that the row values originate from. Cell that remain
            unchanged have the timestamp extended for the version the was
            present the version of origin.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        # Create a modified dictionary of cell values for all columns.
        history = dict()
        for colid, value in values.items():
            if colid in self.cells:
                cell = self.cells[colid]
                cell = cell.merge(value=value, version=version)
            else:
                cell = OmnipresentCell(
                    value=value,
                    timestamp=Timestamp(version=version)
                )
            history[colid] = cell
        # For missing columns either add them as they are (e.g., if the column
        # has been deleted) or if they are included in the unchanged set append
        # the new version to the timestamp for the value that was valid at the
        # source version.
        unchanged = unchanged if unchanged is not None else set()
        for colid, cell in self.cells.items():
            if colid not in history:
                if colid in unchanged:
                    cell = cell.extend(
                        version=version,
                        origin=origin
                    )
                history[colid] = cell
        # Return a modified archive row.
        return ArchiveRow(
            identifier=self.identifier,
            index=self.index.merge(value=pos, version=version),
            cells=history,
            timestamp=self.timestamp.append(version)
        )
