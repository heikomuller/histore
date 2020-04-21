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

from histore.archive.value import SingleVersionValue
from histore.archive.timestamp import Timestamp


class ArchiveRow(object):
    """Each row in an archived dataset has a unique row identifier. The row
    maintains the history of index positions that it had, and the history of
    all cell values.
    """
    def __init__(self, rowid, index, cells, timestamp, key=None):
        """Initialize the row identifier and the objects that maintain history
        information about the row index positions and cell values.

        Parameters
        ----------
        rowid: int
            Unique internal row identifier
        index: histore.archive.value.ArchiveValue
            Index positions for the row in the history of the dataset.
        cells: dict(int, histore.archive.value.ArchiveValue)
            Mapping of column identifier to cell values over the history of the
            dataset.
        timestamp: histore.archive.timestamp.Timestamp
            Sequence of version from the dataset history in which the row was
            present.
        key: int, string, or tuple, default=None
            Derived row key for matching and merging purposes. If the key is
            None the rowid is used as key
        """
        self.rowid = rowid
        self.cells = cells
        self.index = index
        self.timestamp = timestamp
        self.key = key if key is not None else rowid

    def __repr__(self):
        """Unambiguous string representation of the archive row.

        Returns
        -------
        string
        """
        return (
            '<ArchiveRow ('
            '\n\tid={}'
            '\n\tkey={}'
            '\n\ttimestampts=[{}]'
            '\n\tpos={}'
            '\n\tvalues={})>'
        ).format(
            self.rowid,
            self.key,
            str(self.timestamp),
            str(self.index),
            self.cells
        )

    def at_version(self, version, columns, raise_error=True):
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
        raise_error: bool, default=True
            Flag that determines the behavior for cases where there is no value
            for the given version. If the flag is True an error is raised. If
            the flag is False None is returned.

        Returns
        -------
        int, dict
        """
        values = list()
        for colid in columns:
            # There may not be a value for the given column identifier. If the
            # raise error flag is True we will raise an error. Otherwise, None
            # is returned as the result.
            value = self.cells.get(colid)
            if value is None:
                if raise_error:
                    raise ValueError('unknown version %d' % version)
            else:
                value = value.at_version(version, raise_error=raise_error)
            values.append(value)
        return self.index.at_version(version, raise_error=raise_error), values

    def comp(self, key):
        """Compare the identifier of the archive row to the given row key. The
        result is:

        - -1 if the archive key is lower that the given key or if the given
             key is None,
        -  0 if both keys are equal, or
        -  1 if the archive key is greater than the given key.

        Parameters
        ----------
        key: int, string, or tuple
            Key value of a document row.

        Returns
        -------
        int
        """
        if key is None or self.key < key:
            return -1
        elif self.key > key:
            return 1
        else:
            return 0

    def extend(self, version, origin):
        """Extend the timestamp of the row and all cell values and the index
        position that were valid at the given version of origin.

        Parameters
        ----------
        version: int
            Unique identifier of the new version.
        origin: int
            Identifier of the version of origin.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        # If the row was not present at the given version of origin the object
        # is returned unchanged.
        if not self.timestamp.contains(origin):
            return self
        # Extend all cell values that were valid at the origin.
        ext_cells = dict()
        for colid, value in self.cells.items():
            ext_cells[colid] = value.extend(version=version, origin=origin)
        # Return an extended copy of the row. the row position is assumed to be
        # unchanged.
        return ArchiveRow(
            rowid=self.rowid,
            key=self.key,
            index=self.index.extend(version=version, origin=origin),
            cells=ext_cells,
            timestamp=self.timestamp.append(version)
        )

    def merge(self, values, pos, version, unchanged_cells=None, origin=None):
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
        unchanged_cells: set, default=None
            Set of identifier for columns whose cell values are not included in
            the values dictionary but that remain unchanged with respect to the
            specified source version (origin).
        origin: int, default=None
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
                cell = SingleVersionValue(
                    value=value,
                    timestamp=Timestamp(version=version)
                )
            history[colid] = cell
        # For missing columns either add them as they are (e.g., if the column
        # has been deleted) or if they are included in the unchanged set append
        # the new version to the timestamp for the value that was valid at the
        # source version.
        unchanged = unchanged_cells if unchanged_cells is not None else set()
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
            rowid=self.rowid,
            key=self.key,
            index=self.index.merge(value=pos, version=version),
            cells=history,
            timestamp=self.timestamp.append(version)
        )
