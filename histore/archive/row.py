# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Rows in an archived dataset. Each row has a timestamp, the index positions
of the row in the history of the dataset, and the cell values in the row
history.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Union

from histore.key import KeyValue
from histore.archive.provenance.base import ProvOp
from histore.archive.provenance.row import DeleteRow, InsertRow, UpdateRow
from histore.archive.value import ArchiveValue, SingleVersionValue
from histore.archive.timestamp import SingleVersion, Timestamp


class ArchiveRow(object):
    """Each row in an archived dataset has a unique row identifier. The row
    maintains the history of index positions that it had, and the history of
    all cell values.
    """
    def __init__(
        self, rowid: int, key: KeyValue, pos: ArchiveValue,
        cells: Dict[int, ArchiveValue], timestamp: Timestamp
    ):
        """Initialize the row identifier and the objects that maintain history
        information about the row index positions and cell values.

        Parameters
        ----------
        rowid: int
            Unique internal row identifier
        key: histore.key.KeyValue, or tuple
            Derived row key for matching and merging purposes.
        pos: histore.archive.value.ArchiveValue
            Index positions for the row in the history of the dataset.
        cells: dict(int, histore.archive.value.ArchiveValue)
            Mapping of column identifier to cell values over the history of the
            dataset.
        timestamp: histore.archive.timestamp.Timestamp
            Sequence of version from the dataset history in which the row was
            present.
        """
        if isinstance(key, tuple):
            for k in key:
                assert isinstance(k, KeyValue)
        else:
            assert isinstance(key, KeyValue)
        self.rowid = rowid
        self.key = key
        self.cells = cells
        self.pos = pos
        self.timestamp = timestamp

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
            '\n\ttimestamp=[{}]'
            '\n\tpos={}'
            '\n\tvalues={})>'
        ).format(
            self.rowid,
            self.key,
            str(self.timestamp),
            str(self.pos),
            self.cells
        )

    def at_version(
        self, version: int, columns: List[int], raise_error: Optional[bool] = True
    ) -> Tuple[int, List]:
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
        int, list
        """
        values = list()
        for colid in columns:
            # There may not be a value for the given column identifier. If the
            # raise error flag is True we will raise an error. Otherwise, None
            # is returned as the result.
            value = self.cells.get(colid)
            if value is None:
                if raise_error:
                    raise ValueError('unknown column {}'.format(colid))
            else:
                value = value.at_version(version, raise_error=raise_error)
            values.append(value)
        return self.pos.at_version(version, raise_error=raise_error), values

    def comp(self, key: Union[KeyValue, Tuple[KeyValue, ...]]):
        """Compare the identifier of the archive row to the given row key. The
        result is:

        - -1 if the archive key is lower that the given key or if the given
             key is None,
        -  0 if both keys are equal, or
        -  1 if the archive key is greater than the given key.

        Parameters
        ----------
        key: histore.key.KeyValue, or tuple
            Key value of a document row.

        Returns
        -------
        int
        """
        if self.key < key:
            return -1
        elif self.key > key:
            return 1
        else:
            return 0

    def diff(self, original_version: int, new_version: int) -> ProvOp:
        """Get provenance information representing the difference for this
        row between the original version and a new version.

        The three main types of changes that the row can experience are:

        - InsertRow: The row did not exists in the original version but in the
          new version.
        - DeleteRow: The row did exist in the original version but not in the
          new version.
        - UpdateRow: Row updates can be comprised of two types of changes:
                     (i) The row position changed
                     (ii) One or more row values were updated

        If the row has no changes between both versions the result is None.

        Parameters
        ----------
        original_version: int
            Unique identifier for the original version.
        new_version: int
            Unique identifier for the version that the original version is
            compared against.

        Returns
        -------
        histore.archive.provenance.row.RowOp
        """
        exists_in_orig = self.timestamp.contains(original_version)
        exists_in_new = self.timestamp.contains(new_version)
        if exists_in_orig and not exists_in_new:
            # Row has been deleted
            return DeleteRow(self.key)
        elif not exists_in_orig and exists_in_new:
            # Row has been inserted
            return InsertRow(self.key)
        elif exists_in_orig and exists_in_new:
            # Check if row has been updated.
            upd_cells = dict()
            for colid, value in self.cells.items():
                upd_val = value.diff(original_version, new_version)
                if upd_val is not None:
                    upd_cells[colid] = upd_val
            upd_pos = self.pos.diff(original_version, new_version)
            if upd_pos is not None or len(upd_cells) > 0:
                return UpdateRow(
                    key=self.key,
                    cells=upd_cells,
                    position=upd_pos
                )
        # Row has not changed
        return None

    def extend(self, version: int, origin: int) -> ArchiveRow:
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
            pos=self.pos.extend(version=version, origin=origin),
            cells=ext_cells,
            timestamp=self.timestamp.append(version)
        )

    def merge(self, values: Dict, pos: int, version: int,) -> ArchiveRow:
        """Create a new version of the dataset row for a given set of cell
        values and a specified index position.

        Values is a dictionary that maps column identifier to cell values in
        the new row. Columns that are not present in the dictionary have been
        deleted.

        Parameters
        ----------
        values: dict
            Cell values in the new row. The dictionary is a mapping from column
            identifier to cell values.
        pos: int
            Index position of the row in the dataset snapshot.
        version: int
            Identifier of the new row version.

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
                    timestamp=SingleVersion(version=version)
                )
            history[colid] = cell
        # For missing columns add them as they are (e.g., if the column
        # has been deleted).
        for colid, cell in self.cells.items():
            if colid not in history:
                history[colid] = cell
        # Return a modified archive row.
        return ArchiveRow(
            rowid=self.rowid,
            key=self.key,
            pos=self.pos.merge(value=pos, version=version),
            cells=history,
            timestamp=self.timestamp.append(version)
        )

    def rollback(self, version: int) -> ArchiveRow:
        """Rollback the timestamps of the row.

        Truncates the timestamp of all cell values and the row positions.
        Returns a new row or None if the row did not exist prior or at the
        given rollback version.

        Parameters
        ----------
        version: int
            Unique identifier of the rollback version.

        Returns
        -------
        histore.archive.row.ArchiveRow
        """
        # Rollback the row timestamp first. If the result is empty we return
        # None.
        ts = self.timestamp.rollback(version=version)
        if ts.is_empty():
            return None
        # Return a modified row with rolled back row position and cell values.
        # Ignore cell values that did not exists at or prior to the rollback
        # version.
        cells = dict()
        for colid, value in self.cells.items():
            value = value.rollback(version=version)
            if value is not None:
                cells[colid] = value
        return ArchiveRow(
            rowid=self.rowid,
            key=self.key,
            pos=self.pos.rollback(version=version),
            cells=cells,
            timestamp=ts
        )
