# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Columns in an archive schema all have unique identifier together with
timestamped names and positions.
"""
from __future__ import annotations
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from histore.archive.provenance.base import ProvOp
from histore.archive.provenance.column import DeleteColumn, InsertColumn, UpdateColumn
from histore.archive.timestamp import SingleVersion, Timestamp
from histore.archive.value import SingleVersionValue
from histore.document.schema import Column, DocumentSchema


class ArchiveColumn(object):
    """Archive columns have a unique identifier together with a timestamped
    name and schema position.
    """
    def __init__(self, identifier: int, name: str, pos: int, timestamp: Timestamp):
        """initialize the object properties.

        Parameters
        ----------
        identifier: int
            Unique column identifier
        name: histore.archive.value.ArchiveValue
            History of names for the column.
        pos: histore.archive.value.ArchiveValue
            History of schema positions for the column.
        timestamp: histore.archive.timestamp.Timestamp
            Timestamp for the full history of the column.
        """
        self.identifier = identifier
        self.name = name
        self.pos = pos
        self.timestamp = timestamp

    def __repr__(self):
        """Unambiguous string representation of the archive column.

        Returns
        -------
        string
        """
        template = (
            '<ArchiveColumn (\n'
            '\tid=%d\n'
            '\tname=%s\n'
            '\tpos=%s\n'
            '\tts=%s\n)>'
        )
        return template % (
            self.identifier,
            str(self.name),
            str(self.pos),
            str(self.timestamp)
        )

    def at_version(self, version: int) -> Tuple[str, int]:
        """Get the name and schema position for the column in the given
        version of a dataset. Returns a tuple of (name, pos).

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        (string, int)
        """
        return self.name.at_version(version), self.pos.at_version(version)

    def diff(self, original_version: int, new_version: int) -> ProvOp:
        """Get provenance information representing the difference for this
        column between the original version and a new version.

        The three main types of changes that the column can experience are:

        - InsertColumn: The column did not exists in the original version but
          in the new version.
        - DeleteColumn: The column did exist in the original version but not in
          the new version.
        - UpdateColumn: Column updates can be comprised of two types of
          changes: (i) The column position changed, or (ii) the column name
          changed.

        If the column has no changes between both versions the result is None.

        Parameters
        ----------
        original_version: int
            Unique identifier for the original version.
        new_version: int
            Unique identifier for the version that the original version is
            compared against.

        Returns
        -------
        histore.archive.provenance.base.ProvOp
        """
        exists_in_orig = self.timestamp.contains(original_version)
        exists_in_new = self.timestamp.contains(new_version)
        if exists_in_orig and not exists_in_new:
            # Column has been deleted
            return DeleteColumn(self.identifier)
        elif not exists_in_orig and exists_in_new:
            # Column has been inserted
            return InsertColumn(self.identifier)
        elif exists_in_orig and exists_in_new:
            # Check if column name or position has been updated.
            upd_name = self.name.diff(original_version, new_version)
            upd_pos = self.pos.diff(original_version, new_version)
            if upd_pos is not None or upd_name is not None:
                return UpdateColumn(
                    key=self.identifier,
                    name=upd_name,
                    position=upd_pos
                )
        # Column has not changed
        return None

    def merge(self, name: str, pos: int, version: int) -> ArchiveColumn:
        """Create a modified version of the column for the given version. Adds
        the name and index position to the respective history of the column.
        Returns a modified copy of the column.

        Parameters
        ----------
        name: string
            Column name.
        pos: int
            Position of the column in the snapshot schema.

        Returns
        -------
        histore.archive.schema.ArchiveColumn
        """
        return ArchiveColumn(
            identifier=self.identifier,
            name=self.name.merge(value=name, version=version),
            pos=self.pos.merge(value=pos, version=version),
            timestamp=self.timestamp.append(version)
        )

    def rollback(self, version: int) -> ArchiveColumn:
        """Rollback the versions of the column.

        Truncates the timestamps of the column to the given rollback version.
        Returns a new archive column object or None if the column did not exist
        at or prior to the rollback version.

        Parameters
        ----------
        version: int
            Unique identifier of the rollback version.

        Returns
        -------
        histore.archive.schema.ArchiveColumn
        """
        ts = self.timestamp.rollback(version=version)
        if ts.is_empty():
            return None
        return ArchiveColumn(
            identifier=self.identifier,
            name=self.name.rollback(version=version),
            pos=self.pos.rollback(version=version),
            timestamp=ts
        )


class ArchiveSchema(object):
    """The archive schema maintains an index of columns in the history of a
    dataset. Each column has a unique identifier together with timestamped
    names and positions.
    """
    def __init__(self, columns: List[ArchiveColumn] = None):
        """Initialize the index of columns in the schema. Raises a ValueError
        if the identifier of columns are not unique.

        Parameters
        ----------
        columns: dict or list, default=None
            Dictionary or list of archive columns. In a dictionary, columns are
            indexed by their unique identifier. If a list is given it will be
            converted to a dictionary.
        """
        if columns is None:
            # Assert that columns is not None
            self.columns = dict()
        elif isinstance(columns, list):
            # If columns is a list convert it to a dictionary
            self.columns = dict()
            for c in columns:
                if c.identifier in self.columns:
                    raise ValueError('duplicate identifier %d' % c.identifier)
                self.columns[c.identifier] = c
        elif isinstance(columns, dict):
            self.columns = columns
        else:
            raise ValueError('expected list or dict of columns')

    def __iter__(self):
        """Make column list iterable."""
        return iter(self.columns.values())

    def __repr__(self):
        """Unambiguous string representation of the archive schema.

        Returns
        -------
        string
        """
        return '\n'.join(str(c) for c in self.columns.values())

    def at_version(self, version: int) -> List[Column]:
        """Get the schema for the snapshot at the given version of a dataset.
        Returns a list of snapshot columns in their original order.

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        list(histore.document.schema.Column)
        """
        # Get a list of (identifier, name, pos) for all columns in the snapshot
        # schema at the given version.
        cols = list()
        for col in self.columns.values():
            if col.timestamp.contains(version):
                name, pos = col.at_version(version)
                cols.append((col.identifier, name, pos))
        # Sort columns based on their position and return a list of snapshot
        # columns.
        cols.sort(key=lambda x: x[2])
        return [Column(colid=id, name=name, colidx=pos) for id, name, pos in cols]

    def merge(
        self, columns: DocumentSchema, version: int, origin: Optional[int] = None,
        renamed: Optional[List[Tuple[str, str]]] = None
    ) -> Tuple[ArchiveSchema, List[int]]:
        """Add the given snapshot columns to the schema history for a dataset.

        Expects a list of strings or identifiable column objects. If column
        objects are given their identifier must match an existing identifier
        in the schema and all identifier have to be unique. Column names that
        are given as string-only are considered to be new columns.

        When matching by name the columns in the given list are matched against
        columns in the schema of the origin version. Columns that are unmatched
        are considered new columns. If the list of columns of the origin
        schema contains duplicate column names matching by name is not possible
        and an error is raised.

        Returns the new archive schema and a list of column identifier for the
        document columns. The order in the identifier list corresponds to the
        order of columns in the document schema.

        Parameters
        ----------
        columns: list
            List of column names in their original order. Column names can
            either be strings of snapshot columns.
        version: int
            Identifier of the new version.
        origin: int
            Identifier of the previos snapshot. This identifier is required
            when matching columns by name.
        renamed: list of tuple of str, str, default=None
            Optional mapping of columns that have been renamed. Tuples contain
            the old and the new columns name.

        Returns
        -------
        histore.archive.schema.ArchiveSchema, list

        Raises
        ------
        ValueError
        """
        # Initialize the result list of identifier for document columns.
        document_columns = [None] * len(columns)
        # Create type-dependent mappings for identifiable and non-identifiable
        # columns. Identifiable columns are mapped to a tuple of position and
        # columns. Non-identifiable columns are mapped from their name to their
        # position in the document schema.
        id_cols, name_cols = document_schema(columns)
        # Create an index for the new schema columns.
        schema = dict()
        # Add identifiable columns to the new archive schema. Merge them with
        # an existing column that has the same identifier.
        for colid in id_cols:
            pos, name = id_cols[colid]
            if colid in self.columns:
                col = self.columns[colid].merge(name=name, pos=pos, version=version)
            else:
                col = create_column(colid=colid, name=name, pos=pos, version=version)
            schema[colid] = col
            document_columns[pos] = colid
        # Matched non-identifiable columns by name.
        if name_cols:
            # Create mapping of new names to old names for renamed columns.
            renamed_to = {new: old for old, new in renamed} if renamed else dict()
            # Create a mapping of column names in the schema of the previous
            # snapshot to their column identifier. Note that column names are
            # not necessarily unique. Thus, we map names to list of identifier.
            # If a document column is mapped to a previous column with multiple
            # identifier a ValueError is raised.
            prev_schema = schema_mapping(self.at_version(origin)) if origin is not None else dict()
            # Counter for new column identifier.
            colids = list(id_cols.keys()) + list(self.columns.keys())
            col_counter = max(colids) + 1 if colids else 0
            # Match named columns. Columns that are unmatched are considered
            # new columns.
            for name, pos in name_cols.items():
                colname = renamed_to.get(name, name)
                matches = prev_schema.get(colname, [])
                if len(matches) > 1:
                    raise ValueError("cannot match '{}' to unique column".format(name))
                if len(matches) == 1:
                    # Found matching column.
                    colid = matches[0]
                    col = self.columns[colid].merge(name=name, pos=pos, version=version)
                else:
                    # Create a new column.
                    colid = col_counter
                    col = create_column(colid=colid, name=name, pos=pos, version=version)
                    col_counter += 1
                if colid in schema:
                    # Raise an error if a archive column is matched to multiple
                    # document columns.
                    raise ValueError("multiple matches for {}".format(colid))
                schema[colid] = col
                document_columns[pos] = colid
        # Add remaining un-matched archive columns to the new schema.
        for colid, col in self.columns.items():
            if colid not in schema:
                schema[colid] = col
        return ArchiveSchema(columns=list(schema.values())), document_columns

    def rollback(self, version: int) -> ArchiveSchema:
        """Rollback timestamps of the archive schema.

        Truncates the timestamp of all columns. Returns a new schema containing
        only those columns that existed prior or at the given rollback version.
        the result may be an empty schema but never None.

        Parameters
        ----------
        version: int
            Unique identifier of the rollback version.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        columns = dict()
        for colid, col in self.columns.items():
            col = col.rollback(version=version)
            if col is not None:
                columns[colid] = col
        return ArchiveSchema(columns=columns)


# -- Helper Methods -----------------------------------------------------------

def create_column(colid: int, name: str, pos: int, version: int) -> ArchiveColumn:
    """Create a new archive column object for a document schema column.

    Parameters
    ----------
    colid: int
        Unique column identifier.
    name: str
        Column name.
    pos: int
        Column positon in the document schema.
    version: int
        Snapshot version identifier.

    Returns
    -------
    histore.archive.schema.ArchiveColumn
    """
    ts = SingleVersion(version=version)
    return ArchiveColumn(
        identifier=colid,
        name=SingleVersionValue(value=name, timestamp=ts),
        pos=SingleVersionValue(value=pos, timestamp=ts),
        timestamp=ts
    )


def document_schema(columns: DocumentSchema) -> Tuple[Dict[int, Tuple[int, str]], Dict[str, int]]:
    """Create type-dependent mappings for document schema columns.

    For columns that are identifiable a mapping from the column identifier to
    tuples of column objects and their index position is created. A ValueError
    is raised if the identifier is not unique.

    For columns that are not identifiable a second mapping is created that maps
    their name to their index position. Raises a ValueError if the names of
    non-identifiable columns are not unique.

    Parameters
    ----------
    columns: list
        List of column names in their original order. Column names can
        either be strings of snapshot columns.

    Returns
    -------
    tuple of dict, dict
    """
    id_cols, name_cols = dict(), dict()
    for colpos, col in enumerate(columns):
        if isinstance(col, Column):
            colid = col.colid
            if colid in id_cols:
                raise ValueError("duplicate column identifier '{}'".format(colid))
            id_cols[colid] = (colpos, col)
        else:
            if col in name_cols:
                raise ValueError("duplicate column '{}'".format(col))
            name_cols[col] = colpos
    return id_cols, name_cols


def schema_mapping(columns: List[Column]) -> Dict(str, List):
    """Create a mapping of column names in the schema of the previous snapshot
    to their column identifier.

    Column names are mapped to lists of identifiers since column names do not
    have to be unique.

    Parameters
    ----------
    columns: list of histore.document.schema.Column
        Columns in a document schema.

    Returns
    -------
    dict
    """
    prev_schema = defaultdict(list)
    for col in columns:
        prev_schema[col].append(col.colid)
    return prev_schema
