# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Columns in an archive schema all have unique identifier together with
timestamped names and positions.
"""

from histore.archive.provenance.column import (
    DeleteColumn, InsertColumn, UpdateColumn
)
from histore.archive.timestamp import Timestamp
from histore.archive.value import SingleVersionValue
from histore.document.schema import Column


class ArchiveColumn(object):
    """Archive columns have a unique identifier together with a timestamped
    name and schema position.
    """
    def __init__(self, identifier, name, pos, timestamp):
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

    def at_version(self, version):
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

    def diff(self, original_version, new_version):
        """Get provenance information representing the difference for this
        column between the original version and a new version.

        The three main types of changes that the column can experience are:

        - InsertColumn: The column did not exists in the original version but
          in the new version.
        - DeleteColumn: The column did exist in the original version but not in
          the new version.
        - UpdateColumn: Column updates can be comprised of two types of
          changes:
            (i) The column position changed
            (ii) The column name changed

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
        histore.archive.provenance.column.ColumnOp
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

    def merge(self, name, pos, version):
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


class ArchiveSchema(object):
    """The archive schema maintains an index of columns in the history of a
    dataset. Each column has a unique identifier together with timestamped
    names and positions.
    """
    def __init__(self, columns=None):
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

    def at_version(self, version):
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
        return [Column(colid=id, name=name) for id, name, _ in cols]

    def merge(
        self, columns, version, match_by_name=False, renamed=None,
        renamed_to=True, partial=False, origin=None
    ):
        """Add the given snapshot columns to the schema history for a dataset.
        Expects a list of strings or identifiable column objects. If column
        objects are given their identifier must match an existing identifier
        in the schema and all identifier have to be unique. Column names that
        are given as string-only are considered to be to be new columns.

        If the match_by_name flag is True or if the partial flag is True the
        version of origin has to be given as well. When matching by name the
        columns in the given list a re matched against columns in the schema
        of the origin version. Columns that are unmatched are considered new
        columns. If either the list of columns of the origin schema contains
        duplicate column names matching by name is not possible and an error
        is raised.

        If the partial flag is true columns from the origin schema that are
        not matched by columns in the given list (either by name or by their
        identifier) are added to the resulting schema. An attempt is made to
        keep the order of columns as close as possible to the order of columns
        in the origin schema.

        Returns a modified archive schema, alist of column objects
        corresponding to the columns in the given list, and a list of column
        objects for unchanged columns.

        Parameters
        ----------
        columns: list
            List of column names in their original order. Column names can
            either be strings of snapshot columns.
        version: int
            Identifier of the new version.
        match_by_name: bool, default=False
            Match columns from the given list to columns in the origin schema
            by name instead of matching columns by identifier against the
            columns in the archive schema.
        renamed: dict, default=None
            Optional mapping of columns that have been renamed. Maps the new
            column name to the original name.
        renamed_to: bool, default=True
            Flag that determines the semantics of the mapping in the renamed
            dictionary. By default a mapping from the original column name
            (i.e., the dictionary key) to the new column name (the dictionary
            value) is assumed. If the flag is False a mapping from the new
            column name to the original column name is assumed.
        partial: bool, default=False
            If True the list of columns is assumed partial. All columns from
            the snapshot schema that is specified by origin that are not
            matched by any column in the input list are added to the schema
            for the new snapshot before merging the snapshot schema into the
            archive.
        origin: int, default=None
            Version identifier of the original column against which the given
            column list is matched.

        Returns
        -------
        histore.archive.schema.ArchiveSchema, list, list
        """
        if match_by_name or partial:
            # Ensure that a schema origin is given. Load the schema for the
            # specified version.
            if origin is not None:
                orig_schema = self.at_version(version=origin)
            else:
                orig_schema = list()
            if match_by_name:
                # Find matches for columns by name in the snapshot schema. Then
                # modify the columns list by replacing entries with their
                # matched schema columns (if matched).
                matches = match_columns(
                    columns=columns,
                    schema=orig_schema,
                    renamed=renamed,
                    renamed_to=renamed_to
                )
                columns = list()
                for snapcol, origcol in matches:
                    if origcol is not None:
                        # The snapshot column was matched with an existing
                        # column. The name of these columns, however, may be
                        # different if the column was renamed. Create a new
                        # column object with the name from the new snapshot and
                        # the identifier from the original schema.
                        c = Column(colid=origcol.colid, name=str(snapcol))
                        columns.append(c)
                    else:
                        columns.append(snapcol)
            if partial:
                return self.merge_incomplete(
                    columns=columns,
                    schema=orig_schema,
                    version=version
                )
            else:
                return self.merge_complete(columns=columns, version=version)
        else:
            # The given schema is complete. We match each column in the given
            # input list against the column with the same identifier or we
            # add a new column for unidentified columns.
            return self.merge_complete(columns=columns, version=version)

    def merge_complete(self, columns, version):
        """Merge a complete list of columns into the archive schema.

        Parameters
        ----------
        columns: list
            List of column names in their original order. Column names can
            either be strings of snapshot columns.
        version: int
            Identifier of the new version.

        Returns
        -------
        histore.archive.schema.ArchiveSchema, list, list

        Raises
        ------
        ValueError
        """
        arch_columns = dict()
        matched_columns = list()
        # Get counter for new columns.
        colids = max(self.columns) + 1 if len(self.columns) > 0 else 0
        # Merge columns with existing identifier and create new columns for
        # those that did not exists (i.e., the are instances of string and
        # not of snapshot columns).
        for pos in range(len(columns)):
            snapcol = columns[pos]
            if isinstance(snapcol, Column):
                # This is an existing column.
                if snapcol.colid not in self.columns:
                    raise ValueError('unknown column %d' % snapcol.colid)
                name = str(snapcol)
                col = self.columns[snapcol.colid]
                col = col.merge(name=name, pos=pos, version=version)
                matched_columns.append(snapcol)
            else:
                ts = Timestamp(version=version)
                col = ArchiveColumn(
                    identifier=colids,
                    name=SingleVersionValue(value=snapcol, timestamp=ts),
                    pos=SingleVersionValue(value=pos, timestamp=ts),
                    timestamp=ts
                )
                matched_columns.append(Column(colid=colids, name=snapcol))
                colids += 1
            if col.identifier in arch_columns:
                raise ValueError('duplicate column %d' % col.identifier)
            arch_columns[col.identifier] = col
        # Add original columns that are not in the modified schema
        for colid, col in self.columns.items():
            if colid not in arch_columns:
                arch_columns[colid] = col
        # Return a modified archive schema, the resulting list of matched
        # columns and the list of columns that remain unchanged.
        schema = ArchiveSchema(columns=arch_columns)
        return schema, matched_columns, list()

    def merge_incomplete(self, columns, schema, version):
        """Merge an incomplete list of columns into the archive schema. This
        method also Identifies those columns in the given schema that remain
        unmatched and returns them as the third value in the result tuple.

        Parameters
        ----------
        columns: list
            List of column names in their original order. Column names can
            either be strings of snapshot columns.
        schema: list(histore.document.schema.Column)
            List of columns in a snapshot schema.
        version: int
            Identifier of the new version.

        Returns
        -------
        histore.archive.schema.ArchiveSchema, list, list

        Raises
        ------
        ValueError
        """
        # Create a match array in the size of the origin schema. Assign the
        # index of matched columns from the column list to the position in the
        # array that corresponds to the position of the matched schema column.
        # Unmatched columns are added to a separate list.
        schema_matches = [-1] * len(schema)
        unmatched_columns = list()
        for c in range(len(columns)):
            snapcol = columns[c]
            if isinstance(snapcol, Column):
                for i in range(len(schema)):
                    if schema[i].colid == snapcol.colid:
                        schema_matches[i] = c
                        break
            else:
                unmatched_columns.append(c)
        # Create dictionary of modified schema columns, the list of matched
        # columns from the input and the list of unchanged columns from the
        # original schema.
        arch_columns = dict()
        matched_columns = [None] * len(columns)
        unchanged_columns = list()
        for c in range(len(schema)):
            if schema_matches[c] == -1:
                # The column at the index position c remains unchanged.
                # Merge the index position and name into the archive schema
                # column for the new version.
                col = schema[c]
                unchanged_columns.append(col)
            else:
                # The original column was matched by a column from the input
                # schema.
                colidx = schema_matches[c]
                col = columns[colidx]
                matched_columns[colidx] = col
            # Update the archive column with the name and position of the
            # matched (or original) column.
            arch_col = self.columns[col.colid]
            arch_col = arch_col.merge(name=col, pos=c, version=version)
            if arch_col.identifier in arch_columns:
                raise ValueError('duplicate column %d' % arch_col.identifier)
            arch_columns[arch_col.identifier] = arch_col
        # Get counter for new columns. Then add unmatched columns from the
        # new snapshot schema to the archive.
        colids = max(self.columns) + 1 if len(self.columns) > 0 else 0
        for c in unmatched_columns:
            snapcol = columns[c]
            ts = Timestamp(version=version)
            arch_col = ArchiveColumn(
                identifier=colids,
                name=SingleVersionValue(value=snapcol, timestamp=ts),
                pos=SingleVersionValue(value=len(arch_columns), timestamp=ts),
                timestamp=ts
            )
            matched_columns[c] = Column(colid=colids, name=snapcol)
            colids += 1
            if arch_col.identifier in arch_columns:
                raise ValueError('duplicate column %d' % arch_col.identifier)
            arch_columns[arch_col.identifier] = arch_col
        # Return a modified archive schema, the resulting list of matched
        # columns and the list of columns that remain unchanged.
        schema = ArchiveSchema(columns=arch_columns)
        return schema, matched_columns, unchanged_columns


# -- Helper Methods -----------------------------------------------------------

def match_columns(columns, schema, renamed=None, renamed_to=True):
    """Compute matching of given list of column names to columns in the given
    schema. Columns are matched by name. If either list contains duplicate
    column names an error is raised.

    Returns a list of tuples (column, match) where column is an element from
    the list of columns and match is either an element from the schema or None
    for unmatched columns. The order of elements in the result corresponds to
    the order of elements in the column list.

    Parameters
    ----------
    columns: list(string)
        List of column names.
    schema: list(histore.document.schema.Column)
        List of columns in a snapshot schema.
    renamed: dict, default=None
        Optional mapping of columns that have been renamed. Maps the new column
        name to the original name.
    renamed_to: bool, default=True
        Flag that determines the semantics of the mapping in the renamed
        dictionary. By default a mapping from the original column name (i.e.,
        the dictionary key) to the new column name (the dictionary value) is
        assumed. If the flag is False a mapping from the new column name to the
        original column name is assumed.

    Returns
    -------
    list(string, histore.document.schema.Column)

    Raises
    ------
    ValueError
    """
    # Ensure that the semantics of the renamed mapping is from the new name to
    # their original name. This makes it easier to lookup the column names in
    # the new snapshot schema to find their original column name.
    if renamed is None:
        renamed_from = dict()
    elif renamed_to:
        # Flip mapping of renamed columns.
        renamed_from = dict()
        for key, val in renamed.items():
            if val in renamed_from:
                raise ValueError('multiple columns renamed to same name')
            renamed_from[val] = key
    else:
        renamed_from = renamed
    # Create a dictionary of names for the snapshot schema.
    schema_index = dict()
    for col in schema:
        if col in schema_index:
            raise ValueError('cannot match by name for schema with duplicates')
        schema_index[col] = col
    # Match column names against the schema index. Keep track if column names
    # identifiy duplciates. Ensure that the result is a valid mapping where no
    # two names from the column list have been mapped to the same schema
    # element.
    matches = list()
    matched_schema_columns = set()
    names = set()
    for name in columns:
        if name in names:
            raise ValueError('duplicate column name %s' % (str(name)))
        names.add(name)
        # Find matching name in the schema index. Consider the fact that the
        # column may have been renamed from an original column.
        match = schema_index.get(renamed_from.get(name, name))
        if match is not None:
            if match.colid in matched_schema_columns:
                raise ValueError('multiple matches for %d' % match.colid)
            matched_schema_columns.add(match.colid)
        matches.append((name, match))
    return matches
