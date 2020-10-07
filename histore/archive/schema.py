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


"""Constant identifier for the three different schema matching modes."""
MATCH_ID = 'idonly'
MATCH_IDNAME = 'idname'
MATCH_NAME = 'nameonly'


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
        return [
            Column(colid=id, name=name, colidx=pos) for id, name, pos in cols
        ]

    def merge(
        self, columns, version, matching=MATCH_IDNAME, renamed=None,
        renamed_to=True, partial=False, origin=None
    ):
        """Add the given snapshot columns to the schema history for a dataset.
        Expects a list of strings or identifiable column objects. If column
        objects are given their identifier must match an existing identifier
        in the schema and all identifier have to be unique. Column names that
        are given as string-only are considered to be to be new columns.

        If the column matching mode involves matching by name or if the partial
        flag is True the version of origin has to be given as well. When
        matching by name the columns in the given list are matched against
        columns in the schema of the origin version. Columns that are unmatched
        are considered new columns. If either the list of columns of the origin
        schema contains duplicate column names matching by name is not possible
        and an error is raised.

        If the partial flag is true columns from the origin schema that are
        not matched by columns in the given list (either by name or by their
        identifier) are added to the resulting schema. An attempt is made to
        keep the order of columns as close as possible to the order of columns
        in the origin schema.

        Returns a modified archive schema, a list of column objects
        corresponding to the columns in the given list, and a list of column
        objects from the original snapshot schema that were unmatched.

        Parameters
        ----------
        columns: list
            List of column names in their original order. Column names can
            either be strings of snapshot columns.
        version: int
            Identifier of the new version.
        matching: string, default='idname'
            Match mode for columns. Excepts one of three modes:
            - idonly: The columns in the schema of the comitted data frame are
            matched against columns in the archive schema by their identifier.
            Assumes that columns in the data frame schema are instances of the
            class histore.document.schema.Column.
            - nameonly: Columns in the commited data frame schema are matched
            by name against the columns in the schema of the snapshot that is
            identified by origin.
            - idname: Match columns of type histore.document.schema.Column
            first against the columns in the archive schema. Match remaining
            columns by name against the schema of the snapshot that is
            identified by origin.
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

        Raises
        ------
        ValueError
        """
        # Initialize the original schema that is needed when matching by name
        # or if a partial schema is given. Create mappings for column names to
        # archive columns and column position as well as for column identifier
        # to column and position.
        if origin is not None:
            orig_schema = self.at_version(origin)
            archive_index, orig_index = column_index(
                archive_schema=self.columns,
                orig_schema=orig_schema
            )
        else:
            orig_schema, archive_index = list(), dict()
        # Get alignment of data frame columns with archive columns by column
        # identifier. If matching is by name only we pass an empty dictionary
        # as archive schema so that all columns are unmatched. the result is a
        # list of tuples containing the data frame column and the matched
        # archive column (or None if unmatched).
        archive_cols = self.columns if matching != MATCH_NAME else dict()
        matches = columnid_match(df_cols=columns, archive_cols=archive_cols)
        if matching != MATCH_ID:
            # Find matches for columns by name in the snapshot schema. Then
            # modify the columns list by replacing entries with their
            # matched schema columns (if matched).
            matches = match_columns(
                columns=matches,
                schema_index=archive_index,
                renamed=renamed,
                renamed_to=renamed_to
            )
        # Create the new archive schema. Merge matched columns. Add columns
        # in the data frame schema that are unmatched. Keep track of unmatched
        # columns in the original schema.
        arch_columns = dict()
        matched_columns = list()
        colids = max(self.columns) + 1 if len(self.columns) > 0 else 0
        nextpos = len(orig_schema) if partial else 0
        for i in range(len(matches)):
            dfcol, archcol = matches[i]
            name = str(dfcol)
            if archcol is not None:
                # The snapshot column was matched with an existing archive
                # column. The name of these columns, however, may be different
                # if the column was renamed. Merge the snapshot column into the
                # matched archive column. Get the column position either from
                # the origin index or the nextpos counter.
                if partial:
                    if archcol.identifier in orig_index:
                        # Get column position. Then remove the matched column
                        # from the index.
                        _, pos = orig_index[archcol.identifier]
                        del orig_index[archcol.identifier]
                    else:
                        pos = nextpos
                        nextpos += 1
                else:
                    pos = i
                # Create a new column object with the
                # name from the new snapshot and the identifier from the
                # original schema.
                archcol = archcol.merge(name=name, pos=pos, version=version)
                col = Column(colid=archcol.identifier, name=name)
            else:
                # The snapshot column was not matched. Create a new column and
                # add it to the archive. Append the column to a partial schema
                # or use the original index as the column position.
                if partial:
                    pos = nextpos
                    nextpos += 1
                else:
                    pos = i
                ts = Timestamp(version=version)
                archcol = ArchiveColumn(
                    identifier=colids,
                    name=SingleVersionValue(value=name, timestamp=ts),
                    pos=SingleVersionValue(value=pos, timestamp=ts),
                    timestamp=ts
                )
                nextpos += 1
                col = Column(colid=colids, name=name)
                colids += 1
            if archcol.identifier in arch_columns:
                raise ValueError('duplicate column %d' % archcol.identifier)
            arch_columns[archcol.identifier] = archcol
            matched_columns.append(col)
        # Add original columns that are not in the modified schema
        unmatched_columns = list()
        for colid, archcol in self.columns.items():
            if colid not in arch_columns:
                if partial and colid in orig_index:
                    # Missing column from a partial schema.
                    dfcol, pos = orig_index[colid]
                    col = str(dfcol)
                    archcol = archcol.merge(name=col, pos=pos, version=version)
                    unmatched_columns.append(dfcol)
                arch_columns[colid] = archcol
        # Return a modified archive schema, the resulting list of matched
        # columns and the list of columns that remain unchanged.
        schema = ArchiveSchema(columns=arch_columns)
        return schema, matched_columns, unmatched_columns


# -- Helper Methods -----------------------------------------------------------

def columnid_match(df_cols, archive_cols):
    """Match columns in the given data frame schema against columns in the
    archive schema by their identifier. Returns a list of tuples where each
    tuple contains the data frame column and the matched archive schema column.
    If a data frame column is unmatched the second tuple value is None. The
    order of elements in the result corresponds to the order of columns in the
    input data frame schema.

    Parameters
    ----------
    df_cols: list
        List of strings or column objects from a pandas data frame schema.
    archive_cols: dict(histore.archive.schema.ArchiveColumn)
        Dictionary of columns in the schema of a dataset archive.

    Returns
    -------
    string or histore.document.schema.Column,
    histore.archive.schema.ArchiveColumn or None
    """
    alignment = list()
    for pos in range(len(df_cols)):
        dfcol = df_cols[pos]
        if isinstance(dfcol, Column):
            # Get archive column with the identifier if it exists.
            alignment.append((dfcol, archive_cols.get(dfcol.colid)))
        else:
            # Strings cannot be matched by column indentifier.
            alignment.append((dfcol, None))
    return alignment


def match_columns(columns, schema_index, renamed=None, renamed_to=True):
    """Compute matching of given list of 'id-aligned' columns to columns in the
    given snapshot schema. Columns are matched by name. If either list contains
    duplicate column names an error is raised.

    Returns a list of tuples (column, match) where column is an element from
    the list of columns and match is either an element from the schema or None
    for unmatched columns. The order of elements in the result corresponds to
    the order of elements in the column list.

    Parameters
    ----------
    columns: list((string, histore.archive.schema.ArchiveColumn or None)
        List of column names.
    schema_index: dict(string: histore.archive.schema.ArchiveColumn)
        Dictionary mapping of column names (in a snapshot schema) to their
        respective archive column.
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
    # Match column names against the schema index. Keep track if column names
    # identifiy duplciates. Ensure that the result is a valid mapping where no
    # two names from the column list have been mapped to the same schema
    # element.
    matches = list()
    matched_schema_columns = set()
    names = set()
    for df_col, arch_col in columns:
        if arch_col is not None:
            matches.append((df_col, arch_col))
            continue
        name = str(df_col)
        if name in names:
            raise ValueError('duplicate column name %s' % (str(name)))
        names.add(name)
        # Find matching name in the schema index. Consider the fact that the
        # column may have been renamed from an original column.
        match = schema_index.get(renamed_from.get(name, name))
        if match is not None:
            if match.identifier in matched_schema_columns:
                raise ValueError('multiple matches for %d' % match.colid)
            matched_schema_columns.add(match.identifier)
        matches.append((name, match))
    return matches


def column_index(archive_schema, orig_schema):
    """Create a mapping of names for the snapshot schema to their respective
    column in the archive schema. In addition, a mapping of identifier for
    columns in the snapshot schema to the column object at its schema position
    is created. Returns a tuple with the name mapping and the column identifier
    mapping.

    Parameters
    ----------
    archive_schema: dict(int: histore.archive.schema.ArchiveColumn)
        Dictionary mapping of column identifier to archive columns.
    orig_schema: list(histore.document.schema.Column)
        List of columns in a snapshot schema

    Returns
    -------
    dict, dict
    """
    archive_index = dict()
    orig_index = dict()
    for pos in range(len(orig_schema)):
        col = orig_schema[pos]
        if col in archive_index:
            raise ValueError('cannot match by name with duplicates')
        archive_index[col] = archive_schema[col.colid]
        orig_index[col.colid] = (col, pos)
    return archive_index, orig_index
