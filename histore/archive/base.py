# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Archives are collections of snapshots of an evolving dataset."""

from histore.archive.reader import RowIndexReader
from histore.archive.store.mem import VolatileArchiveStore
from archive.document.base import Document

import histore.archive.merge as nested_merge


class Archive(object):
    """An archive maintains a list of snapshot handles. All snapshots are
    expected to follow the same document schema.

    Archives are represented as trees. The root of the tree is maintained by
    an archive store. The store provides a layer of abstraction such that the
    archive object does not have to deal with the different ways in which
    archives are managed by different systems.
    """
    def __init__(self, store=None, primary_key=None):
        """Initialize the associated archive store and the optional primary
        key columns that are used to generate row identifier. If no primary
        key is specified the row index for committed data frame is used to
        generate identifier for archive rows.

        Parameters
        ----------
        store: histore.archive.store.base.ArchiveStore, default=None
            Associated archive store that is used to maintain all archive
            information. Uses the volatile in-memory store by default.
        primary_key: string or list
            Column(s) that are used to generate identifier for snapshot rows.
        """
        self.primary_key = primary_key
        self.store = store if store is not None else VolatileArchiveStore()

    def checkout(self, version):
        """
        """
        raise NotImplementedError()

    def commit(
        self, df, description=None, valid_time=None, match_by_name=True,
        renamed=None, renamed_to=True, partial=False, origin=None
    ):
        """
        Parameters
        ----------
        df: pandas.DataFrame
            Data frame representing the dataset snapshot that is being merged
            into the archive.
        description: string, default=None
            Optional user-provided description for the snapshot.
        valid_time: datetime.datetime
            Timestamp when the snapshot was first valid. A snapshot is valid
            until the valid time of the next snapshot in the archive.
        match_by_name: bool, default=True
            Match columns from the given snapshot to columns in the origin
            schema by name instead of matching columns by identifier against
            the columns in the archive schema.
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
            If True the given snapshot is assumed to be partial. All columns
            from the snapshot schema that is specified by origin that are not
            matched by any column in the snapshot schema are assumed to be
            unchanged. All rows from the orignal snapshot that are not in the
            given snapshot are also assumed to be unchnged.
        origin: int, default=None
            Version identifier of the original column against which the given
            column list is matched.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        # Get a modified snapshot list where the last entry represents the
        # new snapshot
        snapshots = self.snapshots().append(
            valid_time=valid_time,
            description=description
        )
        version = snapshots.last_snapshot().version
        # Merge the new snapshot schema with the current archive schema. Use
        # the last commited version as origin if match_by_name or partial are
        # True and origin is None.
        if (match_by_name or partial) and origin is None:
            origin = self.snapshots().last_snapshot().version()
        schema, matched_columns, unchanged_columns = self.schema().merge(
            columns=list(df.columns),
            version=version,
            match_by_name=match_by_name,
            renamed=renamed,
            renamed_to=renamed_to,
            partial=partial,
            origin=origin
        )
        # Create snapshot document for the given data frame. If the data frame
        # is partial we need to get the row identifier and position for all
        # existing rows first.
        if partial:
            row_index = RowIndexReader(reader=self.reader(), version=origin)
        else:
            row_index = None
        doc = Document(
            df=df,
            schema=matched_columns,
            primary_key=self.primary_key,
            origin_index=row_index
        )
        # Merge document rows into the archive.
        writer = self.store.get_writer()
        nested_merge.merge_rows(
            archive=self,
            document=doc,
            version=version,
            writer=writer,
            partial=partial,
            unchanged_cells=[c.colid for c in unchanged_columns],
            origin=origin
        )
        # Commit all changes to the associated archive store.
        self.store.commit(schema=schema, writer=writer, snapshots=snapshots)
        # Return descriptor for the created snapshot.
        return snapshots.last_snapshot()

    def reader(self):
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.reader.ArchiveReader
        """
        return self.store.get_reader()

    def schema(self):
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        return self.store.get_schema()

    def snapshots(self):
        """Get listing of all snapshots in the archive.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing
        """
        return self.store.get_snapshots()
