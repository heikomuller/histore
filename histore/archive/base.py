# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Archives are collections of snapshots of an evolving dataset."""

import pandas as pd

from histore.archive.reader import RowIndexReader
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore
from histore.document.base import PartialDocument, PKDocument, RIDocument

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
        """Access a dataset snapshot in the archive. Retrieves the datset that
        was commited with the given version identifier. Raises an error if the
        version identifier is unknown.

        Parameters
        ----------
        version: int
            Unique version identifier.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ValueError
        """
        # Ensure that the version exists in the snapshot index.
        if not self.snapshots().has_version(version):
            raise ValueError('unknown version {}'.format(version))
        # Get dataset schema at the given version.
        columns = self.schema().at_version(version)
        colids = [c.colid for c in columns]
        # Get the row values and their position.
        rows = list()
        reader = self.reader()
        while reader.has_next():
            row = reader.next()
            if row.timestamp.contains(version):
                pos, vals = row.at_version(version, colids, raise_error=False)
                rows.append((row.rowid, pos, vals))
        # Sort rows in ascending order.
        rows.sort(key=lambda r: r[1])
        # Create data frame for the retrieved snapshot.
        data, rowindex = list(), list()
        for rowid, _, vals in rows:
            data.append(vals)
            rowindex.append(rowid)
        return pd.DataFrame(data=data, index=rowindex, columns=columns)

    def commit(
        self, df, description=None, valid_time=None, match_by_name=True,
        renamed=None, renamed_to=True, partial=False, origin=None
    ):
        """Commit a new snapshot to the dataset archive. The given data frame
        represents the dataset snapshot that is being merged into the archive.
        The data frame may represent a complete snapshot of the data or only
        a partial snapshot. In the latter case, all columns and rows from the
        snapshot that the data frame originated from (origin) are considered
        unchanged.

        Matching of snapshot schema columns to archive columns can either be
        done by name or by their unique identifier (if present). When matching
        by name a snapshot version of origin is used to match against. Data
        frames or snapshot versions with duplicate column names cannot be used
        for matching columns by name.

        If no origin is specified, the last commited snapshot is assumed as the
        default snapshot of origin. If this is the first snapshot in the
        archive the partial flag cannot be True (will raise a ValueError).

        Returns the descriptor of the merged snapshot in the new version of
        the archive.

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

        Raises
        ------
        ValueError
        """
        # Ensure that partial is not set for an empty archive.
        if partial and self.is_empty():
            raise ValueError('merge partial snapshot into empty archive')
        # Use the last commited version as origin if match_by_name or partial
        # are True and origin is None.
        if (match_by_name or partial) and origin is None:
            last_snapshot = self.snapshots().last_snapshot()
            if last_snapshot:
                origin = last_snapshot.version
        # Get a modified snapshot list where the last entry represents the
        # new snapshot.
        snapshots = self.snapshots().append(
            valid_time=valid_time,
            description=description
        )
        version = snapshots.last_snapshot().version
        # Merge the new snapshot schema with the current archive schema.
        schema, matched_columns, unchanged_columns = self.schema().merge(
            columns=list(df.columns),
            version=version,
            match_by_name=match_by_name,
            renamed=renamed,
            renamed_to=renamed_to,
            partial=partial,
            origin=origin
        )
        # Create snapshot document for the given data frame. The document type
        # depends on how row identifier are generated.
        if self.primary_key is not None:
            doc = PKDocument(
                df=df,
                schema=matched_columns,
                primary_key=self.primary_key
            )
        else:
            doc = RIDocument(df=df, schema=matched_columns)
        # If the data frame is partial we need to adjust the positions of the
        # rows in the document.
        if partial:
            doc = PartialDocument(
                doc=doc,
                row_index=RowIndexReader(reader=self.reader(), version=origin)
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

    def is_empty(self):
        """True if the archive does not contain any snapshots yet.

        Returns
        -------
        bool
        """
        return self.store.is_empty()

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


class PersistentArchive(Archive):
    """Archive that persists all dataset snapshots in files on the file system.
    All the data is maintained within a given directory on the file system.

    This class is a wrapper for an archive with a persistent archive store. It
    uses the default file system store.
    """
    def __init__(
        self, basedir, replace=False, serializer=None, compression=None,
        primary_key=None
    ):
        """Initialize the associated archive store and the optional primary
        key columns that are used to generate row identifier. If no primary
        key is specified the row index for committed data frame is used to
        generate identifier for archive rows.

        Parameters
        ----------
        basedir: string
            Path to the base directory for archive files. If the directory does
            not exist it will be created.
        replace: boolean, default=False
            Do not read existing files in the base directory to initialize the
            archive. Treat the archive as being empty instead if True.
        serializer: histore.archive.serialize.ArchiveSerializer, default=None
            Implementation of the archive serializer interface that is used to
            serialize rows that are written to file.
        compression: string, default=None
            String representing the compression mode. Only te data file will be
            compressed. the metadata file is always storesd as plain text.
        primary_key: string or list
            Column(s) that are used to generate identifier for snapshot rows.
        """
        super(PersistentArchive, self).__init__(
            store=ArchiveFileStore(
                basedir=basedir,
                replace=replace,
                serializer=serializer,
                compression=compression
            ),
            primary_key=primary_key
        )
