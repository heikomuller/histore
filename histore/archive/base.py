# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Archives are collections of snapshots of an evolving dataset."""

from datetime import datetime
from typing import Dict, Optional, Union

import pandas as pd

from histore.archive.provenance.archive import SnapshotDiff
from histore.archive.reader import ArchiveReader, RowPositionReader
from histore.archive.schema import ArchiveSchema, MATCH_ID, MATCH_IDNAME
from histore.archive.snapshot import SnapshotListing
from histore.archive.store.base import ArchiveStore
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore
from histore.document.base import Document, PrimaryKey
from histore.document.csv.base import CSVFile
from histore.document.csv.read import open_document
from histore.document.mem.dataframe import DataFrameDocument

import histore.archive.merge as nested_merge


"""Type aliases for archive API methods."""
InputDocument = Union[pd.DataFrame, CSVFile, str]


class Archive(object):
    """An archive maintains a list of snapshot handles. All snapshots are
    expected to follow the same document schema.

    Archives are represented as trees. The root of the tree is maintained by
    an archive store. The store provides a layer of abstraction such that the
    archive object does not have to deal with the different ways in which
    archives are managed by different systems.
    """
    def __init__(
        self, store: Optional[ArchiveStore] = None, primary_key: Optional[PrimaryKey] = None
    ):
        """Initialize the associated archive store and the optional primary
        key columns that are used to generate row identifier. If no primary
        key is specified the row index for committed document is used to
        generate identifier for archive rows.

        Parameters
        ----------
        store: histore.archive.store.base.ArchiveStore, default=None
            Associated archive store that is used to maintain all archive
            information. Uses the volatile in-memory store by default.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for snapshot rows.
        """
        self.primary_key = primary_key
        self.store = store if store is not None else VolatileArchiveStore()

    def checkout(self, version: Optional[int] = None) -> pd.DataFrame:
        """Access a dataset snapshot in the archive. Retrieves the datset that
        was commited with the given version identifier. Raises an error if the
        version identifier is unknown. If no version identifier is given the
        last snapshot will be returned by default.

        Parameters
        ----------
        version: int, default=None
            Unique version identifier. By default the last version is returned.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ValueError
        """
        # Use the last snapshot as default if no version is specified.
        if version is None:
            version = self.snapshots().last_snapshot().version
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
        # Create document for the retrieved snapshot.
        data, rowindex = list(), list()
        for rowid, _, vals in rows:
            data.append(vals)
            rowindex.append(rowid)
        return pd.DataFrame(data=data, index=rowindex, columns=columns)

    def commit(
        self, doc: InputDocument, description: Optional[str] = None,
        valid_time: Optional[datetime] = None, matching: Optional[str] = MATCH_IDNAME,
        renamed: Optional[Dict] = None, renamed_to: Optional[bool] = True,
        partial: Optional[bool] = False, origin: Optional[int] = None
    ):
        """Commit a new snapshot to the dataset archive. The given document
        represents the dataset snapshot that is being merged into the archive.
        The document may represent a complete snapshot of the data or only
        a partial snapshot. In the latter case, all columns and rows from the
        snapshot that the document originated from (origin) are considered
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
        doc: histore.document.base.Document, pd.DataFrame,
                histore.document.csv.base.CSVFile, or string
            Input document representing the dataset snapshot that is being
            merged into the archive.
        description: string, default=None
            Optional user-provided description for the snapshot.
        valid_time: datetime.datetime
            Timestamp when the snapshot was first valid. A snapshot is valid
            until the valid time of the next snapshot in the archive.
        matching: string, default='idname'
            Match mode for columns. Expects one of three modes:
            - idonly: The columns in the schema of the comitted document are
            matched against columns in the archive schema by their identifier.
            Assumes that columns in the document schema are instances of the
            class histore.document.schema.Column.
            - nameonly: Columns in the commited document schema are matched
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
        # Documents may optionally be specified as data frames or CSV files.
        # Ensure that we have an instance of the Document class.
        doc = to_document(doc=doc, primary_key=self.primary_key)
        try:
            # Use the last commited version as origin if matching columns by
            # name or if a partial document is commited and origin is None.
            if (matching != MATCH_ID or partial) and origin is None:
                last_snapshot = self.snapshots().last_snapshot()
                if last_snapshot:
                    origin = last_snapshot.version
            # Get a modified snapshot list where the last entry represents the
            # new snapshot.
            version = self.snapshots().next_version()
            # Merge the new snapshot schema with the current archive schema.
            schema, matched_columns, unchanged_columns = self.schema().merge(
                columns=doc.columns,
                version=version,
                matching=matching,
                renamed=renamed,
                renamed_to=renamed_to,
                partial=partial,
                origin=origin
            )
            # If the document is partial we need to adjust the positions of the
            # rows in the document.
            if partial:
                posreader = RowPositionReader(
                    reader=self.reader(),
                    version=origin
                )
                doc = doc.partial(reader=posreader)
            # Merge document rows into the archive.
            writer = self.store.get_writer()
            nested_merge.merge_rows(
                arch_reader=self.reader(),
                doc_reader=doc.reader(schema=matched_columns),
                version=version,
                writer=writer,
                partial=partial,
                unchanged_cells=[c.colid for c in unchanged_columns],
                origin=origin
            )
        finally:
            # Ensure that the close method for the document is called to allow
            # for cleanup of temporary files.
            doc.close()
        # Commit all changes to the associated archive store.
        snapshot = self.store.commit(
            schema=schema,
            writer=writer,
            version=version,
            valid_time=valid_time,
            description=description
        )
        # Return descriptor for the created snapshot.
        return snapshot

    def diff(self, original_version: int, new_version: int) -> SnapshotDiff:
        """Get provenance information representing the difference between two
        dataset snapshots.

        Parameters
        ----------
        original_version: int
            Unique identifier for the original version.
        new_version: int
            Unique identifier for the version that the original version is
            compared against.

        Returns
        -------
        histore.archive.provenance.archive.SnapshotDiff
        """
        prov = SnapshotDiff()
        # Get changes in the dataset schema.
        schema_diff = prov.schema()
        for colid, column in self.schema().columns.items():
            col_prov = column.diff(original_version, new_version)
            if col_prov is not None:
                schema_diff.add(col_prov)
        # Get changes in dataset rows.
        rows_diff = prov.rows()
        reader = self.reader()
        row_count = 0
        while reader.has_next():
            row = reader.next()
            row_prov = row.diff(original_version, new_version)
            if row_prov is not None:
                rows_diff.add(row_prov)
            row_count += 1
        return prov

    def is_empty(self) -> bool:
        """True if the archive does not contain any snapshots yet.

        Returns
        -------
        bool
        """
        return self.store.is_empty()

    def reader(self) -> ArchiveReader:
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.reader.ArchiveReader
        """
        return self.store.get_reader()

    def schema(self) -> ArchiveSchema:
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        return self.store.get_schema()

    def snapshots(self) -> SnapshotListing:
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
        primary_key=None, encoder=None, decoder=None
    ):
        """Initialize the associated archive store and the optional primary
        key columns that are used to generate row identifier. If no primary
        key is specified the row index for committed document is used to
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
        encoder: json.JSONEncoder, default=None
            Encoder used when writing archive rows as JSON objects to file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file.
        """
        super(PersistentArchive, self).__init__(
            store=ArchiveFileStore(
                basedir=basedir,
                replace=replace,
                serializer=serializer,
                compression=compression,
                encoder=encoder,
                decoder=decoder
            ),
            primary_key=primary_key
        )


# -- Helper Functions ---------------------------------------------------------

def to_document(doc: InputDocument, primary_key: Optional[PrimaryKey] = None) -> Document:
    """Ensure that a given document object is an instance of class Document.
    Inputs may alternatively be specified as pandas data frames or CSV files.

    Parameters
    ----------
    doc: histore.document.base.Document, pd.DataFrame,
            histore.document.csv.base.CSVFile, or string
        Input document representing a dataset snapshot.
    primary_key: string or list, default=None
        Optional primary key columns for the document.

    Returns
    -------
    histore.document.base.Document
    """
    if isinstance(doc, pd.DataFrame):
        # If the given document is a pandas DataFrame we need to wrap it in
        # the appropriate document class. The document type depends on how row
        # keys are generated.
        return DataFrameDocument(df=doc, primary_key=primary_key)
    elif isinstance(doc, CSVFile):
        return open_document(file=doc, primary_key=primary_key)
    elif isinstance(doc, str):
        return open_document(file=CSVFile(doc), primary_key=primary_key)
    return doc
