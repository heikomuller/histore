# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Archives are collections of snapshots of an evolving dataset."""

from collections import defaultdict
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

from histore.archive.manager.descriptor import decoder_from_string, encoder_from_string, serializer_from_dict
from histore.archive.provenance.archive import SnapshotDiff
from histore.archive.reader import ArchiveReader, SnapshotReader
from histore.archive.schema import ArchiveSchema
from histore.archive.snapshot import Snapshot, SnapshotListing
from histore.archive.store.base import ArchiveStore
from histore.archive.store.fs.base import ArchiveFileStore
from histore.archive.store.mem.base import VolatileArchiveStore
from histore.archive.writer import ValidatingArchiveWriter
from histore.document.base import Document, InputDescriptor
from histore.document.csv.base import CSVFile
from histore.document.df import DataFrameDocument
from histore.document.operator import DatasetOperator
from histore.document.reader import AnnotatedDocumentReader, DefaultDocumentReader
from histore.document.schema import DocumentSchema

import histore.archive.merge as nested_merge


"""Type aliases for archive API methods."""
InputDocument = Union[pd.DataFrame, str, Document]
# Primary key of a dataset.
PrimaryKey = Union[str, List[str]]


class Archive(object):
    """An archive maintains a list of dataset snapshots. Archive snapshots are
    maintained by an archive store. The store provides a layer of abstraction
    such that the archive object does not have to deal with the different ways
    in which archives are managed by different systems.
    """
    def __init__(
        self, store: Optional[ArchiveStore] = None, doc: Optional[InputDocument] = None,
        primary_key: Optional[PrimaryKey] = None, descriptor: Optional[InputDescriptor] = None,
        sorted: Optional[bool] = False, buffersize: Optional[float] = None,
        validate: Optional[bool] = False
    ):
        """Initialize the associated archive store.

        If the archive store is None a volatile store is used as the default. In
        this case the user can provide informaiton for a first document (and its
        primary key) that will be committed into the new archive.

        Parameters
        ----------
        store: histore.archive.store.base.ArchiveStore, default=None
            Associated archive store that is used to maintain all archive
            information. Uses the volatile in-memory store by default.
        doc: histore.archive.base.InputDocument, default=None
            Input document representing the initial dataset snapshot that is
            being loaded into the archive.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for snapshot rows.
        descriptor: histore.document.base.InputDescriptor, default=None
            Optional metadata for the created snapshot.
        sorted: bool, default=False
            Flag indicating if the document is sorted by the optional primary
            key attributes. Ignored if the archive is not keyed.
        buffersize: float, default=None
            Maximum size (in bytes) for the memory buffer when sorting the
            input document.
        validate: bool, default=False
            Validate that the resulting archive is in proper order before
            committing the action.
        """
        if store is not None:
            # If a archive store is given the user cannot provide a document with
            # a primary key.
            self.store = store
            if doc is not None:
                if primary_key is not None:
                    raise ValueError('cannot load initial dataset with primary key')
                # Load the document.
                self.commit(
                    doc=doc,
                    descriptor=descriptor,
                    sorted=sorted,
                    buffersize=buffersize,
                    validate=validate
                )
        else:
            # Create a volatile archive store. If a document is provided this
            # will be committed as the first snapshot.
            if doc is not None:
                doc = to_document(doc)
                # Get the expected identifier for the primary key columns.
                primary_key = get_key_columns(columns=doc.columns, primary_key=primary_key)
                store = VolatileArchiveStore(primary_key=primary_key)
                archive = Archive(store=store)
                archive.commit(
                    doc=doc,
                    descriptor=descriptor,
                    sorted=sorted,
                    buffersize=buffersize,
                    validate=validate
                )
            elif primary_key is not None:
                raise ValueError('missing snapshot document')
            else:
                store = VolatileArchiveStore()
            self.store = store

    def apply(
        self, operators: Union[DatasetOperator, List[DatasetOperator]],
        origin: Optional[int] = None, validate: Optional[bool] = None
    ) -> List[Snapshot]:
        """Apply a given operator or a sequence of operators on a snapshot in
        the archive.

        The resulting snapshot(s) will directly be merged into the archive. This
        method allows to update data in an archive directly without the need
        to checkout the snapshot first and then commit the modified version(s).

        Returns list of handles for the created snapshots.

        Note that there are some limitations for this method. Most importantly,
        the order of rows cannot be modified and neither can it insert new rows
        at this point. Columns can be added, moved, renamed, and deleted.
        It is also important to note that if rows are deleted the position
        information in the created snapshot will not reflect this accurately.
        That is, the ordering of rows will still be correct, but the positions
        are no longer absolute positions since some positin values may be
        missing.

        Parameters
        ----------
        operators: histore.document.operator.DatasetOperator or
                list of histore.document.stream.DatasetOperator
            Operator(s) that is/are used to update the rows in a dataset
            snapshot to create new snapshot(s) in this archive.
        origin: int, default=None
            Unique version identifier for the original snapshot that is being
            updated. By default the last version is updated.
        validate: bool, default=False
            Validate that the resulting archive is in proper order before
            committing the action.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        # Ensure that the origin version is set.
        origin = origin if origin is not None else self.snapshots().last_snapshot().version
        # Create pipeline of operators. Make sure to update the archive schema
        # according to the operators. Each operator will create a new snapshot
        # version.
        result = list()
        pipeline = list()
        schema = self.schema()
        origin_colids = [c.colid for c in schema.at_version(version=origin)]
        # Get snapshot identifier and schema information for each created
        # snapshot.
        snapshots = self.snapshots()
        for op in operators if isinstance(operators, list) else [operators]:
            version = snapshots.next_version()
            snapshots = snapshots.append(version=version, descriptor=op)
            result.append(snapshots.last_snapshot())
            # Merge the new snapshot schema with the current archive schema. Then
            # get the snapshot schema from the merged archive schema to ensure we
            # have all the column identifier.
            schema, columns = schema.merge(
                columns=op.columns,
                version=version,
                origin=origin
            )
            pipeline.append((op, version, columns))
        # Iterate over rows and apply the operator to those that belong to the
        # modified snapshot.
        writer = self.store.get_writer()
        if validate:
            writer = ValidatingArchiveWriter(writer=writer)
        with self.reader() as reader:
            for row in reader:
                if row.timestamp.contains(origin):
                    pos, vals = row.at_version(version=origin, columns=origin_colids)
                    for op, version, snapshot_colids in pipeline:
                        vals = op.handle(rowid=row.rowid, row=vals)
                        if vals is not None:
                            # Merge the archive row and the modified document row.
                            row = row.merge(
                                values={colid: val for colid, val in zip(snapshot_colids, vals)},
                                pos=pos,
                                version=version
                            )
                        else:
                            break
                writer.write_archive_row(row)
        # Commit all changes to the associated archive store. Make sure to
        # unwrapt the writer if validating.
        if validate:
            writer = writer.writer
        self.store.commit(
            schema=schema,
            writer=writer,
            snapshots=snapshots
        )
        # Return descriptor for the created snapshot.
        return result

    def checkout(self, version: Optional[int] = None) -> pd.DataFrame:
        """Access a dataset snapshot in the archive.

        Retrieves the datset that was commited with the given version
        identifier. Raises an error if the version identifier is unknown. If no
        version identifier is given the last snapshot will be returned by
        default.

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
        # Use the last snapshot as default if no version is specified. Ensure
        # a given version exists in the snapshot index.
        if version is None:
            version = self.snapshots().last_snapshot().version
        elif not self.snapshots().has_version(version):
            raise ValueError('unknown version {}'.format(version))
        # The index values for the returned data frame depend on the the type
        # of key that was used for the archive. If the archive does not have a
        # primary key, the row key represents the row index value. If a primary
        # key is defined, we use the row identifier as the row index value.
        is_keyed = self.store.primary_key() is not None
        # Get dataset schema at the given version.
        columns = self.schema().at_version(version)
        colids = [c.colid for c in columns]
        # Get the row values and their position.
        rows = list()
        with self.reader() as reader:
            for row in reader:
                if row.timestamp.contains(version):
                    pos, vals = row.at_version(version, colids, raise_error=False)
                    rowidx = row.rowid if is_keyed else row.key.value
                    rows.append((rowidx, pos, vals))
        # Sort rows in ascending order.
        rows.sort(key=lambda r: r[1])
        # Create document for the retrieved snapshot.
        data, rowindex = list(), list()
        for rowid, _, vals in rows:
            data.append(vals)
            rowindex.append(rowid)
        return pd.DataFrame(data=data, index=rowindex, columns=columns, dtype=object)

    def commit(
        self, doc: InputDocument, descriptor: Optional[InputDescriptor] = None,
        sorted: Optional[bool] = False, buffersize: Optional[float] = None,
        validate: Optional[bool] = None, renamed: Optional[List[Tuple[str, str]]] = None
    ) -> Snapshot:
        """Commit a new snapshot to the dataset archive.

        The given document represents the dataset snapshot that is being merged
        into the archive. Returns the descriptor of the merged snapshot in the
        new version of the archive.

        Parameters
        ----------
        doc: histore.archive.base.InputDocument, or string
            Input document representing the dataset snapshot that is being
            merged into the archive.
        descriptor: histore.document.base.InputDescriptor, default=None
            Optional metadata for the created snapshot.
        sorted: bool, default=False
            Flag indicating if the document is sorted by the optional primary
            key attributes. Ignored if the archive is not keyed.
        buffersize: float, default=None
            Maximum size (in bytes) for the memory buffer when sorting the
            input document.
        validate: bool, default=False
            Validate that the resulting archive is in proper order before
            committing the action.
        renamed: list of tuple of str, str, default=None
            Optional mapping of columns that have been renamed. Tuples contain
            the old and the new columns name.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        # Ensure that we have an instance of the Document class as input.
        doc = to_document(doc=doc)
        # Get the current snapshot version. If the archive is empty the origin
        # is None
        origin = self.snapshots().last_snapshot().version if not self.is_empty() else None
        try:
            # Get version identifier for the next snapshot.
            version = self.snapshots().next_version()
            # Merge the new snapshot schema with the current archive schema.
            schema, columns = self.schema().merge(
                columns=doc.columns,
                version=version,
                renamed=renamed,
                origin=origin
            )
            # Sort the document if the archive has a primary key and the
            # document is not sorted.
            if self.store.primary_key() and not sorted:
                mapping = {colid: colidx for colidx, colid in enumerate(columns)}
                doc = doc.sorted(
                    keys=[mapping[colid] for colid in self.store.primary_key()],
                    buffersize=buffersize
                )
            # Create the document reader. The type of document reader depends
            # on the type of key that is used for the document.
            with doc.open() as iterator:
                if self.store.primary_key():
                    reader = AnnotatedDocumentReader(
                        iterator=iterator,
                        columns=columns,
                        keys=self.store.primary_key()
                    )
                else:
                    reader = DefaultDocumentReader(iterator=iterator, columns=columns)
                # Merge document rows into the archive.
                validate = validate if validate is not None else sorted
                writer = self.store.get_writer()
                if validate:
                    writer = ValidatingArchiveWriter(writer=writer)
                nested_merge.merge_rows(
                    arch_reader=self.reader(),
                    doc_reader=reader,
                    version=version,
                    writer=writer
                )
        finally:
            # Ensure that the close method for the document is called to allow
            # for cleanup of temporary files.
            doc.close()
        # Commit all changes to the associated archive store. Make sure to
        # unwrapt the writer if validating.
        if validate:
            writer = writer.writer
        self.store.commit(
            schema=schema,
            writer=writer,
            snapshots=self.snapshots().append(version=version, descriptor=descriptor)
        )
        # Return descriptor for the created snapshot.
        return self.snapshots().last_snapshot()

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
        row_count = 0
        with self.reader() as reader:
            for row in reader:
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

    def open(self, version: Optional[int] = None) -> SnapshotReader:
        """Get a stream reader for a dataset snapshot.

        Parameters
        ----------
        version: int, default=None
            Unique version identifier. By default the last version is used.

        Returns
        -------
        histore.archive.reader.SnapshotReader
        """
        # Use the last snapshot as default if no version is specified.
        if version is None:
            version = self.snapshots().last_snapshot().version
        # Ensure that the version exists in the snapshot index.
        if not self.snapshots().has_version(version):
            raise ValueError('unknown version {}'.format(version))
        # Get dataset schema at the given version.
        columns = self.schema().at_version(version)
        return SnapshotReader(
            reader=self.reader,
            version=version,
            schema=columns,
            is_keyed=self.store.primary_key() is not None
        )

    def reader(self) -> ArchiveReader:
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.reader.ArchiveReader
        """
        return self.store.get_reader()

    def rollback(self, version: int):
        """Rollback the archive history to the snapshot with the given version
        identifier.

        Raises a ValueError if the given version identifier is invalid.

        Parameters
        ----------
        version: int
            Unique version identifier for the last version in the archive after
            rollback.

        Raises
        ------
        ValueError
        """
        # Ensure that the given version identifier is valid.
        if not self.snapshots().has_version(version=version):
            raise ValueError("unknown version '{}'".format(version))
        # Rollback archive rows.
        reader = self.reader()
        writer = self.store.get_writer()
        for row in reader:
            row = row.rollback(version)
            if row is not None:
                writer.write_archive_row(row)
        # Commit the rolledback archive to the associated archive store.
        self.store.rollback(
            schema=self.schema().rollback(version=version),
            writer=writer,
            version=version
        )

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
        self, store: Optional[ArchiveStore] = None, basedir: Optional[str] = None,
        create: Optional[bool] = False, encoder: Optional[str] = None,
        decoder: Optional[str] = None, serializer: Union[Dict, Callable] = None,
        doc: Optional[InputDocument] = None, primary_key: Optional[PrimaryKey] = None,
        descriptor: Optional[InputDescriptor] = None, sorted: Optional[bool] = False,
        buffersize: Optional[float] = None, validate: Optional[bool] = False
    ):
        """Initialize the associated archive store.

        If the archive store is None a volatile store is used as the default. In
        this case the user can provide informaiton for a first document (and its
        primary key) that will be committed into the new archive.

        Parameters
        ----------
        store: histore.archive.store.base.ArchiveStore, default=None
            Associated archive store that is used to maintain all archive
            information. Uses the volatile in-memory store by default.
        basedir: string
            Path to the base directory for archive files. If the directory does
            not exist it will be created.
        create: boolean, default=False
            Do not read existing files in the base directory to initialize the
            archive. Treat the archive as being empty instead if True.
        encoder: string, default=None
            Full package path for the Json encoder class that is used by the
            persistent archive.
        decoder: string, default=None
            Full package path for the Json decoder function that is used by the
            persistent archive.
        serializer: dict or callable, default=None
            Dictionary or callable that returns a dictionary that contains the
            specification for the serializer. The serializer specification is
            a dictionary with the following elements:
            - ``clspath``: Full package target path for the serializer class
                           that is instantiated.
            - ``kwargs`` : Additional arguments that are passed to the
                           constructor of the created serializer instance.
            Only ``clspath`` is required.
        doc: histore.archive.base.InputDocument, default=None
            Input document representing the initial dataset snapshot that is
            being loaded into the archive.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for snapshot rows.
        descriptor: histore.document.base.InputDescriptor, default=None
            Optional metadata for the created snapshot.
        sorted: bool, default=False
            Flag indicating if the document is sorted by the optional primary
            key attributes. Ignored if the archive is not keyed.
        buffersize: float, default=None
            Maximum size (in bytes) for the memory buffer when sorting the
            input document.
        validate: bool, default=False
            Validate that the resulting archive is in proper order before
            committing the action.
        """
        if store is not None:
            # If a archive store is given the user cannot provide a document with
            # a primary key.
            self.store = store
            if doc is not None:
                if primary_key is not None:
                    raise ValueError('cannot load initial dataset with primary key')
                # Load the document.
                self.commit(
                    doc=doc,
                    descriptor=descriptor,
                    sorted=sorted,
                    buffersize=buffersize,
                    validate=validate
                )
        else:
            # Create a persistent archive store. If a document is provided this
            # will be committed as the first snapshot.
            if doc is not None:
                doc = to_document(doc)
                # Get the expected identifier for the primary key columns.
                primary_key = get_key_columns(columns=doc.columns, primary_key=primary_key)
                # Get serializer dictionary if a function was given.
                serializer = serializer() if serializer is not None and callable(serializer) else serializer
                store = ArchiveFileStore(
                    basedir=basedir,
                    replace=create,
                    serializer=serializer_from_dict(serializer),
                    encoder=encoder_from_string(encoder),
                    decoder=decoder_from_string(decoder),
                    primary_key=primary_key
                )
                archive = Archive(store=store)
                archive.commit(
                    doc=doc,
                    descriptor=descriptor,
                    sorted=sorted,
                    buffersize=buffersize,
                    validate=validate
                )
            elif primary_key is not None:
                raise ValueError('missing snapshot document')
            else:
                store = ArchiveFileStore(
                    basedir=basedir,
                    replace=create,
                    serializer=serializer_from_dict(serializer),
                    encoder=encoder_from_string(encoder),
                    decoder=decoder_from_string(decoder)
                )
            self.store = store


# -- Helper Functions ---------------------------------------------------------

def get_key_columns(columns: DocumentSchema, primary_key: PrimaryKey) -> List[int]:
    """Get identifier for primary key columns.

    Uses the archive schema merge method to get the identifiable columns for the
    given schema. Then returns the identifier for the columns that match the
    primary key columns.

    Note that the primary key specification cannot reference columns that have
    non-unique names. If a primary key column occurs more than once in the
    resulting schema an error is raised. An error is also raised if the primary
    key references a column that does not exist.

    If the list of primary key columns is None or empty the result is None.

    Parameters
    ----------
    columns: list of string
        Schema for the input document.
    primary_key: list of string
        Names of primary key columns. May be None.

    Returns
    -------
    list of int
    """
    # Return None if the list of primary key columns is None or empty.
    if not primary_key:
        return None
    # Ensure that the primary key is a list.
    primary_key = primary_key if isinstance(primary_key, list) else [primary_key]
    # Get the identifiable columns for the document schema. Create a mapping
    # from the column name to the identifier(s).
    schema, _ = ArchiveSchema().merge(columns=columns, version=0)
    columns = defaultdict(list)
    for col in schema.at_version(0):
        columns[col].append(col.colid)
    # Generate the list of primary key column identifier.
    pk = list()
    for name in primary_key:
        col = columns.get(name)
        if not col:
            raise ValueError("unknown column '{}'".format(col))
        elif len(col) > 1:
            raise ValueError("not a unique key column '{}'".format(col))
        pk.append(col[0])
    return pk


def to_document(doc: InputDocument) -> Document:
    """Ensure that a given input document is an instance of class Document.

    For file names and pandas data frame inputs the repsective document is
    returned.

    Parameters
    ----------
    doc: histore.document.base.Document, pd.DataFrame,
            histore.document.csv.base.CSVFile, or string
        Input document representing a dataset snapshot.

    Returns
    -------
    histore.document.base.Document
    """
    if isinstance(doc, pd.DataFrame):
        # If the given document is a pandas DataFrame we need to wrap it in
        # the appropriate document class.
        return DataFrameDocument(df=doc)
    elif isinstance(doc, str):
        return CSVFile(filename=doc)
    return doc
