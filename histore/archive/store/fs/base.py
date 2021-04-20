# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Store for archives that are materialized as files on the file system. Each
archive is maintained in a separate folder on disk. For each archive two
different files are created.

- metadata.dat: Json file containing the archive schema, snapshot information,
  and the row counter.
- rows.dat: Data fie containing the archive rows.
"""

from typing import Callable, List, Optional

import json
import os
import shutil

from histore.archive.schema import ArchiveSchema
from histore.archive.serialize.default import ArchiveSerializer, DefaultSerializer
from histore.archive.snapshot import SnapshotListing
from histore.archive.store.base import ArchiveStore
from histore.archive.store.fs.reader import ArchiveFileReader
from histore.archive.store.fs.writer import ArchiveFileWriter
from histore.document.json.reader import default_decoder
from histore.document.json.writer import DefaultEncoder

import histore.util as util


"""Element labels for metadata serialization."""
META_PRIMARYKEY = 'primaryKey'
META_SCHEMA = 'schema'
META_SNAPSHOTS = 'snapshots'
META_ROWCOUNT = 'rowcount'


class ArchiveFileStore(ArchiveStore):
    """Archive store that maintains archive data in files on the file system.
    Maintains a copy of the schema and the snapshot descriptors in memory for
    faster access.
    """
    def __init__(
        self, basedir: str, primary_key: Optional[List[int]] = None,
        replace: Optional[bool] = False, serializer: Optional[ArchiveSerializer] = None,
        compression: Optional[str] = None, encoder: Optional[json.JSONEncoder] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the archive archive components.

        Parameters
        ----------
        basedir: string
            Path to the base directory for archive files. If the directory does
            not exist it will be created.
        primary_key: list of int, default=None
            List of identifier for primary key columns.
        replace: boolean, default=False
            Do not read existing files in the base directory to initialize the
            archive. Treat the archive as being empty instead if True.
        serializer: histore.archive.serialize.base.ArchiveSerializer, default=None
            Implementation of the archive serializer interface that is used to
            serialize rows that are written to file.
        compression: string, default=None
            String representing the compression mode. Only te data file will be
            compressed. the metadata file is always storesd as plain text.
        encoder: json.JSONEncoder, default=None
            Encoder used when writing archive rows as JSON objects to file.
        decoder: func, default=None
            Custom decoder function when reading archive rows from file.
        """
        self.basedir = util.createdir(basedir)
        self._primary_key = primary_key
        self.serializer = serializer if serializer else DefaultSerializer()
        self.compression = compression
        self.encoder = encoder if encoder is not None else DefaultEncoder
        self.decoder = decoder if decoder is not None else default_decoder
        # Initialize the file names
        self.datafile = os.path.join(self.basedir, 'rows.dat')
        self.metafile = os.path.join(self.basedir, 'metadata.dat')
        self.tmpdatafile = os.path.join(self.basedir, 'rows.tmp')
        self.tmpmetafile = os.path.join(self.basedir, 'metadata.tmp')
        if not replace and os.path.isfile(self.metafile):
            # Read schema and snapshot information from disk if the metadata
            # file exists.
            with open(self.metafile, 'r') as f:
                doc = json.load(f, object_hook=self.decoder)
            # Deserialize schema columns.
            columns = list()
            for c in doc.get(META_SCHEMA, []):
                columns.append(self.serializer.deserialize_column(c))
            self.schema = ArchiveSchema(columns=columns)
            # Deserialize snapshot descriptors
            snapshots = list()
            for s in doc.get(META_SNAPSHOTS, []):
                snapshots.append(self.serializer.deserialize_snapshot(s))
            self.snapshots = SnapshotListing(snapshots=snapshots)
            # Deserialize row counter.
            self.row_counter = doc.get(META_ROWCOUNT, 0)
            # Overwrite primary key if it is present in the metadata file.
            if self._primary_key:
                doc[META_PRIMARYKEY] = self._primary_key
                with open(self.tmpmetafile, 'w') as f:
                    json.dump(doc, f, cls=self.encoder)
            else:
                self._primary_key = doc[META_PRIMARYKEY]
        else:
            # Create an empty archive.
            self.schema = ArchiveSchema()
            self.snapshots = SnapshotListing()
            self.row_counter = 0
            # Remove any previous files that may exist in the base folder.
            for f in [self.datafile, self.metafile]:
                if os.path.isfile(f):
                    os.remove(f)
            # Write primary key to metadata file.
            with open(self.metafile, 'w') as f:
                json.dump({META_PRIMARYKEY: self._primary_key}, f)

    def commit(
        self, schema: ArchiveSchema, writer: ArchiveFileWriter,
        snapshots: SnapshotListing
    ):
        """Commit an updated dataset archive.

        The modified components of the archive are given as the three
        arguments of this method.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the new archive version.
        writer: histore.archive.store.fs.writer.ArchiveFileWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the new archive version.
        snapshots: histore.archive.snapshot.SnapshotListing
            Snapshot listing for the modified archive.
        """
        # Materialize the modified archive.
        self._write(schema=schema, writer=writer, snapshots=snapshots)
        # Update the cached objects
        self.schema = schema
        self.snapshots = snapshots
        self.row_counter = writer.row_counter

    def is_empty(self) -> bool:
        """True if the archive does not contain any snapshots yet.

        Returns
        -------
        bool
        """
        return self.snapshots.is_empty()

    def get_reader(self) -> ArchiveFileReader:
        """Get the row reader for this archive.

        Returns
        -------
        histore.archive.store.mem.BufferedReader
        """
        return ArchiveFileReader(
            filename=self.datafile,
            serializer=self.serializer,
            compression=self.compression,
            decoder=self.decoder
        )

    def get_schema(self) -> ArchiveSchema:
        """Get the schema history for the archived dataset.

        Returns
        -------
        histore.archive.schema.ArchiveSchema
        """
        return self.schema

    def get_snapshots(self) -> SnapshotListing:
        """Get listing of all snapshots in the archive.

        Returns
        -------
        histore.archive.snapshot.SnapshotListing
        """
        return self.snapshots

    def get_writer(self, validate: Optional[bool] = None) -> ArchiveFileWriter:
        """Get a a new archive buffer to maintain rows for a new archive
        version.

        Returns
        -------
        histore.archive.store.mem.ArchiveBuffer
        """
        # Use a temporary file in the base directory to write the new archive
        # before commit.
        return ArchiveFileWriter(
            filename=self.tmpdatafile,
            row_counter=self.row_counter,
            serializer=self.serializer,
            compression=self.compression,
            encoder=self.encoder
        )

    def primary_key(self) -> List[int]:
        """Get the list of identifier for the primary key column(s).

        Returns None if the archive is not keyed by a primary key.

        Returns
        -------
        list of int
        """
        return self._primary_key

    def rollback(self, schema: ArchiveSchema, writer: ArchiveFileWriter, version: int):
        """Store the archive after it has been rolled back to a previous
        version.

        The archive schema and archive writer contain the modified schema and
        rows after the rollback.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the previous archive version.
        writer: histore.archive.store.fs.writer.ArchiveFileWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the previous archive version.
        version: int
            Unique version identifier for the rollback snapshot.
        """
        # Get an updated shapshot listing.
        snapshots = self.snapshots.rollback(version=version)
        # Materialize the modified archive.
        self._write(schema=schema, writer=writer, snapshots=snapshots)
        # Update the cached objects
        self.schema = schema
        self.snapshots = snapshots

    def _write(self, schema, writer, snapshots):
        """Materialize the modified archive.

        Parameters
        ----------
        schema: histore.archive.schema.ArchiveSchema
            Schema history for the previous archive version.
        writer: histore.archive.store.fs.writer.ArchiveFileWriter
            Instance of the archive writer class returned by this store that
            was used to output the rows of the previous archive version.
        snapshots: histore.archive.snapshot.SnapshotListing
            Modified snapshot listing.
        """
        encoder = self.serializer
        doc = {
            META_SCHEMA: [encoder.serialize_column(c) for c in schema],
            META_SNAPSHOTS: [encoder.serialize_snapshot(s) for s in snapshots],
            META_ROWCOUNT: writer.row_counter,
            META_PRIMARYKEY: self._primary_key
        }
        with open(self.tmpmetafile, 'w') as f:
            json.dump(doc, f, cls=self.encoder)
        # Close the archive writer.
        writer.close()
        # Replace existing files with temporary files for new archive version.
        # This is the point of no return.
        # TODO: Instead of moving files we could delete (or keep) previous
        # files as backup.
        shutil.move(src=self.tmpmetafile, dst=self.metafile)
        shutil.move(src=self.tmpdatafile, dst=self.datafile)
