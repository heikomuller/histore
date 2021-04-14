# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

from sqlalchemy import Integer, String, Text
from sqlalchemy import Column, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator, Unicode

import json

from histore.archive.manager.descriptor import ArchiveDescriptor

import histore.util as util


# -- ORM Model ----------------------------------------------------------------

"""Base class for all database tables."""

Base = declarative_base()


class JsonObject(TypeDecorator):
    """Decorator for objects that are stored as serialized JSON strings."""

    impl = Unicode

    def process_literal_param(self, value, dialect):
        """Expects a JSON serializable object."""
        if value is not None:
            return json.dumps(value)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Create JSON object from string serialization."""
        if value is not None:
            return json.loads(value)


class Archive(Base):
    """ORM for maintaining archive descriptors in a relational database."""
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'archive'

    archive_id = Column(
        String(32),
        default=util.get_unique_identifier,
        primary_key=True
    )
    name = Column(String(1024), nullable=False, unique=True)
    created_at = Column(String(32), nullable=False, default=util.current_time)
    description = Column(Text)
    encoder = Column(String(1024))
    decoder = Column(String(1024))
    serializer = Column(JsonObject)

    # -- Relationships --------------------------------------------------------
    keyspec = relationship(
        'ArchiveKey',
        back_populates='archive',
        cascade='all, delete, delete-orphan'
    )

    def descriptor(self) -> ArchiveDescriptor:
        """Get descriptor for the archive database object.

        Returns
        -------
        histore.archive.manager.descriptor.ArchiveDescriptor
        """
        # Create list of promary key columns (if defined).
        pk = [k.colid for k in sorted(self.keyspec, key=lambda x: x.pos)] if self.keyspec else None
        return ArchiveDescriptor.create(
            identifier=self.archive_id,
            name=self.name,
            description=self.description,
            primary_key=pk,
            encoder=self.encoder,
            decoder=self.decoder,
            serializer=self.serializer
        )


class ArchiveKey(Base):
    """Column in the primary key definition for an archive. Each element in the
    primary key has a unique (column) name and the key position.
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'archive_key'

    archive_id = Column(
        String(32),
        ForeignKey('archive.archive_id'),
        primary_key=True
    )
    colid = Column(Integer, primary_key=True)
    pos = Column(Integer, primary_key=True)

    # -- Relationships --------------------------------------------------------
    archive = relationship('Archive', back_populates='keyspec')
