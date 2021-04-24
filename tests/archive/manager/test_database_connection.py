# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the database connector."""

from sqlalchemy.exc import IntegrityError

import pytest

from histore.archive.manager.db.database import DB, TEST_URL
from histore.archive.manager.db.model import Archive


@pytest.mark.parametrize('web_app', [True, False])
def test_database_sessions(web_app):
    """Test creating instances of the database and database sessions."""
    db = DB(connect_url=TEST_URL, web_app=web_app, echo=web_app)
    db.init()
    with db.session() as session:
        archive = Archive(archive_id='0000', name='test')
        session.add(archive)
    with pytest.raises(IntegrityError):
        with db.session() as session:
            archive = Archive(archive_id='0000', name='test')
            session.add(archive)
    with pytest.raises(ValueError):
        with db.session() as session:
            archive = Archive(archive_id='0001', name='test')
            session.add(archive)
            raise ValueError('test')
    db.close()
