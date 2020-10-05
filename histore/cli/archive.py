# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Command line interface to interact with a manager for archives on the local
file system.
"""

import click
import sys

from typing import Optional

from histore import PersistentArchiveManager
from histore.archive.manager.base import ArchiveManager

import histore.config as config
import histore.util as util


"""Datetime format string."""
DTF = '%Y-%m-%d %H:%M:%S'


# -- Create a new archive -----------------------------------------------------

@click.command(name='create')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
@click.option(
    '-c', '--dbconnect',
    required=False,
    help='Connect URL for the database'
)
@click.option(
    '-k', '--pk',
    required=False,
    type=str,
    help='Comma-separate list of primary key columns'
)
@click.option(
    '-t', '--comment',
    required=False,
    type=str,
    help='Optional archive description'
)
@click.option(
    '-e', '--encoder',
    required=False,
    type=str,
    help='JSONEncoder class for the new archive'
)
@click.option(
    '-d', '--decoder',
    required=False,
    type=str,
    help='JSON decoder function for the new archive'
)
@click.argument('name')
def create_archive(basedir, dbconnect, pk, comment, encoder, decoder, name):
    """Create a new archive."""
    manager = get_manager(basedir, dbconnect=dbconnect)
    # Split primary key if it contains ','.
    primary_key = pk.split(',') if pk is not None else None
    try:
        manager.create(
            name=name,
            description=comment,
            primary_key=primary_key,
            encoder=encoder,
            decoder=decoder
        )
        click.echo('Archive created!')
    except ValueError as ex:
        click.echo('{}'.format(ex))
        sys.exit(-1)


# -- Delete archive -----------------------------------------------------------

@click.command(name='delete')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
@click.option(
    '-c', '--dbconnect',
    required=False,
    help='Connect URL for the database'
)
@click.option(
    '-f', '--force',
    is_flag=True,
    default=False,
    help='Delete without confirmation'
)
@click.argument('name')
def delete_archive(basedir, dbconnect, force, name):
    """Delete existing archive."""
    manager = get_manager(basedir, dbconnect=dbconnect)
    for archive in manager.list():
        if archive.name() == name:
            if not force:
                msg = "This will remove archive '{}' permanently"
                click.echo(msg.format(name))
                click.confirm('Continue?', default=True, abort=True)
            manager.delete(archive.identifier())
            click.echo("Archive '{}' deleted!".format(name))
            return
    click.echo("Unknown archive '{}'.".format(name))
    sys.exit(-1)


# -- List archives ------------------------------------------------------------

@click.command(name='list')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
@click.option(
    '-c', '--dbconnect',
    required=False,
    help='Connect URL for the database'
)
@click.option(
    '-d', '--bydate',
    is_flag=True,
    default=False,
    help='Sort by creation date'
)
def list_archives(basedir, dbconnect, bydate):
    """List names of existing archives."""
    manager = get_manager(basedir, dbconnect=dbconnect)
    archives = manager.list()
    if bydate:
        archives = sorted(archives, key=lambda a: a.created_at())
    else:
        archives = sorted(archives, key=lambda a: a.name())
    click.echo()
    click.echo('Archives')
    click.echo('--------')
    for archive in archives:
        ts = util.to_localtime(archive.created_at())
        click.echo('{} (created at {})'.format(
            archive.name(),
            ts.strftime('%Y-%m-%d %H:%M:%S')
        ))
    click.echo()


# -- Rename archive -----------------------------------------------------------

@click.command(name='rename')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
@click.option(
    '-c', '--dbconnect',
    required=False,
    help='Connect URL for the database'
)
@click.argument('oldname')
@click.argument('newname')
def rename_archive(basedir, dbconnect, oldname, newname):
    """Rename existing archive."""
    manager = get_manager(basedir, dbconnect=dbconnect)
    # Get archive with the old name
    archive = manager.get_by_name(oldname)
    if archive is None:
        click.echo("Unknown archive '{}'".format(oldname))
        sys.exit(-1)
    try:
        manager.rename(archive.identifier(), newname)
    except ValueError as ex:
        click.echo('{}'.format(ex))
        sys.exit(-1)


# -- Helper Functions ---------------------------------------------------------

def get_manager(
    basedir: str, dbconnect: Optional[str] = None
) -> ArchiveManager:
    """Create instance of persistent archive manager assuming that it has been
    initialized before.

    Parameters
    ----------
    basedir: string
        Base directory for the archive manager.
    dbconnect: string, default=None
        Database connect string.

    Returns
    -------
    histore.archive.manager.base.ArchiveManager
    """
    basedir = basedir if basedir is not None else config.BASEDIR()
    return PersistentArchiveManager(
        basedir=basedir,
        dbconnect=dbconnect,
        create=False
    )
