# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Command line interface to interact with a manager for archives on the local
file system.
"""

import csv
import click
import pandas as pd
import sys

from histore.cli.archive import get_manager

import histore.util as util


# -- Create a new archive -----------------------------------------------------

@click.command(name='commit')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
@click.option(
    '-c', '--comment',
    required=False,
    type=str,
    help='Optional snapshot description'
)
@click.option(
    '-d', '--delimiter',
    required=False,
    type=str,
    help='One-character string used to separate fields'
)
@click.option(
    '-q', '--quotechar',
    required=False,
    type=str,
    help='One-character string used to quote fields with special characters'
)
@click.option(
    '-z', '--gzip',
    is_flag=True,
    default=False,
    help='Gzip compressed'
)
@click.argument(
    'archive',
    type=str
)
@click.argument(
    'filename',
    type=click.Path(file_okay=True, dir_okay=False, exists=True)
)
def commit_snapshot(
    basedir, comment, delimiter, quotechar, gzip, archive, filename
):
    """Commit file to archive."""
    store = get_archive(basedir, archive)
    if store is None:
        click.echo("Unknown archive '{}'".format(archive))
        sys.exit(-1)
    # Read the data frame.
    df = pd.read_csv(
        filename,
        delimiter=get_delimiter(delimiter),
        quotechar=quotechar if quotechar is not None else '"',
        quoting=csv.QUOTE_MINIMAL,
        compression='gzip' if gzip else None
    )
    s = store.commit(df, description=comment)
    click.echo('Snapshot {} created.'.format(s.version))


# -- Checkout snapshot --------------------------------------------------------

@click.command(name='checkout')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
@click.option(
    '-d', '--delimiter',
    required=False,
    type=str,
    help='One-character string used to separate fields'
)
@click.option(
    '-q', '--quotechar',
    required=False,
    type=str,
    help='One-character string used to quote fields with special characters'
)
@click.option(
    '-z', '--gzip',
    is_flag=True,
    default=False,
    help='Gzip compressed'
)
@click.option(
    '-v', '--version',
    required=False,
    type=int,
    help='Snapshot version'
)
@click.argument(
    'archive',
    type=str
)
@click.argument(
    'filename',
    type=click.Path(file_okay=True, dir_okay=False)
)
def checkout_snapshot(
    basedir, delimiter, quotechar, gzip, version, archive, filename
):
    """Write snapshot to file."""
    store = get_archive(basedir, archive)
    if store is None:
        click.echo("Unknown archive '{}'".format(archive))
        sys.exit(-1)
    # Read the snapshot data frame.
    df = store.checkout(version=version)
    df.to_csv(
        filename,
        sep=get_delimiter(delimiter),
        quotechar=quotechar if quotechar is not None else '"',
        quoting=csv.QUOTE_MINIMAL,
        compression='gzip' if gzip else None,
        index=False
    )


# -- List snapshots -----------------------------------------------------------

@click.command(name='snapshots')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
@click.argument(
    'archive',
    type=str
)
def list_snapshots(basedir, archive):
    """List snapshots."""
    store = get_archive(basedir, archive)
    if store is None:
        click.echo("Unknown archive '{}'".format(archive))
        sys.exit(-1)
    for s in store.snapshots():
        click.echo('{}\t{}\t{}'.format(
            s.version,
            util.to_localtime(s.created_at).strftime('%Y-%m-%d %H:%M:%S'),
            s.description
        ))


# -- Helper Function ----------------------------------------------------------

def get_archive(basedir, name):
    """Get handle for archive with given name. Returns None if no archive with
    the given name exists.

    Parameters
    ----------
    basedir: string
        Base directory for the archive manager.
    name: string
        Unique archive name.

    Returns
    -------
    histore.archive.base.archive
    """
    manager = get_manager(basedir)
    descriptor = manager.get_by_name(name)
    if descriptor is None:
        return None
    return manager.get(descriptor.identifier())


def get_delimiter(delimiter):
    """Get delimiter. Replace encodings for tabulator with tab character.

    Parameters
    ----------
    delimiter: string
        One-character used to separate fields.

    Returns
    -------
    string
    """
    if delimiter is None:
        return ','
    if delimiter.lower() in ['tab', '\\t']:
        return '\t'
    return delimiter
