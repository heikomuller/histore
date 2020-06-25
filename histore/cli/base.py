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
import os
import sys

from histore.archive.manager.fs import PersistentArchiveManager
from histore.cli.archive import (
    create_archive, delete_archive, list_archives, rename_archive
)
from histore.cli.snapshot import (
    checkout_snapshot, commit_snapshot, list_snapshots
)

import histore.config as config


# -- Init the archive manager -------------------------------------------------

@click.command(name='init')
@click.option(
    '-b', '--basedir',
    required=False,
    type=click.Path(file_okay=False, dir_okay=True),
    help='Base directory for archive files'
)
def init_manager(basedir):
    """Initialize the archive manager directory."""
    # Test if the base directory exists and is empty.
    basedir = basedir if basedir is not None else config.BASEDIR()
    if os.path.isdir(basedir):
        if os.listdir(basedir):
            click.echo('Not an empty directory {}.'.format(basedir))
            sys.exit(0)
    # Create instance of persistent archive manager to setup directories and
    # files.
    PersistentArchiveManager(basedir=basedir, exists=False)


# -- Create command group -----------------------------------------------------

@click.group()
def cli():
    """Command line interface for HISTORE archive manager."""
    pass


cli.add_command(init_manager)
cli.add_command(checkout_snapshot)
cli.add_command(commit_snapshot)
cli.add_command(create_archive)
cli.add_command(delete_archive)
cli.add_command(list_archives)
cli.add_command(list_snapshots)
cli.add_command(rename_archive)
