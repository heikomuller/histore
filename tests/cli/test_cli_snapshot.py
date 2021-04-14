# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the command-line snapshot commands."""

import os

from click.testing import CliRunner

from histore.cli.base import cli


DIR = os.path.dirname(os.path.realpath(__file__))
WATERSHED_1 = os.path.join(DIR, '../.files/y43c-5n92.tsv.gz')


def test_snapshot_commit_keyed(tmpdir):
    """Test committing, listing and checking out snapshots."""
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    runner.invoke(cli, ['init', '--basedir', basedir])
    runner.invoke(
        cli,
        [
            'create',
            '--basedir',
            basedir,
            '--pk',
            'Site,Date',
            '-f',
            WATERSHED_1,
            '-d',
            '\\t',
            '-z',
            'myarchive'
        ]
    )
    # Unknown archive
    result = runner.invoke(
        cli,
        ['commit', '--basedir', basedir, 'myarch', WATERSHED_1]
    )
    assert result.exit_code == -1
    # List snapshots.
    result = runner.invoke(
        cli,
        ['snapshots', '--basedir', basedir, 'myarchive']
    )
    print(result.output)
    assert result.exit_code == 0
    assert result.output.startswith('0\t')
    # Unknown archive
    result = runner.invoke(cli, ['snapshots', '--basedir', basedir, 'myarch'])
    assert result.exit_code == -1
    # Checkout snapshot
    outfile = os.path.join(basedir, 'outfile.csv')
    result = runner.invoke(
        cli,
        ['checkout', '--basedir', basedir, 'myarchive', outfile]
    )
    assert result.exit_code == 0
    # Unknown archive
    result = runner.invoke(
        cli,
        ['checkout', '--basedir', basedir, 'myarch', outfile]
    )
    assert result.exit_code == -1


def test_snapshot_commit_unkeyed(tmpdir):
    """Test committing, listing and checking out snapshots."""
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    runner.invoke(cli, ['init', '--basedir', basedir])
    runner.invoke(
        cli,
        ['create', '--basedir', basedir, 'myarchive']
    )
    # Commit a snapshot.
    result = runner.invoke(
        cli,
        [
            'commit',
            '--basedir',
            basedir,
            '-d',
            '\\t',
            '-z',
            'myarchive',
            WATERSHED_1
        ]
    )
    assert result.exit_code == 0
    assert result.output == 'Snapshot 0 created.\n'
    # Unknown archive
    result = runner.invoke(
        cli,
        ['commit', '--basedir', basedir, 'myarch', WATERSHED_1]
    )
    assert result.exit_code == -1
    # List snapshots.
    result = runner.invoke(
        cli,
        ['snapshots', '--basedir', basedir, 'myarchive']
    )
    assert result.exit_code == 0
    assert result.output.startswith('0\t')
    # Unknown archive
    result = runner.invoke(cli, ['snapshots', '--basedir', basedir, 'myarch'])
    assert result.exit_code == -1
    # Checkout snapshot
    outfile = os.path.join(basedir, 'outfile.csv')
    result = runner.invoke(
        cli,
        ['checkout', '--basedir', basedir, 'myarchive', outfile]
    )
    assert result.exit_code == 0
    # Unknown archive
    result = runner.invoke(
        cli,
        ['checkout', '--basedir', basedir, 'myarch', outfile]
    )
    assert result.exit_code == -1
