# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the command-line archive commands."""

import os

from click.testing import CliRunner

from histore.cli.base import cli


def test_create_archive(tmpdir):
    """Test creating a new archive."""
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    runner.invoke(cli, ['init', '--basedir', basedir])
    result = runner.invoke(cli, ['create', '--basedir', basedir, 'myarchive'])
    assert result.exit_code == 0
    assert result.output == 'Archive created!\n'
    result = runner.invoke(cli, ['create', '--basedir', basedir, 'myarchive'])
    assert result.exit_code == -1
    assert result.output == "archive 'myarchive' already exists\n"


def test_delete_archive(tmpdir):
    """Test deleting an existing archive."""
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    runner.invoke(cli, ['init', '--basedir', basedir])
    runner.invoke(cli, ['create', '--basedir', basedir, 'a_archive'])
    runner.invoke(cli, ['create', '--basedir', basedir, 'myarchive'])
    # Abort deletion.
    result = runner.invoke(
        cli,
        ['delete', '--basedir', basedir, 'myarchive'],
        input='n'
    )
    assert result.exit_code == 1
    assert 'Aborted!\n' in result.output
    result = runner.invoke(
        cli,
        ['delete', '--basedir', basedir, 'myarchive'],
        input='Y'
    )
    assert result.exit_code == 0
    assert "Archive 'myarchive' deleted!\n" in result.output
    # Delete with force option
    result = runner.invoke(
        cli,
        ['delete', '--basedir', basedir, '--force', 'a_archive']
    )
    assert result.exit_code == 0
    assert "Archive 'a_archive' deleted!\n" in result.output
    result = runner.invoke(
        cli,
        ['delete', '--basedir', basedir, '--force', 'a_archive']
    )
    assert result.exit_code == -1
    assert result.output == "Unknown archive 'a_archive'.\n"


def test_list_archives(tmpdir):
    """Test listing an existing archive."""
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    runner.invoke(cli, ['init', '--basedir', basedir])
    runner.invoke(cli, ['create', '--basedir', basedir, 'archive B'])
    runner.invoke(cli, ['create', '--basedir', basedir, 'archive A'])
    # Order by name
    result = runner.invoke(cli, ['list', '--basedir', basedir])
    assert result.exit_code == 0
    lines = result.output.split('\n')
    assert lines[3].startswith('archive A')
    assert lines[4].startswith('archive B')
    # Order by created_at timestamp
    result = runner.invoke(cli, ['list', '--basedir', basedir, '-d'])
    assert result.exit_code == 0
    lines = result.output.split('\n')
    assert lines[3].startswith('archive B')
    assert lines[4].startswith('archive A')


def test_rename_archive(tmpdir):
    """Test renaming an existing archive."""
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    runner.invoke(cli, ['init', '--basedir', basedir])
    runner.invoke(cli, ['create', '--basedir', basedir, 'archive B'])
    runner.invoke(cli, ['create', '--basedir', basedir, 'archive A'])
    # Rename archive B to C
    result = runner.invoke(
        cli,
        ['rename', '--basedir', basedir, 'archive B', 'archive C']
    )
    assert result.exit_code == 0
    result = runner.invoke(cli, ['list', '--basedir', basedir])
    lines = result.output.split('\n')
    assert lines[3].startswith('archive A')
    assert lines[4].startswith('archive C')
    # Error cases
    result = runner.invoke(
        cli,
        ['rename', '--basedir', basedir, 'archive B', 'archive C']
    )
    assert result.exit_code == -1
    assert result.output == "Unknown archive 'archive B'\n"
    result = runner.invoke(
        cli,
        ['rename', '--basedir', basedir, 'archive C', 'archive A']
    )
    assert result.exit_code == -1
    assert result.output == "archive 'archive A' already exists\n"
