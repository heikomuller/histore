# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the command-line archive commands."""

import os
import pytest

from click.testing import CliRunner

from histore.cli.base import cli

import histore.config as config


# -- Fixtures and Helper Functions --------------------------------------------

@pytest.fixture
def test_runner(tmpdir):
    """Initialize archive manager in temporary directory and set basedir
    environment variable.
    """
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    os.environ[config.ENV_HISTORE_BASEDIR] = basedir
    runner = CliRunner()
    runner.invoke(cli, ['init'])
    return runner


def cleanup():
    """Clear base directory environment variable."""
    del os.environ[config.ENV_HISTORE_BASEDIR]


# -- Unit tests ---------------------------------------------------------------

def test_create_archive(test_runner):
    """Test creating a new archive."""
    result = test_runner.invoke(cli, ['create', 'myarchive'])
    assert result.exit_code == 0
    assert result.output == 'Archive created!\n'
    result = test_runner.invoke(cli, ['create', 'myarchive'])
    assert result.exit_code == -1
    assert result.output == "archive 'myarchive' already exists\n"
    # Cleanup environment
    cleanup()


def test_delete_archive(test_runner):
    """Test deleting an existing archive."""
    test_runner.invoke(cli, ['create', 'a_archive'])
    test_runner.invoke(cli, ['create', 'myarchive'])
    # Abort deletion.
    result = test_runner.invoke(cli, ['delete', 'myarchive'], input='n')
    assert result.exit_code == 1
    assert 'Aborted!\n' in result.output
    result = test_runner.invoke(cli, ['delete', 'myarchive'], input='Y')
    assert result.exit_code == 0
    assert "Archive 'myarchive' deleted!\n" in result.output
    # Delete with force option
    result = test_runner.invoke(cli, ['delete', '--force', 'a_archive'])
    assert result.exit_code == 0
    assert "Archive 'a_archive' deleted!\n" in result.output
    result = test_runner.invoke(cli, ['delete', '--force', 'a_archive'])
    assert result.exit_code == -1
    assert result.output == "Unknown archive 'a_archive'.\n"
    # Cleanup environment
    cleanup()


def test_list_archives(test_runner):
    """Test listing an existing archive."""
    test_runner.invoke(cli, ['create', 'archive B'])
    test_runner.invoke(cli, ['create', 'archive A'])
    # Order by name
    result = test_runner.invoke(cli, ['list'])
    assert result.exit_code == 0
    lines = result.output.split('\n')
    assert lines[3].startswith('archive A')
    assert lines[4].startswith('archive B')
    # Order by created_at timestamp
    result = test_runner.invoke(cli, ['list', '-d'])
    assert result.exit_code == 0
    lines = result.output.split('\n')
    assert lines[3].startswith('archive B')
    assert lines[4].startswith('archive A')
    # Cleanup environment
    cleanup()


def test_rename_archive(test_runner):
    """Test renaming an existing archive."""
    test_runner.invoke(cli, ['create', 'archive B'])
    test_runner.invoke(cli, ['create', 'archive A'])
    # Rename archive B to C
    result = test_runner.invoke(cli, ['rename', 'archive B', 'archive C'])
    assert result.exit_code == 0
    result = test_runner.invoke(cli, ['list'])
    lines = result.output.split('\n')
    assert lines[3].startswith('archive A')
    assert lines[4].startswith('archive C')
    # Error cases
    result = test_runner.invoke(cli, ['rename', 'archive B', 'archive C'])
    assert result.exit_code == -1
    assert result.output == "Unknown archive 'archive B'\n"
    result = test_runner.invoke(cli, ['rename', 'archive C', 'archive A'])
    assert result.exit_code == -1
    assert result.output == "archive 'archive A' already exists\n"
    # Cleanup environment
    cleanup()
