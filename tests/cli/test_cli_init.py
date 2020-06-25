# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Unit tests for the command-line init command."""

import os

from click.testing import CliRunner
from pathlib import Path

from histore.cli.base import init_manager


def test_init_manager(tmpdir):
    """Test initializing the archive manager directory."""
    runner = CliRunner()
    basedir = os.path.abspath(str(tmpdir))
    result = runner.invoke(init_manager, ['--basedir', basedir])
    assert result.exit_code == 0
    assert result.output == 'Initialized in {}.\n'.format(basedir)
    # Create file in temp dir to simulate non-empty directory.
    Path(os.path.join(basedir, 'myfile.txt')).touch()
    result = runner.invoke(init_manager, ['--basedir', basedir])
    assert result.exit_code == -1
    # Create archive manager for non-existing folder
    subdir = os.path.join(basedir, 'subfolder')
    result = runner.invoke(init_manager, ['--basedir', subdir])
    assert result.exit_code == 0
    assert result.output == 'Initialized in {}.\n'.format(subdir)
