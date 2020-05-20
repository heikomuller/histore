# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Helper methods to access configuration parameters. Configuration parameters
are maintained in environment variables.
"""

from pathlib import Path

import os


"""Environment variables."""
# Base directory for all files that are created by histore components.
ENV_HISTORE_BASEDIR = 'HISTORE_BASEDIR'


def BASEDIR():
    """Get value for environment variable HISTORE_BASEDIR. The default value
    if the variable is not set is $HOME/.histore.

    Returns
    -------
    string
    """
    value = os.environ.get(ENV_HISTORE_BASEDIR)
    if value is None or value == '':  # pragma: no cover
        # Use the default value if the environment variable is not set.
        value = os.path.join(str(Path.home()), '.histore')
    return value
