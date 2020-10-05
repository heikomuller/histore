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
# Database connection URL for archive manager.
ENV_HISTORE_DBCONNECT = 'HISTORE_DBCONNECT'
# Size of the sort buffer for CSV files that are sorted using external memory.
ENV_HISTORE_SORTBUFFER = 'HISTORE_SORTBUFFER'


def BASEDIR() -> str:
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


def DBCONNECT() -> str:
    """Get value for environment variable HISTORE_DBCONNECT. The default value
    for the database connector is not defined (None).

    Returns
    -------
    string
    """
    return os.environ.get(ENV_HISTORE_DBCONNECT)


def SORTBUFFER() -> float:
    """Get value for environment variable HISTORE_SORTBUFFER. The default value
    if the variable is not set is 50% of the available main-memory.

    Returns
    -------
    float
    """
    value = os.environ.get(ENV_HISTORE_SORTBUFFER)
    if value is None or value == '':
        # Use 50% of the available main-memory as buffer size. Ensure that the
        # buffer is at least 1MB.
        import psutil
        mem = psutil.virtual_memory()
        value = max(mem.available / (1024 * 1024 * 2), 1.0)
    else:
        value = float(value)
    return value
