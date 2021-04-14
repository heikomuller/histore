# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Classes to maintain information about dataset snapshot documents."""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional


@dataclass
class InputDescriptor:
    """Descriptor for archive snapshot input documents."""
    # Timestamp when the snapshot was first valid.
    valid_time: Optional[datetime] = None
    # Optional user-provided description for the snapshot.
    description: Optional[str] = ''
    # Optional metadata defining the action that created the snapshot.
    action: Optional[Dict] = None
