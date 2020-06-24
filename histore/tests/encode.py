# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Test encoders and decoders for Json objects."""

import json

from datetime import datetime


class TestEncoder(json.JSONEncoder):
    """Json encoder that handles datetime objects."""
    def default(self, obj):
        """Convert datatime to dictionary."""
        if isinstance(obj, datetime):
            return {'$dt': obj.isoformat()}
        return obj.value  # Assumes histore.key.base.KeyValue


def test_decoder(obj):
    """Decode objects generated by the TestEncoder. Returns datetime object
    as string instead of datetime instances.
    """
    if '$dt' in obj:
        return obj['$dt']
    return obj
