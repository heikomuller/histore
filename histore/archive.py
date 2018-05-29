# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Archive."""

class ArchiveNode(object):
    def __init__(self, parent=None, timestamp=None):
        if parent is None and timestamp is None:
            raise ValueError('invalid arguments')
        self.parent = parent
        self.timestamp = timestamp

    def get_timestamp(self):
        if not self.timestamp is None:
            return self.timestamp
        else:
            return self.parent.get_timestamp()

    def has_timestamp(self):
        return not self.timestamp is None

    def set_timestamp(self, timestamp, validate=False):
        if validate:
            if not timestamp.is_subset_of(parent.get_timestamp()):
                raise ValueError()
        self.timestamp = timestamp
