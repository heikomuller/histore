# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

from abc import abstractmethod


class Archive(object):
    """
    """
    @abstractmethod
    def get(self, version):
        """
        """
        raise NotImplementedError

    @abstractmethod
    def insert(self, doc, name=None):
        """
        """
        raise NotImplementedError

    @abstractmethod
    def snapshots(self):
        """
        """
        raise NotImplementedError
