# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Archives are collections of snapshots of an evolving dataset."""


class Archive(object):
    """An archive maintains a list of snapshot handles. All snapshots are
    expected to follow the same document schema.

    Archives are represented as trees. The root of the tree is maintained by
    an archive store. The store provides a layer of abstraction such that the
    archive object does not have to deal with the different ways in which
    archives are managed by different systems.
    """
    def checkout(self, version):
        """
        """
        raise NotImplementedError()

    def commit(self, df, match_by_name=False, partial=False, origin=None):
        """
        """
        raise NotImplementedError()

    def is_empty(self):
        """Returns True if the archive is empty. That is, no database snapshot
        has been commited to the archive yet.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    def schema(self):
        """
        """
        raise NotImplementedError()

    def snapshots(self):
        """
        """
        raise NotImplementedError()
