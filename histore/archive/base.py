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

    def commit(
        self, df, match_by_name=False, renamed=None, renamed_to=True,
        partial=False, origin=None
    ):
        """
        Parameters
        ----------
        df: pandas.DataFrame
            Data frame representing the dataset snapshot that is being merged
            into the archive.
        match_by_name: bool, default=False
            Match columns from the given snapshot to columns in the origin
            schema by name instead of matching columns by identifier against
            the columns in the archive schema.
        renamed: dict, default=None
            Optional mapping of columns that have been renamed. Maps the new
            column name to the original name.
        renamed_to: bool, default=True
            Flag that determines the semantics of the mapping in the renamed
            dictionary. By default a mapping from the original column name
            (i.e., the dictionary key) to the new column name (the dictionary
            value) is assumed. If the flag is False a mapping from the new
            column name to the original column name is assumed.
        partial: bool, default=False
            If True the given snapshot is assumed to be partial. All columns
            from the snapshot schema that is specified by origin that are not
            matched by any column in the snapshot schema are assumed to be
            unchanged. All rows from the orignal snapshot that are not in the
            given snapshot are also assumed to be unchnged.
        origin: int, default=None
            Version identifier of the original column against which the given
            column list is matched.
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
