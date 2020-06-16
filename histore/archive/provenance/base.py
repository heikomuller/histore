# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Collections of provenance information for archive snapshots. The provenance
represents the difference between two dataset snapshots with respect to
inserts, deletions, and updates.
"""

from histore.archive.provenance.describe import DefaultDescribe


"""Identifier for the three different provenance operator types."""
PROV_DELETE = 'DELETE'
PROV_INSERT = 'INSERT'
PROV_UPDATE = 'UPDATE'


class ProvOp(object):
    """Base class for provenance operators that represent changes between two
    versions of the same object. There are three different types of operators:
    (i) INSERT, (ii) DELETE, and (iii) UPDATE.
    """
    def __init__(self, type, key):
        """Initialize the operator type and the object key. Raises a ValueError
        if an invalid type is given.

        Parameters
        ----------
        type: string, enum=[PROV_DELETE, PROV_INSERT, PROV_UPDATE]
            Provenance operator type identifier.
        key: scalar or tuple
            Unique object identifier. This is either a column identifier, a row
            identifier, or a row key.

        Raises
        ------
        ValueError
        """
        if type not in [PROV_DELETE, PROV_INSERT, PROV_UPDATE]:
            raise ValueError("invalid type '{}'".format(type))
        self._type = type
        self.key = key

    def is_delete(self):
        """True if the operator represents object deletion.

        Returns
        -------
        bool
        """
        return self._type == PROV_DELETE

    def is_insert(self):
        """True if the operator represents object insertion.

        Returns
        -------
        bool
        """
        return self._type == PROV_INSERT

    def is_update(self):
        """True if the operator represents object update.

        Returns
        -------
        bool
        """
        return self._type == PROV_UPDATE


class Provenance(object):
    """Provenance information for a set of identifiable objects. Provenance for
    each object is represented by one of three operators: insert, delete, or
    update. The provenance represents the difference for the object between two
    dataset snapshots.
    """
    def __init__(self, insert=None, delete=None, update=None, descriptor=None):
        """Initialize the lists that maintain the different types of provenance
        operators. The optional provenance descriptor is used to print a
        summary of the maintained information.

        Parameters
        ----------
        insert: list, default=None
            List containing provenance insert operators for objects that have
            been inserted.
        delete: list, default=None
            List containing provenance delete operators for objects that have
            been deleted.
        update: list, default=None
            List containing provenance update operators for objects that have
            been udated.
        descriptor: histore.archive.provenance.describe.ProvDescriptor,
                    default=None
            Descriptor object that is used to print a summary of the maintained
            provenance information.
        """
        self.prov = {
            PROV_DELETE: delete if delete is not None else list(),
            PROV_INSERT: insert if insert is not None else list(),
            PROV_UPDATE: update if update is not None else list()
        }
        self.descriptor = descriptor if descriptor else DefaultDescribe()

    def add(self, prov):
        """Add provenance element to the collection.

        Parameters
        ----------
        prov: histore.archive.base.ProvOp
            Provenance operator.
        """
        if prov.is_delete():
            self.delete().append(prov)
        elif prov.is_insert():
            self.insert().append(prov)
        else:
            self.update().append(prov)

    def delete(self):
        """List containing provenance delete operators for objects that have
        been deleted.

        Returns
        -------
        list
        """
        return self.prov[PROV_DELETE]

    def describe(self):
        """Print summary of the maintained provenance information."""
        self.descriptor.describe(self)

    def insert(self):
        """List containing provenance insert operators for objects that have
        been inserted.

        Returns
        -------
        list
        """
        return self.prov[PROV_INSERT]

    def update(self):
        """List containing provenance update operators for objects that have
        been udated.

        Returns
        -------
        list
        """
        return self.prov[PROV_UPDATE]
