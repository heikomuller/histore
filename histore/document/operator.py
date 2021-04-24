# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Interfaces for mixin class for dataset operators that allow to directly
process rows in a dataset snapshot to create a new snapshot.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, Optional

from histore.document.base import DataRow, InputDescriptor
from histore.document.schema import DocumentSchema


class DatasetOperator(InputDescriptor, metaclass=ABCMeta):
    """Abstract class for operators on rows in a dataset snapshots. The operator
    can be used to directly process rows in a dataset archive to create a new
    dataset snapshot.
    """
    def __init__(
        self, columns: DocumentSchema, description: Optional[str] = None,
        action: Optional[Dict] = None
    ):
        """Initialize the schema for rows that this operator will produce and
        the input descriptor.

        Parameters
        ----------
        columns: list of string
            Columns in the output schema of the operator.
        description: string, default=None
            Optional user-provided description for the snapshot that is created
            by this operator.
        action: dict, default=None
            Optional metadata defining the action that created the snapshot that
            is created by this operator.
        """
        super(DatasetOperator, self).__init__(
            description=description,
            action=action
        )
        self.columns = columns

    @abstractmethod
    def handle(self, rowid: int, row: DataRow) -> DataRow:
        """Evaluate the operator on the given row.

        Returns the processed row. If the result is None this signals that the
        given row should not be part of the collected result.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.

        Returns
        -------
        list
        """
        raise NotImplementedError()  # pragma: no cover
