# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Iterator for document rows. Provides a reader that allows to iterate over
the rows in a data frame sorted by the row key.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Tuple, Union

from histore.document.base import DocumentIterator, RowIndex
from histore.document.row import DocumentRow
from histore.key import KeyValue, NewRow, to_key


class DocumentReader(metaclass=ABCMeta):
    """Reader for rows in a document. Reads rows in order defined by the row
    key that is used for merging the document.
    """
    def __init__(self, iterator: DocumentIterator, columns: List[int]):
        """Initialize the document row iterator and the identifiers for the
        document columns.

        Parameters
        ----------
        iterator: histore.document.base.DocumentIterator
            Iterator for rows in the document.
        columns: list of int
            List of identifier for columns in the document schema.
        """
        self.iterator = iterator
        self.columns = columns

    @abstractmethod
    def annotate(self, rowidx: RowIndex, values: Dict) -> Union[KeyValue, Tuple[KeyValue, ...]]:
        """Get key value for a document row.

        The key value may either be generated from the row index or from the
        cell values.

        Parameters
        ----------
        rowidx: histore.document.base.RowIndex
            Row index value. May be a scalar value or a tuple of scalars.
        values: dict
            Mapping of column identifier to cell values.

        Returns
        -------
        histore.key.KeyValue or tuple of histore.key.KeyValue
        """
        raise NotImplementedError()  # pragma: no cover

    def close(self):
        """Close the associated document iterator."""
        self.iterator.close()

    def next(self) -> DocumentRow:
        """Read the next row in the document.

        Returns None if the end of the document has been reached.

        Returns
        -------
        histore.document.row.DocumentRow
        """
        try:
            rowpos, rowidx, values = next(self.iterator)
        except StopIteration:
            return None
        values = {colid: val for colid, val in zip(self.columns, values)}
        key = self.annotate(rowidx, values)
        return DocumentRow(pos=rowpos, key=key, values=values)


class AnnotatedDocumentReader(DocumentReader):
    """The annotated document reader uses the values from one or more document
    columns to generate keys for document rows.
    """
    def __init__(self, iterator: DocumentIterator, columns: List[int], keys: List[int]):
        """Initialize the document row iterator, the identifiers for the
        document columns, and the identifier for the key columns.

        Parameters
        ----------
        iterator: histore.document.base.DocumentIterator
            Iterator for rows in the document.
        columns: list of int
            List of identifier for columns in the document schema.
        keys: list of int
            List of identifier for document key columns.
        """
        super(AnnotatedDocumentReader, self).__init__(iterator=iterator, columns=columns)
        self.keys = keys

    def annotate(self, rowidx: RowIndex, values: Dict) -> Union[KeyValue, Tuple[KeyValue, ...]]:
        """Get key value for a document row.

        The default document reader uses the row index as the row key..

        Parameters
        ----------
        rowidx: histore.document.base.RowIndex
            Row index value. May be a scalar value or a tuple of scalars.
        values: dict
            Mapping of column identifier to cell values.

        Returns
        -------
        histore.key.KeyValue or tuple of histore.key.KeyValue
        """
        if len(self.keys) == 1:
            return to_key(values[self.keys[0]])
        return tuple([to_key(values[c]) for c in self.keys])


class DefaultDocumentReader(DocumentReader):
    """The default document reader uses the row index as the row key."""
    def __init__(self, iterator: DocumentIterator, columns: List[int]):
        """Initialize the document row iterator and the identifiers for the
        document columns.

        Parameters
        ----------
        iterator: histore.document.base.DocumentIterator
            Iterator for rows in the document.
        columns: list of int
            List of identifier for columns in the document schema.
        """
        super(DefaultDocumentReader, self).__init__(iterator=iterator, columns=columns)

    def annotate(self, rowidx: RowIndex, values: Dict) -> Union[KeyValue, Tuple[KeyValue, ...]]:
        """Get key value for a document row.

        The default document reader uses the row index as the row key..

        Parameters
        ----------
        rowidx: histore.document.base.RowIndex
            Row index value. May be a scalar value or a tuple of scalars.
        values: dict
            Mapping of column identifier to cell values.

        Returns
        -------
        histore.key.KeyValue or tuple of histore.key.KeyValue
        """
        if isinstance(rowidx, tuple) or isinstance(rowidx, list):
            return tuple([to_key(v) for v in rowidx])
        return to_key(rowidx if rowidx != -1 else NewRow())
