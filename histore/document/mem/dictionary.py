# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""In-memory document created from a dictionary serialization of a document.
The dictionary serialization of a document contains the list of columns
('columns') and an array or arrays for document rows ('data').
"""

import jsonschema

from histore.document.mem.base import InMemoryDocument
from histore.document.schema import Column

import histore.key.annotate as anno


"""Json schema for dictionary document serializations."""

COLUMN_SCHEMA = {
    'type': 'object',
    'properties': {
        'id': {'type': 'number'},
        'name': {'type': 'string'}
    },
    'required': ['id', 'name']
}

DOCUMENT_SCHEMA = {
    'type': 'object',
    'properties': {
        'columns': {
            'type': 'array',
            'items': {'anyOf': [COLUMN_SCHEMA, {'type': 'string'}]}
        },
        'data': {'type': 'array', 'items': {'type': 'array'}}
    },
    'required': ['columns', 'data']
}


class DictionaryDocument(InMemoryDocument):
    """Create an in-memory document from a dictionary serialization of a
    document. Expects a dictionary in the following format:

    {
        "columns": [],
        "data": [[]]
    }

    Columns are either represented as strings (the column name) or as
    serializations of identifiable columns {"id": "int": "name": "string"}.

    Data is a list of rows, each a list of values.

    For documents that are keyed by a primary key a list of column names is
    expected. For documents that are keyed by a row index the respective list
    of integers is expected. The key elements are optional.
    """
    def __init__(self, doc, validate=True):
        """Create a in-memory document from the given dictionary.

        Parameters
        ----------
        doc: dict
            Dictionary serialization of a document.
        validate: bool default=True
            Validate the given dictionary if the flag is True. Raises a
            ValidationError if the validation fails.

        Raises
        ------
        jsonschema.ValidationError
        """
        self.doc = doc
        if validate:
            jsonschema.validate(instance=doc, schema=DOCUMENT_SCHEMA)
        # Deserialize columns.
        columns = list()
        for obj in doc['columns']:
            if isinstance(obj, dict):
                col = Column(
                    colid=obj['id'],
                    name=obj['name'],
                    colidx=len(columns)
                )
            else:
                # Assumes that the object is a scalar value (string or number)
                # representing the column name.
                col = Column(colid=-1, name=obj, colidx=len(columns))
            columns.append(col)
        # Get the document rows.
        rows = doc['data']
        # Create instance of the in-memory document.
        super(DictionaryDocument, self).__init__(
            columns=columns,
            rows=rows,
            readorder=anno.rowindex_readorder(index=list(range(len(rows))))
        )
