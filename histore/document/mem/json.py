# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""In-memory document created from a Json serialization of a document. The Json
serialization of a document contains the list of columns ('columns') and an
array or arrays for document rows ('data'). The serialization has the optional
primary key element ('primaryKey') that contains a list of key columns.
"""

import jsonschema

from histore.document.mem.base import InMemoryDocument
from histore.document.schema import Column

import histore.document.schema as schema
import histore.key.annotate as anno


"""Json schema for Json document serializations."""

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
        'data': {'type': 'array', 'items': {'type': 'array'}},
        'primaryKey': {'type': 'array', 'items': {'type': 'string'}},
        'rowIndex': {'type': 'array', 'items': {'type': 'number'}}
    },
    'required': ['columns', 'data']
}


class JsonDocument(InMemoryDocument):
    """Create an in-memory document from a Json serialization of a document.
    Expects a Json document in the following format:

    {
        "columns": [],
        "data": [[]],
        "primaryKey": ["string" | "int"] | "rowIndex": ["int"]
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
            Json serialization of a document.
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
        # Create the keys for the document rows.
        if 'primaryKey' in doc:
            readorder = anno.pk_readorder(
                rows=rows,
                primary_key=schema.column_index(
                    schema=columns,
                    columns=doc['primaryKey']
                )
            )
        elif 'rowIndex' in doc:
            readorder = anno.rowindex_readorder(index=doc['rowIndex'])
        else:
            readorder = anno.rowindex_readorder(index=list(range(len(rows))))
        # Create instance of the in-memory document.
        super(JsonDocument, self).__init__(
            columns=columns,
            rows=rows,
            readorder=readorder
        )
