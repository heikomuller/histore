# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Nested-merge logic that merges rows in a snapshot document into an existing
dataset archive.
"""


def merge_rows(arch_reader, doc_reader, version, writer):
    """Merge rows in the given archive and database snapshot. Outputs the
    merged rows in the resulting archive to the given archive writer.

    Parameters
    ----------
    archive: histore.archive.reader.ArchiveReader
        Reader for archive containing previous snapshots of the dataset.
    doc_reader: histore.document.reader.DocumentReader
        Reader for rows in the merged dataset snapshot.
    version: int
        Identifier of the new archive version.
    writer: histore.archive.writer.ArchiveWriter
        Consumer for rows in the new archive version.
    """
    # Get the first row for each reader.
    arch_row = arch_reader.next()
    doc_row = doc_reader.next()
    while arch_row is not None and doc_row is not None:
        comp = arch_row.comp(doc_row.key)
        if comp < 0:
            # The row is not present in the document. Include the row without
            # changes to the new archive version and read the next archive row.
            writer.write_archive_row(arch_row)
            arch_row = arch_reader.next()
        elif comp > 0:
            # The document row is a new row. Create an archive row from the
            # document row and pass it to the writer. Progress the document
            # reader.
            writer.write_document_row(row=doc_row, version=version)
            doc_row = doc_reader.next()
        else:
            # Merge the archive row and the document row.
            arch_row = arch_row.merge(
                values=doc_row.values,
                pos=doc_row.pos,
                version=version
            )
            # Add the merged row to the new archive version and progress
            # both readers.
            writer.write_archive_row(arch_row)
            arch_row = arch_reader.next()
            doc_row = doc_reader.next()
    # Add remaining rows to the archive. Only one of the two conditions can
    # be true; either there are additional archive rows or there are additional
    # document rows.
    # Add remaining archive rows to the new archive version.
    while arch_row is not None:
        writer.write_archive_row(arch_row)
        arch_row = arch_reader.next()
    # Add remaining document rows to the new archive version.
    while doc_row is not None:
        # Output an archive row created from the document row.
        writer.write_document_row(row=doc_row, version=version)
        doc_row = doc_reader.next()
    doc_reader.close()
