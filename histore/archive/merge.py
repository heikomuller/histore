# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2020 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Nested-merge logic that merges rows in a snapshot document into an existing
dataset archive.
"""

from histore.archive.row import ArchiveRow


def merge_rows(
    self, archive, document, version, writer, partial=False,
    unchanged_cells=None, origin=None
):
    """Merge rows in the given archive and database snapshot. Outputs the
    merged rows in the resulting archive to the given archive writer.

    Parameters
    ----------
    archive: histore.archive.base.Archive
        Archive containing previous snapshots of the dataset.
    document: histore.document.base.Document
        Dataset snapshot that is being merged into the archive.
    version: int
        Identifier of the new archive version.
    writer: histore.archive.writer.ArchiveWriter
        Consumer for rows in the new archive version.
    partial: bool, default=False
        Flag indicating whether the given document is a partial document. For
        partial documents missing rows in the archive are considered unchanged
        with respect to the given origin.
    unchanged_cells: set, default=None
        Set of identifier for columns whose cell values are not included in
        the values dictionary for document rows but that remain unchanged with
        respect to the specified source version (origin).
    origin: int, default=None
        Version that the row values originate from. Rows that remain unchanged
        have the timestamp extended for the version of origin.
    """
    # Get reader for the given archive and document.
    arch_reader = archive.reader()
    doc_reader = document.reader()
    # Get the first row for each reader.
    arch_row = arch_reader.next()
    doc_row = doc_reader.next()
    while arch_row is not None and doc_row is not None:
        comp = arch_row.comp(doc_row.identifier)
        if comp < 0:
            # The row is not present in the document. If the document is
            # a partial document we need to extend the row for the given
            # origin.
            if partial:
                arch_row = arch_row.extend(version=version, origin=origin)
            # Add the row to the new archive version and progress the
            # archive reader.
            writer.write(arch_row)
            arch_row = arch_reader.next()
        elif comp > 0:
            # The document row is a new row. Create an archive row from the
            # document row and pass it to the writer. Progress the document
            # reader.
            writer.write(ArchiveRow.from_doc(row=doc_row, version=version))
            doc_row = doc_reader.next()
        else:
            # Merge the archive row and the document row.
            arch_row = arch_row.merge(
                values=doc_row.values,
                pos=doc_row.pos,
                version=version,
                unchanged_cells=unchanged_cells,
                origin=origin
            )
            # Add the merged row to the new archive version and progress
            # both readers.
            writer.write(arch_row)
            arch_row = arch_reader.next()
            doc_row = doc_reader.next()
    # Add remaining rows to the archive. Only one of the two conditions can
    # be true; either there are additional archive rows or there are additional
    # document rows.
    # Add remaining archive rows to the new archive version.
    while arch_row is not None:
        # Extend the row for the given origin if the document is partial.
        if partial:
            arch_row = arch_row.extend(version=version, origin=origin)
        writer.write(arch_row)
        arch_row = arch_reader.next()
    # Add remaining document rows to the new archive version.
    while doc_row is not None:
        # Outout an archive row created from the document row.
        writer.write(ArchiveRow.from_doc(row=doc_row, version=version))
        doc_row = doc_reader.next()
