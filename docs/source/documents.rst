==================
Snapshot Documents
==================

Individual snapshots that are added to an archive are represented as ``histore.document.base.Document`` objects. A document primarily serves two purposes: (1) it contains the document schema (a list of column names), and (2) it supports reading the document rows using a ``histore.document.base.DocumentIterator``.

We distinguish two main types of documents: internal and external document. An internal document is read completely into memory as the name suggests. Internal documents can be sorted in memory. Sorting is a prerequisite for documents that are merged into archives that are keyed by a primary key. For external documents there is no guarantee that all document rows will fit into the available main memory. Thus, external documents are sorted using external merge sort before merging them into the archive.

The document iterator provides access to the document rows. With each row we also maintain the row position in the document (starting at 0) and the row index. For external documents the index and the position are the same in most cases. For internal documents that are represented as pandas.DataFrame the index comes from the data frame index for the row. For archives that are not keyed by a primary key the row index is used as the merge key.