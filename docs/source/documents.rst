==================
Snapshot Documents
==================

HISTORE archives are collections of snapshots (versions) of an evolving dataset. The individual snapshots are for example given as ``pandas.DataFrame`` objects or as CSV files. Internally, HISTORE represents the snapshots as ``histore.document.base.Document`` objects. The document object serves two main purposes: (1) it contains the document schema (i.e., a list of column names), and (2) it supports reading the document rows using a ``histore.document.base.DocumentIterator``.


histore.document.base.Document
------------------------------

We distinguish two main types of documents: in-memory documents and external documents. An in-memory document, as the name suggests, is read completely into memory. The ``histore.document.df.DataFrameIterator`` document class that wraps a ``pandas.DataFrame`` object is one example for an in-memory document. External documents represent snapshots that are maintained in external storage, e.g., as files on the file system. The ``histore.document.csv.CSVFile`` is an example for snapshots that are given as CSV files.

From HISTORE's point of view, the main differemce between the two main document types is how they are being sorted. Sorting is a prerequisite for documents that are merged into archives that are keyed by a primary key. For these documents the rows need to be sorted by the primary key column(s) before merging them into the archive (see ()[] for more details on the underlying algorithms for maintaining archives for large datasets). In-memory documents can be sorted in memory. For external documents, however, there is no guarantee that all document rows will fit into the available main memory. Thus, external documents are sorted using external merge sort before merging them into the archive.

For archives that are not keyed by a primary key (referred to as un-keyed)  the row index is used as the merge key (see below). For external documents this index in general corresponds to the position of the row in the document. Thus, external documents are considered as being sorted by the row index and can be merged into an un-keyed archive.


histore.document.base.DocumentIterator
--------------------------------------

The document iterator provides access to the document rows. With each row we also maintain the row position in the document (starting at 0) and the row index. For external documents the index and the row position are the same values in most cases. For in-memory documents that are represented as ``pandas.DataFrame`` objects, the index value is taken from the data frame index for the row.
