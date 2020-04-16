History Store
=============

**History Store** (histore) is a Pyhton library for maintaining snapshots of evolving data sets in an efficient manner. **histore** is an implementation of the core functionality that was implemented in [XArch](http://xarch.sourceforge.net/).

**histore** is based on a nested merge approach that efficiently stores multiple dataset snapshots in a compact archive ([Buneman, Khanna, Tajima, Tan. 2004](https://dl.acm.org/citation.cfm?id=974752)). The library allows one to create new archives, to merge new snapshots of a dataset into an existing archive, and to execute both snapshot and temporal queries.

This is a light-weight version for tabular data only. No key specification. Assumes data frames where row index contains unique row identifier and columns have unique identifier.
