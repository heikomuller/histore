History Store
=============

**History Store** (HiStore) is a library for maintaining snapshots of evolving datasets in an efficient manner. HiStore is an implementation of the core functionality that was implemented in [XArch](http://xarch.sourceforge.net/).

HiStore is based on a nested merge approach that efficiently stores multiple dataset snapshots in a compact archive ([Buneman, Khanna, Tajima, Tan. 2004](https://dl.acm.org/citation.cfm?id=974752)). The library allows one to create new archives, to merge new snapshots of a dataset into an existing archive, and to execute both snapshot and temporal queries.
