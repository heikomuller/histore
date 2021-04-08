# Data Frame History Store - Changelog

### 0.1.0 - 2020-05-06

* Initial version. Support for snapshot archives in main-memory and on file system.


### 0.1.1 - 2020-06-16

* Allow different types of input documents (e.g., CSV files or Json).
* External merge-sort for large CSV files.
* Add managers for maintaining sets of archives.


### 0.1.2 - 2020-06-25

* Proper handling of date/time objects by the default archive reader and writer.
* Optional arguments for Json encoder and decoder for persistent archives.
* Add encoder and decoder information to archive manager metadata.
* Simple command-line interface for persistent archive manager.


### 0.1.3 - 2020-10-05

* Add archive manager that maintains descriptors in a relational database (\#8).


### 0.1.4 - 2020-10-07

* Add index position information to column class (\#11).


### 0.1.5 - 2020-11-06

* Add `__getitem__` and `get()` method to `SnapshotListing`.


### 0.2.0 - 2020-11-10

* Include wrapper for CSV files.
* Commit CSV files directly to a HISTORE archive.


### 0.2.1 - 2020-11-11

* Fix bug when adding snapshot from file without primary key (\#19).


### 0.2.2 - 2020-11-17

* Add default Json encoder and decoder for `ArchiveFileStore`.
* Add optional operation descriptor to snapshots (\#21).


### 0.3.0 - 2021-02-08

* Add support for archive rollback.


### 0.3.1 - 2021-02-22

* Disable type inference when checking out dataset snapshot as data frame (\#24).


### 0.4.0 - 2021-04-08

* Add more compact archive serialization option.
* Add option to select archive serializer (\#27)
* Add option to load initial archive snapshot from a data stream.