# Data Frame History Store - Changelog

### 0.1.0 - 05-06-2020

* Initial version. Support for snapshot archives in main-memory and on file system.


### 0.1.1 - 06-16-2020

* Allow different types of input documents (e.g., CSV files or Json)
* External merge-sort for large CSV files.
* Add managers for maintaining sets of archives


### 0.1.2 - ???

* Proper handling of date/time objects by the default archive reader and writer
* Optional arguments for Json encoder and decoder for persistent archives
* Add encoder and decoder information to archive manager metadata
* Simple command-line interface for persistent archive manager
