==================================
HISTORE - Data Frame History Store
==================================

**History Store** (HISTORE) is a Pyhton package for maintaining snapshots of evolving data sets. This package provides an implementation of the core functionality that was implemented in the `XML Archiver (XArch) <http://xarch.sourceforge.net/>`_. The package is a lightweight implementation that is intended for maintaining data set snapshots that are represented as pandas data frames.

**HISTORE** is based on a nested merge approach that efficiently stores multiple dataset snapshots in a compact archive `[Buneman, Khanna, Tajima, Tan. 2004] <https://dl.acm.org/citation.cfm?id=974752>`_. The library allows one to create new archives, to merge new data set snapshots into an existing archive, and to retrieve data set snapshots from the archive.


Installation
============

Install ``histore`` from the  `Python Package Index (PyPI) <https://pypi.org/>`_ using ``pip`` with:

.. code-block:: bash

  pip install histore


Examples
========

**HISTORE** maintains data set versions (snapshots) in an archive. A separate archive is created for each data set. The package currently provides two different types of archive: a volatile archive that maintains all data set snapshots in main-memory and a persistent archive that writes data set snapshots to disk.


Example using Volatile Archive
------------------------------

Start by creating a new archive. For each archive, a optional primary key (list of column names) can be specified. If a primary key is given, the values in the key attributes are used as row keys when data set snapshots are merged into the archive. If no primary key is specified the row index of the data frame is used to match rows during the merge phase.

For archives that have a primary key, the initial dataset snapshot (or at least the dataset schema) needs to be given when creating the archive.

.. code-block:: python

   # Create a new archive that merges snapshots
   # based on a primary key attribute

   import histore as hs
   import pandas as pd

  # First version
   df = pd.DataFrame(
       data=[['Alice', 32], ['Bob', 45], ['Claire', 27], ['Dave', 23]],
       columns=['Name', 'Age']
   )
   archive = hs.Archive(doc=df, primary_key='Name', descriptor=hs.Descriptor('First snapshot'))


Add the first two data set versions to the archive:

.. code-block:: python

   # Second version: Change age for Alice and Bob
   df = pd.DataFrame(
       data=[['Alice', 33], ['Bob', 44], ['Claire', 27], ['Dave', 23]],
       columns=['Name', 'Age']
   )
   archive.commit(df, descriptor=hs.Descriptor('Alice is 33 and Bob 44'))


List information about all snapshots in the archive. This also shows how to use the checkout method to retrieve a particular data set version:

.. code-block:: python

   # Print all data frame versions
   for s in archive.snapshots():
       df = archive.checkout(s.version)
       print('({}) {}\n'.format(s.version, s.description))
       print(df)
       print()

The result should look like this:

.. code-block:: console

   (0) First snapshot

        Name  Age
   0   Alice   32
   1     Bob   45
   2  Claire   27
   3    Dave   23

   (1) Alice is 33 and Bob 44

        Name  Age
   0   Alice   33
   1     Bob   44
   2  Claire   27
   3    Dave   23


Example using Persistent Archive
--------------------------------

To create persistent archive that maintains all data on disk use the ``PersistentArchive`` class:

.. code-block:: python

   archive = hs.PersistentArchive(basedir='path/to/archive/dir', create=True, doc=df, primary_key=['Name'])

The persistent archive maintains the data set snapshots in two files that are created in the directory that is given as the ``basedir`` argument.

For more examples see the notebooks in the `examples folder <https://github.com/heikomuller/histore/tree/pandas/examples>`_.
