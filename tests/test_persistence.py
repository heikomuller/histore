import os
import shutil
import unittest

from histore.archive.base import Archive
from histore.archive.store.fs import PersistentArchiveStore
from histore.archive.store.fs import JSON, YAML, CACHE_ALL, CACHE_METADATA, LABEL_ROOT, LABEL_SCHEMA
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.schema.key import PathValuesKey, ListIndexKey, NodeValueKey


DOC1 = {
    'name': 'MY NAME',
    'modules': [
        {'id': 0, 'commands': [
            {'type': 'LOAD', 'run': {'pid': 1, 'time': '124'}, 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
            {'type': 'RUN', 'run': {'pid': 2}, 'args' :  [{'key': 'C', 'value': 2}]}
        ]}
    ],
    'tasks' : {
        'complete': ['B', 'A', 'C']
    },
    'outputs' : {
        'stdout': ['A'],
        'stderr': []
    }
}
DOC2 = {
    'name': 'A NAME',
    'modules': [
        {'id': 0, 'commands': [
            {'type': 'LOAD', 'run': {'pid': 1, 'time': '123'}, 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
            {'type': 'RUN', 'run': {'pid': 2}, 'args' :  [{'key': 'C', 'value': 2}]}
        ]}
    ],
    'tasks' : {
        'complete': ['B']
    },
    'outputs' : {
        'stderr': ['A', 'B']
    }
}
DOC3 = {
    'name': 'THE NAME',
    'modules': [
        {'id': 0, 'commands': [
            {'type': 'LOAD', 'run': {'pid': 1, 'time': '123'}, 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
            {'type': 'RUN', 'run': {'pid': 2}, 'args' :  [{'key': 'C', 'value': 2}]}
        ]},
        {'id': 0, 'commands': [
            {'type': 'LOAD', 'run': {'pid': 1, 'time': '123'}, 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
            {'type': 'DEL', 'run': {'pid': 2}, 'args' :  [{'key': 'C', 'value': 2}]}
        ]}
    ],
    'tasks' : {
        'complete': ['B']
    },
    'outputs' : {
        'stderr': [{}]
    }
}

SCHEMA=DocumentSchema(keys=[
    PathValuesKey(target_path=Path('modules/commands'), value_paths=[Path('type'), Path('run/pid')]),
    PathValuesKey(target_path=Path('modules/commands/args'), value_paths=[Path('key')]),
    PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')]),
    NodeValueKey(target_path=Path('tasks/complete')),
    ListIndexKey(target_path=Path('outputs/stdout')),
    ListIndexKey(target_path=Path('outputs/stderr')),
])

ENV_DIR = '.env'

ARCHIVE_JSON = '.env/archive.json'
ARCHIVE_JSON_GZ = '.env/archive.json.gz'
ARCHIVE_YAML = '.env/archive.yaml'
ARCHIVE_YAML_GZ = '.env/archive.yaml.gz'
ARCHIVE_YML = '.env/archive.yml'
ARCHIVE_YML_GZ = '.env/archive.yml.gz'
ARCHIVE_TXT = '.env/archive.txt'
ARCHIVE_TXT_GZ = '.env/archive.txt.gz'


class TestPersistentArchive(unittest.TestCase):

    def setUp(self):
        """Create an empty file server repository."""
        # Drop the env directory and re-create it
        if os.path.isdir(ENV_DIR):
            shutil.rmtree(ENV_DIR)
        os.makedirs(ENV_DIR)

    def tearDown(self):
        """Clean-up by dropping file server directory.
        """
        # Drop the env directory
        if os.path.isdir(ENV_DIR):
            shutil.rmtree(ENV_DIR)

    def test_archive_merge(self):
        """Test creating an archive and merging and retrieving documents."""
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=store)
        archive.insert(doc=DOC1)
        archive.insert(doc=DOC2)
        archive.insert(doc=DOC3)
        archive.insert(doc=DOC1)
        archive.insert(doc=DOC2)
        archive.insert(doc=DOC3)
        archive.insert(doc=DOC1)
        archive.insert(doc=DOC2)
        archive.insert(doc=DOC3)
        self.validate_archive(archive, [DOC1, DOC2, DOC3])

    def test_archive_merge_cache_metadata(self):
        """Test creating an archive and merging and retrieving documents
        when caching onlt metadata.
        """
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA, cache=CACHE_METADATA)
        archive = Archive(store=store)
        archive.insert(doc=DOC1)
        archive.insert(doc=DOC2)
        archive.insert(doc=DOC3)
        archive.insert(doc=DOC1)
        archive.insert(doc=DOC2)
        archive.insert(doc=DOC3)
        archive.insert(doc=DOC1)
        archive.insert(doc=DOC2)
        archive.insert(doc=DOC3)
        self.validate_archive(archive, [DOC1, DOC2, DOC3])

    def test_archive_merge_uncached(self):
        """Test creating an archive and merging and retrieving documents
        without caching. Also ensure that reading (initializing the archive)
        works.
        """
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA, cache=None)
        archive = Archive(store=store)
        archive.insert(doc=DOC1)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC2)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC3)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC1)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC2)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC3)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC1)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC2)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC3)
        self.validate_archive(archive, [DOC1, DOC2, DOC3])

    def test_archive_merge_with_relaod(self):
        """Test creating an archive and merging and retrieving documents while
        reloading the archive in between."""
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC1)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC2)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC3)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC1)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC2)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC3)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC1)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC2)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC3)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        self.validate_archive(archive, [DOC1, DOC2, DOC3])


    def test_cache(self):
        """Ensure that caching works as expected."""
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON))
        archive.insert(doc=DOC1)
        # All values should be cached. Delete file and ensure we can still
        # access everything without raising and exception.
        os.remove(ARCHIVE_JSON)
        archive.root()
        archive.schema()
        archive.snapshots()
        # Test that CACHE_ALL also caches all
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=CACHE_ALL))
        archive.insert(doc=DOC1)
        # All values should be cached. Delete file and ensure we can still
        # access everything without raising and exception.
        os.remove(ARCHIVE_JSON)
        archive.root()
        archive.schema()
        archive.snapshots()
        # Test that CACHE_METADTA also caches schema and snapshots
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=CACHE_METADATA))
        archive.insert(doc=DOC1)
        # Schema and snapshots should be cached. Delete file and ensure we can
        # still access them.
        os.remove(ARCHIVE_JSON)
        archive.schema()
        archive.snapshots()
        with self.assertRaises(IOError):
            archive.root()
        # Test that caching none raises exception for all objects when file
        # is deleted.
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=None))
        archive.insert(doc=DOC1)
        os.remove(ARCHIVE_JSON)
        with self.assertRaises(IOError):
            archive.schema()
        with self.assertRaises(IOError):
            archive.snapshots()
        with self.assertRaises(IOError):
            archive.root()
        # Test that caching only root would also work.
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=[LABEL_ROOT]))
        archive.insert(doc=DOC1)
        os.remove(ARCHIVE_JSON)
        with self.assertRaises(IOError):
            archive.schema()
        with self.assertRaises(IOError):
            archive.snapshots()
        archive.root()
        # Test that caching root and schema works.
        store = PersistentArchiveStore.create(ARCHIVE_JSON, SCHEMA)
        archive = Archive(store=PersistentArchiveStore(ARCHIVE_JSON, cache=[LABEL_ROOT, LABEL_SCHEMA]))
        archive.insert(doc=DOC1)
        os.remove(ARCHIVE_JSON)
        archive.schema()
        with self.assertRaises(IOError):
            archive.snapshots()
        archive.root()

    def test_guess_format_and_compression(self):
        """Ensure that deriving format and compression from filename works
        as exprected."""
        # Create all files to prevent exception
        for filename in [ARCHIVE_JSON, ARCHIVE_JSON_GZ, ARCHIVE_YAML, ARCHIVE_YAML_GZ, ARCHIVE_YML, ARCHIVE_YML_GZ, ARCHIVE_TXT, ARCHIVE_TXT_GZ]:
                open(filename, 'w').close()
        store = PersistentArchiveStore(filename=ARCHIVE_JSON, cache=None)
        self.validate_store(store, JSON, False)
        store = PersistentArchiveStore(filename=ARCHIVE_JSON_GZ, cache=None)
        self.validate_store(store, JSON, True)
        store = PersistentArchiveStore(filename=ARCHIVE_YAML, cache=None)
        self.validate_store(store, YAML, False)
        store = PersistentArchiveStore(filename=ARCHIVE_YAML_GZ, cache=None)
        self.validate_store(store, YAML, True)
        store = PersistentArchiveStore(filename=ARCHIVE_YML, cache=None)
        self.validate_store(store, YAML, False)
        store = PersistentArchiveStore(filename=ARCHIVE_YML_GZ, cache=None)
        self.validate_store(store, YAML, True)
        store = PersistentArchiveStore(filename=ARCHIVE_TXT, cache=None)
        self.validate_store(store, JSON, False)
        store = PersistentArchiveStore(filename=ARCHIVE_TXT_GZ, cache=None)
        self.validate_store(store, JSON, True)
        # Ensure that if compressed is True the store will also be compressed
        # independently of the file name
        store = PersistentArchiveStore(filename=ARCHIVE_JSON, compressed=True, cache=None)
        self.validate_store(store, JSON, True)
        store = PersistentArchiveStore(filename=ARCHIVE_JSON_GZ, compressed=True, cache=None)
        self.validate_store(store, JSON, True)
        # Ensure that if a format is given the store format is initialized
        # correctly
        store = PersistentArchiveStore(filename=ARCHIVE_JSON, format=YAML, cache=None)
        self.validate_store(store, YAML, False)
        store = PersistentArchiveStore(filename=ARCHIVE_JSON_GZ, format=YAML, cache=None)
        self.validate_store(store, YAML, True)
        store = PersistentArchiveStore(filename=ARCHIVE_JSON, format=YAML, compressed=True, cache=None)
        self.validate_store(store, YAML, True)
        store = PersistentArchiveStore(filename=ARCHIVE_JSON_GZ, format=YAML, compressed=True, cache=None)
        self.validate_store(store, YAML, True)

    def test_invalid_format(self):
        """Ensure that exceptions are raised if invalid format is given."""
        # Ensure an exception is raised if file does not exist.
        with self.assertRaises(ValueError):
            PersistentArchiveStore(filename=ARCHIVE_JSON, format='json')
        # Create the archive file to avoid exception
        open(ARCHIVE_JSON, 'w').close()
        PersistentArchiveStore(filename=ARCHIVE_JSON, format='json', cache=None)
        PersistentArchiveStore(filename=ARCHIVE_JSON, format='yaml', cache=None)
        with self.assertRaises(ValueError):
            PersistentArchiveStore(filename=ARCHIVE_JSON, format='txt', cache=None)
        with self.assertRaises(ValueError):
            PersistentArchiveStore(filename=ARCHIVE_JSON, format='TEXt', cache=None)

    def validate_archive(self, archive, documents):
        """Validate the snapshots in the archive. It is assumes that the
        documents in the given list were alternating merged into the
        archive.

        Parameters
        ----------
        archive: histore.archive.base.archive
        documents: list(dict)
        """
        index = 0
        for i in range(archive.length()):
            snapshot = archive.snapshot(i)
            doc = archive.get(snapshot.version)
            self.assertEquals(doc, documents[index])
            index = (index + 1) % len(documents)

    def validate_store(self, store, format, compressed):
        """Ensure that the given store has the given values for format and
        compressed.

        Parameters
        ----------
        store: histore.archive.store.fs.PersistentArchiveStore
        format: string
        compressed: bool
        """
        self.assertEquals(store.format, format)
        self.assertEquals(store.compressed, compressed)


if __name__ == '__main__':
    unittest.main()
