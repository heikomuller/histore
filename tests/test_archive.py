import unittest

from histore.archive.base import Archive
from histore.archive.node import ArchiveElement
from histore.archive.snapshot import Snapshot
from histore.archive.store.mem import InMemoryArchiveStore
from histore.schema.document import SimpleDocumentSchema
from histore.timestamp import Timestamp

class TestArchive(unittest.TestCase):

    def test_initialize(self):
        """Test initializing a new archive."""
        archive = Archive()
        self.assertIsNotNone(archive.schema())
        self.assertEquals(len(archive.schema().keys()), 0)
        self.assertEquals(archive.length(), 0)
        doc = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 2, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 1, 'B': 2}}}
            ],
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        }
        archive = Archive(schema=SimpleDocumentSchema(doc))
        self.assertIsNotNone(archive.schema())
        self.assertEquals(len(archive.schema().keys()), 2)
        self.assertEquals(archive.length(), 0)
        archive = Archive(
            store=InMemoryArchiveStore(
                root=ArchiveElement(label='root', timestamp=Timestamp.from_string('0-1')),
                schema=SimpleDocumentSchema(doc),
                snapshots=[Snapshot(0), Snapshot(1)]
            )
        )
        self.assertIsNotNone(archive.schema())
        self.assertEquals(len(archive.schema().keys()), 2)
        self.assertEquals(archive.length(), 2)
        with self.assertRaises(ValueError):
            archive = Archive(
                store=InMemoryArchiveStore(
                    schema=SimpleDocumentSchema(doc),
                    snapshots=[Snapshot(0), Snapshot(1)]
                )
            )
        with self.assertRaises(ValueError):
            archive = Archive(
                store=InMemoryArchiveStore(
                    root=ArchiveElement(label='root', timestamp=Timestamp.from_string('0-8')),
                    schema=SimpleDocumentSchema(doc),
                    snapshots=[Snapshot(0), Snapshot(1), Snapshot(8)]
                )
            )
        with self.assertRaises(ValueError):
            archive = Archive(
                store=InMemoryArchiveStore(
                    root=ArchiveElement(label='root', timestamp=Timestamp.from_string('0-1')),
                    schema=SimpleDocumentSchema(doc),
                    snapshots=[Snapshot(0), Snapshot(1), Snapshot(1)]
                )
            )
        with self.assertRaises(ValueError):
            archive = Archive(
                schema=SimpleDocumentSchema(doc),
                store=InMemoryArchiveStore()
            )

    def test_snapshot_serializatin(self):
        """Test dictionary serialization for snapshot objects."""
        s = Snapshot(1, name='My Name')
        c_time = s.created_at
        self.assertEquals(s.version, 1)
        self.assertEquals(s.name, 'My Name')
        self.assertIsNone(s.archive)
        s = Snapshot.from_dict(s.to_dict())
        self.assertEquals(s.version, 1)
        self.assertEquals(s.created_at, c_time)
        self.assertEquals(s.name, 'My Name')
        self.assertIsNone(s.archive)
        s = Snapshot.from_dict(s.to_dict(), archive=Archive())
        self.assertEquals(s.version, 1)
        self.assertEquals(s.created_at, c_time)
        self.assertEquals(s.name, 'My Name')
        self.assertIsNotNone(s.archive)

if __name__ == '__main__':
    unittest.main()
