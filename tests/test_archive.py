import unittest

from histore.archive.base import Archive
from histore.path import Path
from histore.schema import DocumentSchema, KeySpec


class TestArchive(unittest.TestCase):

    def test_first_snapshot(self):
        """Test adding a snapshot to an empty archive."""
        doc = {
            'name': 'A',
            'modules': [
                {'id': 100, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 1, 'B': 2}}}
            ],
            'tasks' : {
                'complete': ['A', 'B', 'C']
            }
        }
        key1 = KeySpec(target_path=Path('modules'), value_paths=[Path('id'), Path('command/args/A')])
        key2 = KeySpec(target_path=Path('tasks/complete'))
        schema=DocumentSchema(keys=[key1, key2])
        archive = Archive(schema=schema)
        self.assertIsNone(archive.root)
        self.assertEquals(archive.length(), 0)
        snapshot = archive.insert(doc=doc, name='My Snapshot')
        self.assertEquals(snapshot.version, 0)
        self.assertEquals(snapshot.name, 'My Snapshot')
        self.assertIsNotNone(archive.root)
        self.assertEquals(archive.length(), 1)
        print archive.root.to_json_string()
        print archive.root.to_json_string(schema=schema, compact=True)

if __name__ == '__main__':
    unittest.main()
