import unittest

from histore.archive.base import Archive
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.schema.key import KeyByChildNodes, KeyByNodeIndex


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
                'complete': ['B', 'A', 'C']
            }
        }
        key1 = KeyByChildNodes(target_path=Path('modules'), value_paths=[Path('id'), Path('command/args/A')])
        key2 = KeyByNodeIndex(target_path=Path('tasks/complete'))
        schema=DocumentSchema(keys=[key1, key2])
        archive = Archive(schema=schema)
        self.assertIsNone(archive.root)
        self.assertEquals(archive.length(), 0)
        snapshot = archive.insert(doc=doc, name='My Snapshot')
        #print_archive(archive, indent='  ')
        self.assertEquals(snapshot.version, 0)
        self.assertEquals(snapshot.name, 'My Snapshot')
        self.assertIsNotNone(archive.root)
        self.assertEquals(archive.length(), 1)
        snapshot = archive.insert(doc=doc)
        #print archive.root.to_json_string(compact=True, schema=schema)

    def test_sort(self):
        """Test adding a snapshot to an empty archive."""
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
        key1 = KeyByChildNodes(target_path=Path('modules'), value_paths=[Path('id'), Path('command/args/A')])
        key2 = KeyByNodeIndex(target_path=Path('tasks/complete'))
        schema = DocumentSchema(keys=[key1, key2])
        archive = Archive(schema=schema)
        archive.insert(doc=doc)
        anno_nodes = archive.root.children
        self.assertEquals(anno_nodes[0].positions[0].value, 2)
        self.assertEquals(anno_nodes[0].key, [101, 1])
        self.assertEquals(anno_nodes[1].positions[0].value, 1)
        self.assertEquals(anno_nodes[1].key, [101, 2])
        self.assertEquals(anno_nodes[2].positions[0].value, 0)
        self.assertEquals(anno_nodes[2].key, [200, 1])
        tasks = anno_nodes[-1]
        anno_nodes = tasks.children
        self.assertEquals(anno_nodes[0].positions[0].value, 0)
        self.assertEquals(anno_nodes[1].positions[0].value, 1)
        self.assertEquals(anno_nodes[2].positions[0].value, 2)


if __name__ == '__main__':
    unittest.main()
