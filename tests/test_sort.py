import unittest

from histore.archive.base import Archive
from histore.path import Path
from histore.schema.document import DocumentSchema, SimpleDocumentSchema
from histore.schema.key import PathValuesKey, ListIndexKey, NodeValueKey


class TestSort(unittest.TestCase):

    def test_sort(self):
        """Test sort order of nodes in archive."""
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
        key1 = PathValuesKey(target_path=Path('modules'), value_paths=[Path('id'), Path('command/args/A')])
        key2 = ListIndexKey(target_path=Path('tasks/complete'))
        schema = DocumentSchema(keys=[key1, key2])
        archive = Archive(schema=schema)
        archive.insert(doc=doc)
        anno_nodes = archive.root().children
        self.assertEquals(anno_nodes[0].list_index[0].value, 2)
        self.assertEquals(anno_nodes[0].key, [101, 1])
        self.assertEquals(anno_nodes[1].list_index[0].value, 1)
        self.assertEquals(anno_nodes[1].key, [101, 2])
        self.assertEquals(anno_nodes[2].list_index[0].value, 0)
        self.assertEquals(anno_nodes[2].key, [200, 1])
        tasks = anno_nodes[-1]
        anno_nodes = tasks.children
        self.assertEquals(anno_nodes[0].list_index[0].value, 0)
        self.assertEquals(anno_nodes[1].list_index[0].value, 1)
        self.assertEquals(anno_nodes[2].list_index[0].value, 2)

    def test_strict_sort(self):
        """Test adding a snapshot to an empty archive with duplicate keys."""
        # Duplicate path values
        key1 = PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')])
        key2 = ListIndexKey(target_path=Path('tasks/complete'))
        schema = DocumentSchema(keys=[key1, key2])
        archive = Archive(schema=schema)
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
        archive.insert(doc=doc, strict=False)
        with self.assertRaises(ValueError):
            archive.insert(doc=doc, strict=True)
        # Duplicate node value
        key1 = PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')])
        key2 = NodeValueKey(target_path=Path('tasks/complete'))
        schema = DocumentSchema(keys=[key1, key2])
        archive = Archive(schema=schema)
        doc = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 2, 'B': 2}}},
                {'id': 103, 'command': {'args' : {'A': 1, 'B': 2}}}
            ],
            'tasks' : {
                'complete': ['B', 'B', 'C']
            }
        }
        archive.insert(doc=doc, strict=False)
        with self.assertRaises(ValueError):
            archive.insert(doc=doc, strict=True)
        # Handle documents if archives with empty schema
        archive = Archive()
        doc = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : [{'A': 1, 'B': 2}, {'A': 1, 'B': 2}]}},
                {'id': 101, 'command': {'args' : [{'A': 2, 'B': 2}, {'A': 2, 'B': 2}]}},
                {'id': 103, 'command': {'args' : [{'A': 1, 'B': 2}, {'A': 1, 'B': 2}]}}
            ],
            'tasks' : {
                'complete': ['B', 'B', 'C']
            }
        }
        archive.insert(doc=doc, strict=False)
        with self.assertRaises(ValueError):
            archive.insert(doc=doc, strict=True)
        # When using a simple document schema every node should have a unique
        # key and no exception is raised in strict mode
        archive = Archive(schema=SimpleDocumentSchema(doc))
        archive.insert(doc=doc, strict=False)
        archive.insert(doc=doc, strict=True)


if __name__ == '__main__':
    unittest.main()
