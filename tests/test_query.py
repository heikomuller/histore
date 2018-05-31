import unittest

from histore.archive.base import Archive
from histore.archive.query.path import PathQuery
from histore.path import Path
from histore.schema.document import DocumentSchema, SimpleDocumentSchema
from histore.schema.key import PathValuesKey, NodeIndexKey, NodeValueKey


class TestQuery(unittest.TestCase):

    def test_query_unique_tree(self):
        """Test queries of archive nodes on a document where all nodes have
        unique identifier.
        """
        doc = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 6}}}
            ],
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        }
        key1 = PathValuesKey(target_path=Path('modules'), value_paths=[Path('id'), Path('command/args/A')])
        key2 = NodeValueKey(target_path=Path('tasks/complete'))
        archive = Archive(schema=DocumentSchema(keys=[key1, key2]))
        archive.insert(doc=doc)
        # Query arguments of individual modules
        query = PathQuery().add('modules', key=[200, 1]).add('command').add('args')
        node = query.find_one(archive.root())
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 1)
        self.assertEquals(node.children[1].children[0].value, 2)
        # There should only be one matching node
        self.assertEquals(len(query.find_all(archive.root())), 1)
        query = PathQuery().add('modules', key=[101, 5]).add('command').add('args')
        node = query.find_one(archive.root())
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 5)
        self.assertEquals(node.children[1].children[0].value, 6)
        # Non-matches
        query = PathQuery().add('modules', key=[101, 2]).add('command').add('args')
        self.assertIsNone(query.find_one(archive.root()))
        query = PathQuery().add('modules', key=[101, 5]).add('commando').add('args')
        self.assertIsNone(query.find_one(archive.root()))
        query = PathQuery().add('modules', key=[101, 5]).add('command').add('arg')
        self.assertIsNone(query.find_one(archive.root()))
        # Query tasks
        for key in ['B', 'A', 'C']:
            query = PathQuery().add('tasks').add('complete', key=[key])
            node = query.find_one(archive.root())
            self.assertEquals(node.children[0].value, key)
            self.assertEquals(len(query.find_all(archive.root())), 1)
        node = PathQuery().add('tasks').add('complete', key=['D']).find_one(archive.root())
        self.assertIsNone(node)

    def test_query_tree_with_duplicates(self):
        """Test queries of archive nodes on a document containing duplicate
        nodes.
        """
        doc = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 6}}}
            ],
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        }
        archive = Archive()
        archive.insert(doc=doc)
        query = PathQuery().add('modules').add('command').add('args')
        nodes = query.find_all(archive.root())
        self.assertEquals(len(nodes), 3)
        for node in nodes:
            self.assertEquals(len(node.children), 2)
            self.assertEquals(node.children[0].label, 'A')
            self.assertEquals(node.children[1].label, 'B')
        query = PathQuery().add('tasks').add('complete')
        nodes = query.find_all(archive.root())
        self.assertEquals(len(nodes), 3)
        values = set()
        for node in nodes:
            self.assertEquals(len(node.children), 1)
            values.add(node.children[0].value)
        self.assertEquals(values, set(['B', 'A', 'C']))
        # Check that exception is raised if find_one has more than one matches
        # and strict is True.
        query.find_one(archive.root())
        with self.assertRaises(ValueError):
            query.find_one(archive.root(), strict=True)

    def test_query_with_index_expressions(self):
        """Test queries of archive nodes on a document containing duplicate
        nodes.
        """
        doc = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 6}}}
            ],
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        }
        archive = Archive(schema=SimpleDocumentSchema(doc))
        archive.insert(doc=doc)
        # Query arguments of individual modules
        query = PathQuery().add('modules', key=[0]).add('command').add('args')
        node = query.find_one(archive.root())
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 1)
        self.assertEquals(node.children[1].children[0].value, 2)
        # There should only be one matching node
        self.assertEquals(len(query.find_all(archive.root())), 1)
        query = PathQuery().add('modules', key=[2]).add('command').add('args')
        node = query.find_one(archive.root())
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 5)
        self.assertEquals(node.children[1].children[0].value, 6)
        # Non-matches
        query = PathQuery().add('modules', key=[10])
        self.assertIsNone(query.find_one(archive.root()))
        query = PathQuery().add('modules', key=[101, 5])
        self.assertIsNone(query.find_one(archive.root()))
        query = PathQuery().add('modules', key=[1]).add('command').add('arg')
        self.assertIsNone(query.find_one(archive.root()))
        # Query tasks
        for key in range(2):
            query = PathQuery().add('tasks').add('complete', key=[key])
            node = query.find_one(archive.root())
            self.assertEquals(len(query.find_all(archive.root())), 1)
        node = PathQuery().add('tasks').add('complete', key=['D']).find_one(archive.root())
        self.assertIsNone(node)

    def test_snapshot_query(self):
        """Test queries of archive nodes on a document where all nodes have
        unique identifier.
        """
        doc1 = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 6}}}
            ],
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        }
        doc2 = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 3}}},
                {'id': 102, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 6}}}
            ],
            'tasks' : {
                'complete': ['A', 'B', 'C']
            }
        }
        doc3 = {
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 7}}}
            ],
            'tasks' : {
                'complete': ['B', 'C', 'A']
            }
        }
        archive = Archive(schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id'), Path('command/args/A')]),
            NodeValueKey(target_path=Path('tasks/complete'))
        ]))
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        archive.insert(doc=doc3)
        node = archive.get(1)
        for task in node.get('tasks').children:
            if task.value == 'A':
                self.assertEquals(task.index, 0)
            elif task.value == 'B':
                self.assertEquals(task.index, 1)
            elif task.value == 'C':
                self.assertEquals(task.index, 2)
            else:
                raise ValueError('unexpected task node \'' + str(task) + '\'')
        #from histore.document.node import print_node
        #print_node(node, indent='    ')

if __name__ == '__main__':
    unittest.main()
