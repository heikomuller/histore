import unittest

from histore.archive.base import Archive
from histore.archive.query.path import PathQuery, KeyConstraint, LabelConstraint
from histore.path import Path
from histore.schema.document import DocumentSchema, SimpleDocumentSchema
from histore.schema.key import PathValuesKey, ListIndexKey, NodeValueKey


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
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[200, 1])) \
            .add(KeyConstraint('command')) \
            .add(KeyConstraint('args'))
        node = archive.find_one(query)
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 1)
        self.assertEquals(node.children[1].children[0].value, 2)
        # There should only be one matching node
        self.assertEquals(len(archive.find_all(query)), 1)
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[101, 5])) \
            .add(KeyConstraint('command')) \
            .add(KeyConstraint('args'))
        node = archive.find_one(query)
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 5)
        self.assertEquals(node.children[1].children[0].value, 6)
        # Non-matches
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[101, 2])) \
            .add(KeyConstraint('command')) \
            .add(KeyConstraint('args'))
        self.assertIsNone(archive.find_one(query))
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[101, 5])) \
            .add(KeyConstraint('commando')) \
            .add(KeyConstraint('args'))
        self.assertIsNone(archive.find_one(query))
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[101, 5])) \
            .add(KeyConstraint('command')) \
            .add(KeyConstraint('arg'))
        self.assertIsNone(archive.find_one(query))
        # Query tasks
        for key in ['B', 'A', 'C']:
            query = PathQuery() \
                .add(KeyConstraint('tasks')) \
                .add(KeyConstraint('complete', key=[key]))
            node = archive.find_one(query)
            self.assertEquals(node.children[0].value, key)
            self.assertEquals(len(archive.find_all(query)), 1)
        node = archive.find_one(
            PathQuery() \
                .add(KeyConstraint('tasks')) \
                .add(KeyConstraint('complete', key=['D']))
            )
        self.assertIsNone(node)
        # Label Constraint
        nodes = archive.find_all(PathQuery().add(LabelConstraint('modules')))
        self.assertEquals(len(nodes), 3)
        # Any constraint
        nodes = archive.find_all(PathQuery().add())
        self.assertEquals(len(nodes), 5)
        nodes = archive.find_all(PathQuery().add(LabelConstraint('tasks')).add())
        self.assertEquals(len(nodes), 3)

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
        for i in range(3):
            query = PathQuery() \
                .add(KeyConstraint('modules', key=[i])) \
                .add(KeyConstraint('command')) \
                .add(KeyConstraint('args'))
            nodes = archive.find_all(query)
            self.assertEquals(len(nodes), 1)
            node = nodes[0]
            self.assertEquals(len(node.children), 2)
            self.assertEquals(node.children[0].label, 'A')
            self.assertEquals(node.children[1].label, 'B')
        values = set()
        for i in range(3):
            query = PathQuery() \
                .add(KeyConstraint('tasks')) \
                .add(KeyConstraint('complete', key=[i]))
            nodes = archive.find_all(query)
            self.assertEquals(len(nodes), 1)
            node = nodes[0]
            self.assertEquals(len(node.children), 1)
            values.add(node.children[0].value)
        self.assertEquals(values, set(['B', 'A', 'C']))
        # Check that exception is raised if find_one has more than one matches
        # and strict is True.
        archive.find_one(query)
        with self.assertRaises(ValueError):
            archive.find_one(query, strict=True)

    def test_query_with_missing_schema(self):
        """Test queries of archive generated without schema."""
        doc1 = {
            'name': 'MY NAME',
            'modules': [
                {'id': 0, 'commands': [
                    {'type': 'LOAD', 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
                    {'type': 'RUN', 'args' :  [{'key': 'C', 'value': 2}]}
                ]},
                {'id': 1, 'commands': [
                    {'type': 'LOAD', 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]}
                ]}
            ],
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        }
        doc2 = {
            'name': 'MY NAME',
            'modules': [
                {'id': 0, 'commands': [
                    {'type': 'LOAD', 'args' :[{'key': 'C', 'value': 1}, {'key': 'A', 'value': 3}]},
                    {'type': 'RUN', 'args' : [{'key': 'C', 'value': 2}]}
                ]},
                {'id': 1, 'commands': [
                    {'type': 'LOAD', 'args' :  [{'key': 'C', 'value': 2}, {'key': 'B', 'value': 2}]},
                    {'type': 'RUN'}
                ]}
            ],
            'tasks' : {
                'complete': ['A', 'B', 'C']
            }
        }
        archive = Archive()
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        nodes = archive.find_all(
            PathQuery() \
                .add(KeyConstraint('tasks')) \
                .add(KeyConstraint('complete'))
            )
        self.assertEquals(len(nodes), 0)

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
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[0])) \
            .add(KeyConstraint('command')) \
            .add(KeyConstraint('args'))
        node = archive.find_one(query)
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 1)
        self.assertEquals(node.children[1].children[0].value, 2)
        # There should only be one matching node
        self.assertEquals(len(archive.find_all(query)), 1)
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[2])) \
            .add(KeyConstraint('command')) \
            .add(KeyConstraint('args'))
        node = archive.find_one(query)
        self.assertEquals(len(node.children), 2)
        self.assertEquals(node.children[0].children[0].value, 5)
        self.assertEquals(node.children[1].children[0].value, 6)
        # Non-matches
        query = PathQuery().add(KeyConstraint('modules', key=[10]))
        self.assertIsNone(archive.find_one(query))
        query = PathQuery().add(KeyConstraint('modules', key=[101, 5]))
        self.assertIsNone(archive.find_one(query))
        query = PathQuery() \
            .add(KeyConstraint('modules', key=[1])) \
            .add(KeyConstraint('command')) \
            .add(KeyConstraint('arg'))
        self.assertIsNone(archive.find_one(query))
        # Query tasks
        for key in range(2):
            query = PathQuery() \
                .add(KeyConstraint('tasks')) \
                .add(KeyConstraint('complete', key=[key]))
            node = archive.find_one(query)
            self.assertEquals(len(archive.find_all(query)), 1)
        node = archive.find_one(
            PathQuery() \
                .add(KeyConstraint('tasks')) \
                .add(KeyConstraint('complete', key=['D']))
            )
        self.assertIsNone(node)

    def test_snapshot_query(self):
        """Test queries of archive nodes on a document where all nodes have
        unique identifier.
        """
        documents = list()
        documents.append({
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 6}}}
            ],
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        })
        documents.append({
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 3}}},
                {'id': 102, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 6}}}
            ],
            'tasks' : {
                'complete': ['A', 'B', 'C']
            }
        })
        documents.append({
            'name': 'A',
            'modules': [
                {'id': 200, 'command': {'args' : {'A': 1, 'B': 2}}},
                {'id': 101, 'command': {'args' : {'A': 3, 'B': 4}}},
                {'id': 101, 'command': {'args' : {'A': 5, 'B': 7}}}
            ],
            'tasks' : {
                'complete': ['B', 'C', 'A']
            }
        })
        archive = Archive(schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id'), Path('command/args/A')]),
            NodeValueKey(target_path=Path('tasks/complete'))
        ]))
        snapshots = list()
        for i in range(len(documents)):
            snapshots.append(archive.insert(doc=documents[i]))
        for i in range(len(snapshots)):
            self.assertEquals(archive.get(snapshots[i].version), documents[i])


if __name__ == '__main__':
    unittest.main()
