import unittest

from histore.archive.base import Archive
from histore.archive.query.path import PathQuery
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.schema.key import PathValuesKey, NodeIndexKey, NodeValueKey
from histore.timestamp import TimeInterval, Timestamp


class TestMerge(unittest.TestCase):

    def test_repeated_merge(self):
        """Test repeatedly merging the same documents."""
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
            'name': 'SOME VALUE',
            'modules': [
                {'id': 0, 'commands': [
                    {'type': 'LOAD', 'args' :[{'key': 'C', 'value': 1}, {'key': 'A', 'value': 3}]},
                    {'type': 'RUN', 'args' : [{'key': 'C', 'value': 2}]}
                ]},
                {'id': 1, 'commands': [
                    {'type': 'LOAD', 'args' :  [{'key': 'C', 'value': 2}, {'key': 'B', 'value': None}]},
                    {'type': 'RUN'}
                ]}
            ],
            'tasks' : {
                'complete': ['A', 'B', 'C']
            }
        }
        schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules/commands'), value_paths=[Path('type')]),
            PathValuesKey(target_path=Path('modules/commands/args'), value_paths=[Path('key')]),
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')]),
            NodeValueKey(target_path=Path('tasks/complete'))
        ])
        archive = Archive(schema=schema)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        # modules(0)/commands(LOAD)/args(A)
        q = PathQuery().add('modules', key=[0]).add('commands', key=['LOAD']).add('args', key=['A'])
        node = q.find_one(archive.root())
        self.assertIsNotNone(node)
        self.assertTrue(node.timestamp.is_equal(Timestamp([TimeInterval(0,5)])))
        self.assertEquals(len(node.positions), 2)
        for pos in node.positions:
            self.assertTrue(pos.value in [0, 1])
            if pos.value == 0:
                self.assertTrue(pos.timestamp.is_equal(Timestamp.from_string('0,2,4')))
            else:
                self.assertTrue(pos.timestamp.is_equal(Timestamp.from_string('1,3,5')))
        # modules(1)/commands(LOAD)/args(B)
        q = PathQuery().add('modules', key=[1]).add('commands', key=['LOAD']).add('args', key=['B']).add('value')
        node = q.find_one(archive.root())
        self.assertEquals(len(node.children), 2)
        values = set()
        for child in node.children:
            values.add(child.value)
        self.assertEquals(values, set([1, None]))
        # tasks/complete(A)
        q = PathQuery().add('tasks').add('complete', key=['A'])
        node = q.find_one(archive.root())
        self.assertIsNotNone(node)
        self.assertEquals(len(node.positions), 2)
        for pos in node.positions:
            self.assertTrue(pos.value in [0, 1])
            if pos.value == 1:
                self.assertTrue(pos.timestamp.is_equal(Timestamp.from_string('0,2,4')))
            else:
                self.assertTrue(pos.timestamp.is_equal(Timestamp.from_string('1,3,5')))
        # Validate all snapshots in the archive
        self.validate_archive(archive, [doc1, doc2])
        #print archive.root().to_json_string(compact=True, schema=schema)

    def test_unkeyed_merge(self):
        """Test repeatedly merging the same documents."""
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
        changed_tasks = list()
        unchanged_tasks = list()
        for i in range(3):
            nodes = PathQuery().add('tasks').add('complete', key=[i]).find_all(archive.root())
            self.assertEquals(len(nodes), 1)
            node = nodes[0]
            if len(node.children) == 1:
                unchanged_tasks.append(node)
            elif len(node.children) == 2:
                changed_tasks.append(node)
        self.assertEquals(len(changed_tasks), 2)
        self.assertEquals(len(unchanged_tasks), 1)
        self.assertEquals(unchanged_tasks[0].children[0].value, 'C')
        for node in changed_tasks:
            labels = set()
            for child in node.children:
                labels.add(child.value)
            self.assertEquals(labels, set(['A', 'B']))
        # Validate all snapshots in the archive
        self.validate_archive(archive, [doc1, doc2])
        #print archive.root().to_json_string(compact=True)

    def test_value_and_element_children(self):
        """Test repeatedly merging the same documents."""
        doc1 = {
            'name': 'MY NAME',
            'tasks' : {
                'complete': ['B', 'A', 'C']
            }
        }
        doc2 = {
            'name':  {'value': 'MY NAME', 'suffix': 'txt'},
            'tasks' : {
                'complete': ['A', {'key': 'B'}, 'C']
            }
        }
        schema=DocumentSchema(keys=[
            NodeIndexKey(target_path=Path('tasks/complete'))
        ])
        archive = Archive(schema=schema)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        node = PathQuery().add('tasks').add('complete', key=[1]).find_one(archive.root())
        self.assertIsNotNone(node)
        self.assertEquals(len(node.children), 2)
        val_node = None
        el_node = None
        for child in node.children:
            if child.is_element():
                el_node = child
            elif child.is_value():
                val_node = child
        self.assertIsNotNone(el_node)
        self.assertIsNotNone(val_node)
        self.assertTrue(el_node.timestamp.is_equal(Timestamp.from_string('1,3,5')))
        self.assertEquals(el_node.children[0].value, 'B')
        self.assertTrue(val_node.timestamp.is_equal(Timestamp.from_string('0,2,4')))
        self.assertEquals(val_node.value, 'A')
        # Validate all snapshots in the archive
        self.validate_archive(archive, [doc1, doc2])
        #print archive.root().to_json_string(compact=True)

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


if __name__ == '__main__':
    unittest.main()
