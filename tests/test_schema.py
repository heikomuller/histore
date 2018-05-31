import unittest

from histore.document.base import Document
from histore.path import Path
from histore.schema.document import DocumentSchema, SimpleDocumentSchema
from histore.schema.key import KeySpec, PathValuesKey, NodeIndexKey, NodeValueKey

class TestSchema(unittest.TestCase):

    def test_annotate(self):
        """Test node annotations for given key specification."""
        obj = {
            'name': 'A',
            'modules': [
                {'id': 100, 'command': {'name': 'A', 'type': 1}, 'value': 'v1'},
                {'id': 101, 'command': {'name': 'B'}, 'value': 'v2'},
                {'id': [102, 103], 'command': {'name': 'A'}, 'value': 'v3'},
                {'id': 101, 'value': 'v2'},
                {'id': 101, 'command': [{'name': 'B'}, {'name': 'C'}], 'value': 'v2'}
            ]
        }
        doc = Document(doc=obj)
        key = PathValuesKey(target_path=Path('modules'), value_paths=[Path('id'), Path('command/name')])
        for node in doc.nodes:
            if key.matches(Path(node.label)):
                if node.index == 0:
                    self.assertEquals(key.annotate(node), [100, 'A'])
                elif node.index == 1:
                    self.assertEquals(key.annotate(node), [101, 'B'])
                else:
                    try:
                        key.annotate(node)
                    except ValueError as ex:
                        msg = str(ex)
                    if node.index == 2:
                        self.assertEquals(msg, "not a unique path 'id'")
                    elif node.index == 3:
                        self.assertEquals(msg, "missing key value for 'command/name'")
                    elif node.index == 4:
                        self.assertEquals(msg, "not a unique path 'command/name'")
        key = NodeIndexKey(target_path=Path('modules'))
        for node in doc.nodes:
            if key.matches(Path(node.label)):
                self.assertEquals(key.annotate(node), [node.index])
        key = NodeValueKey(target_path=Path('modules/value'))
        for node in doc.nodes:
            if node.label == 'modules':
                for child in node.children:
                    if key.matches(Path(node.label).extend(child.label)):
                        self.assertEquals(key.annotate(child), [child.value])
        # Exception if value-keyed node is not a leaf
        key = NodeValueKey(target_path=Path('modules/command'))
        for node in doc.nodes:
            if node.label == 'modules':
                for child in node.children:
                    if child.label == 'command':
                        with self.assertRaises(ValueError):
                            key.annotate(node)

    def test_match(self):
        """Test path matching."""
        key = NodeIndexKey(target_path=Path('A/B/C'))
        self.assertTrue(key.matches(Path(['A', 'B', 'C'])))
        self.assertFalse(key.matches(Path(['A', 'B'])))
        self.assertFalse(key.matches(Path(['A', 'B', 'E'])))

    def test_schema(self):
        """Test document schema functinality."""
        schema = DocumentSchema()
        schema.add(NodeIndexKey(Path('A/B')))
        schema.add(NodeIndexKey(Path('A/C')))
        self.validate_schema(schema)
        self.validate_schema(
            DocumentSchema([NodeIndexKey(Path('A/B')), NodeIndexKey(Path('A/C'))])
        )
        with self.assertRaises(ValueError):
            DocumentSchema([
                NodeIndexKey(Path('A/B')),
                NodeIndexKey(Path('A/C')),
                NodeIndexKey(Path('A/C'))
            ])

    def test_serialization(self):
        """Test serializing keys using .to_dict() and .from_dict()."""
        # Node index key
        key = NodeIndexKey(Path('A/B/C'))
        self.assertEquals(key.target_path.length(), 3)
        self.assertEquals(key.target_path.to_key(), 'A/B/C')
        key = KeySpec.from_dict(key.to_dict())
        self.assertTrue(isinstance(key, NodeIndexKey))
        self.assertEquals(key.target_path.length(), 3)
        self.assertEquals(key.target_path.to_key(), 'A/B/C')
        # Node value key
        key = NodeValueKey(Path('A'))
        self.assertEquals(key.target_path.length(), 1)
        self.assertEquals(key.target_path.to_key(), 'A')
        key = KeySpec.from_dict(key.to_dict())
        self.assertTrue(isinstance(key, NodeValueKey))
        self.assertEquals(key.target_path.length(), 1)
        self.assertEquals(key.target_path.to_key(), 'A')
        # Path values key
        key = PathValuesKey(Path('A/B/C'), [Path('A'), Path('C/D')])
        self.assertEquals(key.target_path.length(), 3)
        self.assertEquals(key.target_path.to_key(), 'A/B/C')
        self.assertEquals(len(key.value_paths), 2)
        self.assertEquals(key.value_paths[0].to_key(), 'A')
        self.assertEquals(key.value_paths[1].to_key(), 'C/D')
        key = KeySpec.from_dict(key.to_dict())
        self.assertTrue(isinstance(key, PathValuesKey))
        self.assertEquals(key.target_path.length(), 3)
        self.assertEquals(key.target_path.to_key(), 'A/B/C')
        self.assertEquals(len(key.value_paths), 2)
        self.assertEquals(key.value_paths[0].to_key(), 'A')
        self.assertEquals(key.value_paths[1].to_key(), 'C/D')
        # Document Schema
        schema = DocumentSchema()
        self.assertEquals(len(schema.keys()), 0)
        schema = DocumentSchema.from_dict(schema.to_dict())
        self.assertEquals(len(schema.keys()), 0)
        schema = DocumentSchema(keys=[
            NodeIndexKey(Path('A/B/C')),
            NodeValueKey(Path('A')),
            PathValuesKey(Path('A/B'), [Path('A'), Path('C/D')])
        ])
        self.assertEquals(len(schema.keys()), 3)
        schema = DocumentSchema.from_dict(schema.to_dict())
        self.assertEquals(len(schema.keys()), 3)

    def test_simple_schema(self):
        """Test creating a simple document schema from a dictionary."""
        doc = {
            'name': 'A',
            'modules': [
                {'id': 100, 'command': {'args' : [{'A': 1}, {'B': 2}]}},
                {'id': 101, 'command': {'args' : [{'C': 1}, {'D': 2}]}}
            ],
            'tasks' : {
                'complete': ['A', 'B', 'C']
            }
        }
        schema = SimpleDocumentSchema(doc)
        self.assertEquals(len(schema.keys()), 3)
        self.assertIsNotNone(schema.get(Path('modules')))
        self.assertIsNotNone(schema.get(Path('modules/command/args')))
        self.assertIsNotNone(schema.get(Path('tasks/complete')))
        doc['error'] = {
            'data': [{'A'}, ['Nested', 'list']]
        }
        with self.assertRaises(ValueError):
            SimpleDocumentSchema(doc)

    def validate_schema(self, schema):
        key = schema.get(Path('A/C'))
        self.assertEquals(key.target_path.length(), 2)
        self.assertEquals(key.target_path.get(1), 'C')
        self.assertIsNone(schema.get(Path('A')))
        with self.assertRaises(ValueError):
            schema.add(NodeIndexKey(Path('A/C')))


if __name__ == '__main__':
    unittest.main()
