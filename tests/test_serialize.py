import unittest

from histore.archive.base import Archive
from histore.archive.serialize import ArchiveSerializer, LABEL_META, LABEL_VALUE
from histore.debug import archive_node_to_json_string
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.schema.key import PathValuesKey, NodeIndexKey, NodeValueKey


class TestSerialize(unittest.TestCase):

    def test_mapping(self):
        """Test correctness of user-provided mappings for keywords."""
        # Should not raise an exception
        serializer = ArchiveSerializer(mapping={LABEL_META: '@info', LABEL_VALUE: '@text'})
        self.assertEquals(serializer.mapping[LABEL_META], '@info')
        self.assertEquals(serializer.mapping[LABEL_VALUE], '@text')
        # Non-unique mapping should raise an exception
        with self.assertRaises(ValueError):
            ArchiveSerializer(mapping={LABEL_META: "@info", LABEL_VALUE: '@info'})

    def test_serialize_archive_with_schema(self):
        """Test serializing an archive."""
        doc1 = {
            'name': 'MY NAME',
            'modules': [
                {'id': 0, 'commands': [
                    {'type': 'LOAD', 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
                    {'type': 'RUN', 'args' :  [{'key': 'C', 'value': 2}]}
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
        doc2 = {
            'name': 'A NAME',
            'modules': [
                {'id': 0, 'commands': [
                    {'type': 'LOAD', 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
                    {'type': 'RUN', 'args' :  [{'key': 'C', 'value': 2}]}
                ]}
            ],
            'tasks' : {
                'complete': ['B']
            },
            'outputs' : {
                'stderr': ['A', 'B']
            }
        }
        schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules/commands'), value_paths=[Path('type')]),
            PathValuesKey(target_path=Path('modules/commands/args'), value_paths=[Path('key')]),
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')]),
            NodeValueKey(target_path=Path('tasks/complete')),
            NodeIndexKey(target_path=Path('outputs/stdout')),
            NodeIndexKey(target_path=Path('outputs/stderr')),
        ])
        archive = Archive(schema=schema)
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        #print archive_node_to_json_string(archive.root(), schema=schema)

    def test_serialize_archive_without_schema(self):
        """Test serializing an archive that was generated without specifying a
        document schema.
        """
        doc1 = {
            'name': 'MY NAME',
            'modules': [
                {'id': 0, 'commands': [
                    {'type': 'LOAD', 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
                    {'type': 'RUN', 'args' :  [{'key': 'C', 'value': 2}]}
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
        doc2 = {
            'name': 'A NAME',
            'modules': [
                {'id': 0, 'commands': [
                    {'type': 'LOAD', 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
                    {'type': 'RUN', 'args' :  [{'key': 'C', 'value': 2}]}
                ]}
            ],
            'tasks' : {
                'complete': ['B']
            },
            'outputs' : {
                'stderr': ['A', 'B']
            }
        }
        archive = Archive()
        archive.insert(doc=doc1)
        archive.insert(doc=doc2)
        print archive_node_to_json_string(archive.root())


if __name__ == '__main__':
    unittest.main()
