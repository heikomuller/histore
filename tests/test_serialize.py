import unittest

from histore.archive.base import Archive
from histore.archive.serialize import ArchiveSerializer, DefaultArchiveSerializer
from histore.archive.serialize import LABEL_META, LABEL_VALUE
from histore.archive.store.mem import InMemoryArchiveStore
from histore.debug import archive_root_to_json_string, print_archive
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.schema.key import PathValuesKey, ListIndexKey, NodeValueKey


class TestSerialize(unittest.TestCase):

    def test_mapping(self):
        """Test correctness of user-provided mappings for keywords."""
        # Should not raise an exception
        serializer = ArchiveSerializer(mapping={LABEL_META: '@info', LABEL_VALUE: '@text'})
        self.assertEquals(serializer.mapping[LABEL_META], '@info')
        self.assertEquals(serializer.mapping[LABEL_VALUE], '@text')
        # Create simple serializarion to ensure that the mapping works
        archive = Archive()
        archive.insert(doc={'name':'MY NAME'})
        serializer = DefaultArchiveSerializer(schema=DocumentSchema())
        doc = serializer.to_dict(archive.root())
        self.assertTrue(LABEL_META, doc['root'])
        self.assertTrue(LABEL_VALUE, doc['root']['name'][0])
        serializer = DefaultArchiveSerializer(schema=DocumentSchema(), mapping={LABEL_META: '@info', LABEL_VALUE: '@text'})
        doc = serializer.to_dict(archive.root())
        self.assertTrue('@info', doc['root'])
        self.assertTrue('@text', doc['root']['name'][0])
        # Non-unique mapping should raise an exception
        with self.assertRaises(ValueError):
            ArchiveSerializer(mapping={LABEL_META: "@info", LABEL_VALUE: '@info'})

    def test_serialize_archive_with_schema(self):
        """Test serializing an archive."""
        doc1 = {
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
        doc2 = {
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
        schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules/commands'), value_paths=[Path('type'), Path('run/pid')]),
            PathValuesKey(target_path=Path('modules/commands/args'), value_paths=[Path('key')]),
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')]),
            NodeValueKey(target_path=Path('tasks/complete')),
            ListIndexKey(target_path=Path('outputs/stdout')),
            ListIndexKey(target_path=Path('outputs/stderr')),
        ])
        archive = Archive(schema=schema)
        serializer = DefaultArchiveSerializer(schema=archive.schema())
        archive.insert(doc=doc1)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc2)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc1)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc2)
        #print_archive(archive)
        self.validate_archive(archive, [doc1, doc2])

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
        serializer = DefaultArchiveSerializer(schema=archive.schema())
        archive.insert(doc=doc1)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc2)
        self.validate_archive(archive, [doc1, doc2])
    def test_serialize_with_changing_list_behavior(self):
        """Test merging documents where the same element is a list ine one
        version and a single element in the other.
        """
        doc1 = {
            'modules': {
                'id': 1,
                'name': 'A'
            }
        }
        doc2 = {
            'modules': [{
                'id': 1,
                'name': 'B'
            }]
        }
        doc3 = {
            'modules': [{
                'id': 2,
                'name': 'A'
            }]
        }
        schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')])
        ])
        archive = Archive(schema=schema)
        serializer = DefaultArchiveSerializer(schema=archive.schema())
        archive.insert(doc=doc1)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc2)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc3)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc1)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc2)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc3)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        self.validate_archive(archive, [doc1, doc2, doc3])
        doc1 = {
            'name': 'MY NAME'
        }
        doc2 = {
            'name': ['MY NAME']
        }
        doc3 = {
            'name': 'A NAME'
        }
        doc4 = {
            'name': ['A NAME']
        }
        schema=DocumentSchema(keys=[
            NodeValueKey(target_path=Path('name'))
        ])
        archive = Archive(schema=schema)
        archive.insert(doc=doc1)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc2)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc3)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc4)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc1)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc2)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc3)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        archive.insert(doc=doc4)
        a_doc = serializer.to_dict(archive.root())
        archive = Archive(
            store=InMemoryArchiveStore(
                root=serializer.from_dict(a_doc),
                snapshots=archive.snapshots(),
                schema=archive.schema()
            )
        )
        self.validate_archive(archive, [doc1, doc2, doc3, doc4])

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
