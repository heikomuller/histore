import unittest

from histore.archive.base import Archive
from histore.archive.query.path import PathQuery
from histore.debug import print_archive
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.schema.key import PathValuesKey, NodeIndexKey, NodeValueKey


DOC = {
    'name': 'MY NAME',
    'modules': [
        {'id': 0, 'commands': [
            {'type': 'LOAD', 'args' : [{'key': 'A', 'value': 2}, {'key': 'B', 'value': 1}]},
            {'type': 'RUN', 'args' :  [{'key': 'C', 'value': 2}]}
        ]}
    ],
    'tasks' : {
        'complete': ['B', 'A', 'C'],
        'incomplete': {
            'error': ['E'],
            'canceled': ['F', 'G']
        }
    }
}


class TestAnnotate(unittest.TestCase):

    def test_annotate(self):
        """Ensure that the document gets annotated without error."""
        schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')]),
            PathValuesKey(target_path=Path('modules/commands'), value_paths=[Path('type')]),
            PathValuesKey(target_path=Path('modules/commands/args'), value_paths=[Path('key')]),
            NodeValueKey(target_path=Path('tasks/complete')),
            NodeIndexKey(target_path=Path('tasks/incomplete/error'))
        ])
        archive = Archive(schema=schema)
        archive.insert(doc=DOC)
        self.assertIsNotNone(archive.root())
        # Ensure that all complete nodes have keys
        incomplete = None
        for task in PathQuery().add('tasks').find_one(archive.root()).children:
            if task.label == 'complete':
                self.assertIsNotNone(task.key)
            elif task.label == 'incomplete':
                incomplete = task
        # Ensure that incomplete node does not kave key but all the children
        # have keys.
        self.assertIsNotNone(incomplete)
        self.assertIsNone(incomplete.key)
        for node in incomplete.children:
            self.assertIsNotNone(node.key)

    def test_annotation_errors(self):
        """Ensure that exceptions are thrown when documents are annotated with
        schemas that do not match the correct document structure."""
        schema=DocumentSchema(keys=[
            PathValuesKey(target_path=Path('modules'), value_paths=[Path('id')]),
            PathValuesKey(target_path=Path('modules/commands'), value_paths=[Path('type')]),
            PathValuesKey(target_path=Path('modules/commands/args'), value_paths=[Path('key')]),
            NodeValueKey(target_path=Path('tasks/complete')),
            NodeIndexKey(target_path=Path('tasks/incomplete')),
            NodeIndexKey(target_path=Path('tasks/incomplete/error'))
        ])
        archive = Archive(schema=schema)
        with self.assertRaises(ValueError):
            archive.insert(doc=DOC)


if __name__ == '__main__':
    unittest.main()
