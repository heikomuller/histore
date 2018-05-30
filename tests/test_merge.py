import unittest

from histore.archive.merge import AnnotatedNode, sort_nodes
from histore.document.base import Document
from histore.path import Path
from histore.schema.document import DocumentSchema
from histore.schema.key import KeyByChildNodes, KeyByNodeIndex


class TestMerge(unittest.TestCase):

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
        schema=DocumentSchema(keys=[key1, key2])
        path = Path('')
        anno_nodes = list()
        for node in Document(doc=doc).nodes:
            key = schema.get(path.extend(node.label))
            if not key is None:
                anno_nodes.append(AnnotatedNode(node, key=key.annotate(node)))
            else:
                anno_nodes.append(AnnotatedNode(node))
        anno_nodes = sort_nodes(anno_nodes)
        self.assertEquals(anno_nodes[0].node.index, 2)
        self.assertEquals(anno_nodes[0].key, [101, 1])
        self.assertEquals(anno_nodes[1].node.index, 1)
        self.assertEquals(anno_nodes[1].key, [101, 2])
        self.assertEquals(anno_nodes[2].node.index, 0)
        self.assertEquals(anno_nodes[2].key, [200, 1])
        tasks = anno_nodes[-1]
        anno_nodes = list()
        path = Path('tasks')
        for node in tasks.node.children:
            key = schema.get(path.extend(node.label))
            if not key is None:
                anno_nodes.append(AnnotatedNode(node, key=key.annotate(node)))
            else:
                anno_nodes.append(AnnotatedNode(node))
        anno_nodes = sort_nodes(anno_nodes)
        self.assertEquals(anno_nodes[0].node.index, 0)
        self.assertEquals(anno_nodes[1].node.index, 1)
        self.assertEquals(anno_nodes[2].node.index, 2)


if __name__ == '__main__':
    unittest.main()
