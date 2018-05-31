import unittest

from histore.document.base import Document
from histore.document.node import Node, InternalNode, LeafNode
from histore.path import Path


class TestDocument(unittest.TestCase):

    def test_document(self):
        """Test document initialization."""
        doc = Document()
        self.assertEquals(len(doc.nodes), 0)
        doc = Document(nodes=[InternalNode('A'), InternalNode('B')])
        self.assertEquals(len(doc.nodes), 2)
        tree = {
            'A': 1,
            'B': 2,
            'C': [
                1,
                {'E': 1, 'F': 2},
                3,
                {'E': 3, 'F': 4}
            ],
            'D': {'G': 3, 'H': 4}
        }
        doc = Document(doc=tree)
        self.assertEquals(len(doc.nodes), 7)
        counts = dict()
        leaf_nodes = 0
        c_nodes = dict()
        for node in doc.nodes:
            if not node.label in counts:
                counts[node.label] = 1
            else:
                counts[node.label] += 1
            if node.is_leaf():
                leaf_nodes += 1
            if node.label == 'C':
                c_nodes[node.index] = node
        self.assertEquals(counts['A'], 1)
        self.assertEquals(counts['B'], 1)
        self.assertEquals(counts['C'], 4)
        self.assertEquals(counts['D'], 1)
        self.assertEquals(leaf_nodes, 4)
        self.assertEquals(c_nodes[0].value, 1)
        c1 = c_nodes[1]
        self.assertEquals(c1.get('E').value, 1)
        self.assertEquals(c1.get('F').value, 2)
        self.assertEquals(c_nodes[2].value, 3)
        c3 = c_nodes[3]
        self.assertEquals(c3.get('E').value, 3)
        self.assertEquals(c3.get('F').value, 4)

    def test_get_path(self):
        """ Test .get(path) method."""
        a = InternalNode('A')
        a.add(LeafNode('B', index=0))
        a.add(LeafNode('B', index=1))
        c = a.add(InternalNode('C'))
        a.add(LeafNode('E'))
        c.add(LeafNode('D', index=0))
        c.add(LeafNode('D', index=1))
        e = c.add(InternalNode('E'))
        e.add(LeafNode('F'))
        self.assertTrue(a.get(Path('E')).label, 'E')
        self.assertTrue(a.get(Path('C/E')).label, 'E')
        self.assertTrue(a.get('C/E/F').label, 'F')
        self.assertIsNone(a.get(Path('K')))
        self.assertIsNone(a.get(Path('C/K')))
        self.assertIsNone(a.get('C/E/K'))
        with self.assertRaises(ValueError):
            a.get(Path('B'))
        with self.assertRaises(ValueError):
            a.get(Path('C/D/E'))

    def test_internal_node(self):
        """Ensure that internal nodes are initialized properly."""
        element = InternalNode('my_label')
        self.assertFalse(element.is_leaf())
        self.assertEquals(element.label, 'my_label')
        self.assertEquals(len(element.children), 0)
        self.assertEquals(element.index, None)
        element = InternalNode('my_label', children=[1, 2, 3], index=2)
        self.assertFalse(element.is_leaf())
        self.assertEquals(element.label, 'my_label')
        self.assertEquals(len(element.children), 3)
        self.assertEquals(element.index, 2)
        with self.assertRaises(ValueError):
            InternalNode(None)
        a = InternalNode('A')
        a.add(LeafNode('B', index=0))
        a.add(LeafNode('B', index=1))
        with self.assertRaises(ValueError):
            a.add(LeafNode('B'))
        a.add(LeafNode('B'), strict=False)

    def test_leaf_node(self):
        """Ensure that leaf nodes are initialized properly."""
        value = LeafNode('A', 1)
        self.assertTrue(value.is_leaf())
        self.assertEquals(value.label, 'A')
        self.assertEquals(value.value, 1)
        self.assertEquals(value.index, None)
        value = LeafNode('A', 1, index=2)
        self.assertTrue(value.is_leaf())
        self.assertEquals(value.label, 'A')
        self.assertEquals(value.value, 1)
        self.assertEquals(value.index, 2)

    def test_node(self):
        """Ensure that an exception is raised when initializing a Node with
        an invalid node type.
        """
        with self.assertRaises(ValueError):
            Node('A', 100)

    def test_node_index(self):
        """Test that only those nodes in the document have an index that are
        elements of a list.
        """
        tree = {
            'A': 1,
            'B': 2,
            'C': [
                1,
                {'E': 1, 'F': 2},
                3,
                {'E': 3, 'F': 4}
            ],
            'D': {'G': 3, 'H': 4},
            'I': [{'J': 1}]
        }
        doc = Document(doc=tree)
        self.assertEquals(len(doc.nodes), 8)
        for node in doc.nodes:
            if node.label in ['C', 'I']:
                self.assertIsNotNone(node.index)
            else:
                self.assertIsNone(node.index)


if __name__ == '__main__':
    unittest.main()
