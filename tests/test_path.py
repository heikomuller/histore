import unittest

from histore.path import Path


class TestPath(unittest.TestCase):

    def test_extend(self):
        """Test path extensions."""
        p = Path('A/B')
        p1 = p.extend('C')
        self.assertEquals(p.length(), 2)
        self.assertEquals(p1.length(), 3)
        self.assertEquals(p1.get(2), 'C')

    def test_match(self):
        """Test path matching."""
        p = Path('A/B/C')
        self.assertTrue(p.matches(Path(['A', 'B', 'C'])))
        self.assertFalse(p.matches(Path(['A', 'B'])))
        self.assertFalse(p.matches(Path(['A', 'B', 'E'])))

    def test_path(self):
        """Test Path initialization."""
        self.validate_path(Path('A/B/C'))
        self.validate_path(Path(['A', 'B', 'C']))
        with self.assertRaises(ValueError):
            Path(['A', 'B/C'])

    def validate_path(self, p):
        self.assertEquals(p.length(), 3)
        self.assertEquals(p.get(0), 'A')
        self.assertEquals(p.first_element(), 'A')
        self.assertEquals(p.get(1), 'B')
        self.assertEquals(p.get(2), 'C')
        self.assertFalse(p.is_empty())
        p2 = p.subpath()
        self.assertEquals(p2.length(), 2)
        self.assertEquals(p2.get(0), 'B')
        self.assertEquals(p2.first_element(), 'B')
        self.assertEquals(p2.get(1), 'C')
        self.assertFalse(p2.is_empty())
        p3 = p2.subpath()
        self.assertEquals(p3.length(), 1)
        self.assertEquals(p3.get(0), 'C')
        self.assertEquals(p3.first_element(), 'C')
        self.assertFalse(p3.is_empty())
        p4 = p3.subpath()
        self.assertEquals(p4.length(), 0)
        self.assertTrue(p4.is_empty())


if __name__ == '__main__':
    unittest.main()
