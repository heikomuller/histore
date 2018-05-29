import unittest

from histore.path import Path
from histore.schema import DocumentSchema, KeySpec


class TestPath(unittest.TestCase):

    def test_match(self):
        """Test path matching."""
        key = KeySpec(target_path=Path('A/B/C'))
        self.assertTrue(key.matches(Path(['A', 'B', 'C'])))
        self.assertFalse(key.matches(Path(['A', 'B'])))
        self.assertFalse(key.matches(Path(['A', 'B', 'E'])))

    def test_schema(self):
        """Test document schema functinality."""
        schema = DocumentSchema()
        schema.add(KeySpec(Path('A/B')))
        schema.add(KeySpec(Path('A/C')))
        self.validate_schema(schema)
        self.validate_schema(
            DocumentSchema([KeySpec(Path('A/B')), KeySpec(Path('A/C'))])
        )
        with self.assertRaises(ValueError):
            DocumentSchema([
                KeySpec(Path('A/B')),
                KeySpec(Path('A/C')),
                KeySpec(Path('A/C'))
            ])

    def validate_schema(self, schema):
        key = schema.get(Path('A/C'))
        self.assertEquals(key.target_path.length(), 2)
        self.assertEquals(key.target_path.get(1), 'C')
        self.assertIsNone(schema.get(Path('A')))
        with self.assertRaises(ValueError):
            schema.add(KeySpec(Path('A/C')))


if __name__ == '__main__':
    unittest.main()
