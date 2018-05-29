import unittest

from histore.archive.serialize import ArchiveSerializer, LABEL_META, LABEL_VALUE


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


if __name__ == '__main__':
    unittest.main()
