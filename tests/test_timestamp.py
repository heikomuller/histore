import unittest

from histore.timestamp import TimeInterval, Timestamp


class TestTimestamp(unittest.TestCase):

    def test_interval(self):
        """Test interval initialization, containment and overlap."""
        # Test contains
        i1 = TimeInterval(start=0, end=10)
        self.assertTrue(i1.contains(interval=TimeInterval(1)))
        self.assertTrue(i1.contains(interval=TimeInterval(10)))
        self.assertTrue(i1.contains(interval=TimeInterval(start=0, end=5)))
        self.assertTrue(i1.contains(interval=TimeInterval(start=5, end=8)))
        self.assertTrue(i1.contains(interval=TimeInterval(start=9, end=10)))
        self.assertTrue(i1.contains(interval=TimeInterval(start=0, end=10)))
        self.assertFalse(i1.contains(interval=TimeInterval(start=0, end=11)))
        self.assertFalse(i1.contains(interval=TimeInterval(start=1, end=11)))
        # Test overlaps
        i1 = TimeInterval(start=10, end=20)
        self.assertFalse(i1.overlap(interval=TimeInterval(start=1, end=5)))
        self.assertTrue(i1.overlap(interval=TimeInterval(start=0, end=10)))
        self.assertTrue(i1.overlap(interval=TimeInterval(start=5, end=15)))
        self.assertTrue(i1.overlap(interval=TimeInterval(start=5, end=25)))
        self.assertTrue(i1.overlap(interval=TimeInterval(start=10, end=20)))
        self.assertTrue(i1.overlap(interval=TimeInterval(start=15, end=25)))
        self.assertTrue(i1.overlap(interval=TimeInterval(start=19, end=25)))
        self.assertTrue(i1.overlap(interval=TimeInterval(start=20, end=25)))
        self.assertFalse(i1.overlap(interval=TimeInterval(start=21, end=25)))
        # Initialize
        with self.assertRaises(ValueError):
            TimeInterval(start=10, end=9)

    def test_timestamp_append(self):
        """Test appending values to a timestamp."""
        t = Timestamp()
        t1 = t.append(1)
        self.assertTrue(t.is_empty())
        self.assertFalse(t1.is_empty())
        t2 = t1.append(2)
        t3 = t2.append(3)
        t4 = t3.append(5)
        self.assertEquals(len(t1.intervals), 1)
        self.assertEquals(len(t2.intervals), 1)
        self.assertEquals(len(t3.intervals), 1)
        self.assertEquals(len(t4.intervals), 2)
        self.assertTrue(t4.contains(1))
        self.assertTrue(t4.contains(2))
        self.assertTrue(t4.contains(3))
        self.assertFalse(t4.contains(4))
        self.assertTrue(t4.contains(5))

    def test_timestamp_contains(self):
        """Test contains function of timestamp."""
        t = Timestamp([TimeInterval(1, 5), TimeInterval(7,9), TimeInterval(14,16)])
        for value in [1,2,3,4,5,7,8,9,14,15,16]:
            self.assertTrue(t.contains(value=value))
        for value in [0,6,10,11,12,13,17,18]:
            self.assertFalse(t.contains(value=value))

    def test_timestamp_from_string(self):
        """Test generating timestamps from stings."""
        t = Timestamp.from_string('5')
        self.assertEquals(len(t.intervals), 1)
        self.assertTrue(t.contains(5))
        t = Timestamp.from_string('1,3,5')
        self.assertEquals(len(t.intervals), 3)
        for i in [1,3,5]:
            self.assertTrue(t.contains(i))
        t = Timestamp.from_string('1-3,5')
        self.assertEquals(len(t.intervals), 2)
        for i in [1,2,3,5]:
            self.assertTrue(t.contains(i))
        with self.assertRaises(ValueError):
            Timestamp.from_string('1,3--5')
            Timestamp.from_string('abc')

    def test_timestamp_init(self):
        """Test timestamp initialization."""
        t = Timestamp()
        self.assertTrue(t.is_empty())
        t = Timestamp([TimeInterval(1), TimeInterval(3)])
        self.assertFalse(t.is_empty())
        with self.assertRaises(ValueError):
            Timestamp([TimeInterval(1, 3), TimeInterval(3, 4)])
            Timestamp([TimeInterval(1, 3), TimeInterval(2, 3)])
            Timestamp([TimeInterval(-1, 3), TimeInterval(2, 3)])
            Timestamp([TimeInterval(-1)])

    def test_timestamp_is_equal(self):
        """Test contains function of timestamp."""
        t = Timestamp([TimeInterval(1,5), TimeInterval(7,9), TimeInterval(14,16)])
        t1 = Timestamp([TimeInterval(1,5), TimeInterval(7,9), TimeInterval(14,16)])
        t2 = Timestamp([TimeInterval(1,5), TimeInterval(7,9), TimeInterval(14,17)])
        t3 = Timestamp([TimeInterval(1,5), TimeInterval(7,9), TimeInterval(14,16), TimeInterval(18)])
        t4 = Timestamp([TimeInterval(1,5), TimeInterval(7,9)])
        self.assertTrue(t.is_equal(t1))
        self.assertFalse(t.is_equal(t2))
        self.assertFalse(t.is_equal(t3))
        self.assertFalse(t.is_equal(t4))

    def test_timestamp_subsets(self):
        """Test timestamp initialization."""
        t = Timestamp([TimeInterval(1, 5), TimeInterval(7,9), TimeInterval(14,16)])
        self.assertTrue(Timestamp([TimeInterval(1,5), TimeInterval(14)]).is_subset_of(t))
        self.assertTrue(Timestamp([TimeInterval(2,5), TimeInterval(8), TimeInterval(14,16)]).is_subset_of(t))
        self.assertFalse(t.is_subset_of(t))
        self.assertFalse(Timestamp([TimeInterval(0,5), TimeInterval(8), TimeInterval(14,16)]).is_subset_of(t))

    def test_timestamp_tostring(self):
        """Test timestamp string representation."""
        t = Timestamp([TimeInterval(1, 5), TimeInterval(7,9), TimeInterval(14,16)])
        self.assertEquals(str(t), '1-5,7-9,14-16')
        t = Timestamp([TimeInterval(1, 5), TimeInterval(7), TimeInterval(14,16)])
        self.assertEquals(str(t), '1-5,7,14-16')
        self.assertEquals(str(Timestamp([TimeInterval(1)])), '1')


if __name__ == '__main__':
    unittest.main()
