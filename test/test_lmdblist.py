import unittest
from lmdb_containers.lmdb_list import LMDBList
from test_utils import random_string


class MyTestCase(unittest.TestCase):
    def test_intlist(self):
        raw_list = list(range(100))
        lmdb_list = LMDBList(raw_list, './lmdb_test', map_write=lambda x: str(x),
                             map_read=lambda x: int(x))

        # test length
        self.assertEqual(len(raw_list), len(lmdb_list))

        # test elements
        for e1, e2 in zip(raw_list, lmdb_list):
            self.assertEqual(e1, e2)

    def test_strlist(self):
        raw_list = [str(i) for i in list(range(100))]
        lmdb_list = LMDBList(raw_list, './lmdb_test')

        # test length
        self.assertEqual(len(raw_list), len(lmdb_list))

        # test elements
        for e1, e2 in zip(raw_list, lmdb_list):
            self.assertEqual(e1, e2)

    def test_random_strlist(self):
        N = 40
        raw_list = [random_string(N) for i in list(range(100))]
        lmdb_list = LMDBList(raw_list, './lmdb_test')

        # test length
        self.assertEqual(len(raw_list), len(lmdb_list))

        # test elements
        for e1, e2 in zip(raw_list, lmdb_list):
            self.assertEqual(e1, e2)


if __name__ == '__main__':
    unittest.main()
