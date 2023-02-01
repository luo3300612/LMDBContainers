import unittest
from lmdb_containers.lmdb_list import LMDBList
import time
from test_utils import random_string


class MyTestCase(unittest.TestCase):
    def test_intlist(self):
        print('=' * 50)
        print(f'Test: intlist')
        raw_list = list(range(1000000))
        lmdb_list = LMDBList(raw_list, './lmdb_test', map_write=lambda x: str(x),
                             map_read=lambda x: int(x))

        # test iter speed
        start = time.time()
        for e in raw_list:
            pass
        end = time.time()
        print(f'Time consume {end - start}s for python list')
        print(f'{len(raw_list) / (end - start)} elements per sec')  # 1ww

        # test iter speed
        start = time.time()
        for e in lmdb_list:
            pass
        end = time.time()
        print(f'Time consume {end - start}s for lmdb_list')
        print(f'{len(lmdb_list) / (end - start)} elements per sec')  # 70w

    def test_strlist(self):
        print('=' * 50)
        print(f'Test: strlist')
        raw_list = [str(i) for i in list(range(1000000))]
        lmdb_list = LMDBList(raw_list, './lmdb_test')

        # test iter speed
        start = time.time()
        for e in raw_list:
            pass
        end = time.time()
        print(f'Time consume {end - start}s for python list')
        print(f'{len(raw_list) / (end - start)} elements per sec')  # 1ww

        # test iter speed
        start = time.time()
        for e in lmdb_list:
            pass
        end = time.time()
        print(f'Time consume {end - start}s for lmdb_list')
        print(f'{len(lmdb_list) / (end - start)} elements per sec')  # 70w

    def test_longstrlist(self):
        print('=' * 50)
        print(f'Test: longstrlist')
        # test build speed
        start = time.time()
        raw_list = [f'{random_string(68)}\t正常' for i in list(range(10000000))]
        end = time.time()
        print(f'Time consume {end - start}s for build python list')
        print(f'{len(raw_list) / (end - start)} elements per sec')

        start = time.time()
        lmdb_list = LMDBList(raw_list, './lmdb_test')
        end = time.time()
        print(f'Time consume {end - start}s for build lmdb list')
        print(f'{len(lmdb_list) / (end - start)} elements per sec')

        # test iter speed
        start = time.time()
        for e in raw_list:
            pass
        end = time.time()
        print(f'Time consume {end - start}s for python list')
        print(f'{len(raw_list) / (end - start)} elements per sec')  # 1ww

        # test iter speed
        start = time.time()
        for e in lmdb_list:
            pass
        end = time.time()
        print(f'Time consume {end - start}s for lmdb_list')
        print(f'{len(lmdb_list) / (end - start)} elements per sec')  # 70w


if __name__ == '__main__':
    unittest.main()
