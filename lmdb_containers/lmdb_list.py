import lmdb
from collections.abc import Sequence
from collections import Iterable
import multiprocessing as mp
from multiprocessing import Process, Manager, Value
import os
import shutil
from time import sleep


class LMDBList(Sequence):
    r"""
    List `cached' by LMDB. LMDBList consumes little memory but is expected
     to be used like a regular python list.

    Sometimes, we have a very large list in our memory, which may cause
    OOM problem. In LMDBList, we solve the memory problem at the cost
    of performance.

    Arguments:
        raw_list (Iterable): list (anything iterable) to be cached,
        lmdb_path (str): path to save the lmdb,
        map_write (callable, optional): Since both the key&value in lmdb
            should be strings, we need to map the element in `raw_list` to a
            string.
        map_read (callable, optional): Map function when we call lmdblist[idx]
            to read an element from the LMDBList. Typically, the following
            equation holds:
                x == map_read(map_write(x))

    .. warning:: I do not implement `append` and `extend`. I can do this but
        I think it is unnecessary currently. LMDBList is designed as a read-
        only list at the most of the time.
    .. warning::
    """

    def __init__(self, raw_list, lmdb_path, map_write=lambda x: x,
                 map_read=lambda x: x):
        self.length = None
        self.lmdb_path = lmdb_path

        self.map_write = map_write
        self.map_read = map_read

        self.env = None

    def append(self, item):
        raise NotImplementedError(f"{self.__class__} do not support online writing")
        # with self.db.begin(write=True) as txn:
        #     txn.put(str(self.size).encode(), self.map_write(item))
        #     self.size += 1

    def create_lmdb(self, raw_list):
        env = lmdb.open(self.lmdb_path, map_size=1099511627776)
        with env.begin(write=True) as txn:
            n = 0
            for i, content in enumerate(raw_list):
                content = self.map_write(content)
                txn.put(str(i).encode(), content.encode())
                n += 1
        self.length = n
        env.close()

    def open_lmdb(self):
        # self.add_process_count()
        self.env = lmdb.open(self.lmdb_path, map_size=1099511627776, lock=False, readonly=True, readahead=False,
                             meminit=False, max_readers=512)

    def close_lmdb(self):
        self.env.close()
        self.env = None

    def __len__(self):
        return self.length

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, index):
        if self.env is None:
            self.open_lmdb()
        with self.env.begin() as txn:
            data = txn.get(str(index).encode()).decode()
        # self.close_lmdb()
        data = self.map_read(data)
        return data

    # def __del__(self):
    #     self.minus_process_count()
    #     print('dataset deleted, rm lmdb')
    #     shutil.rmtree(self.lmdb_path)


class LMDBListMP(Sequence):
    def __init__(self, raw_list, lmdb_path, map_write=lambda x: x,
                 map_read=lambda x: x, is_main=True):
        # raw list: anything iterable
        n = len(raw_list)  # cache a list

        self.length = n
        self.lmdb_path = lmdb_path

        self.map_write = map_write
        self.map_read = map_read

        self.env = None
        if is_main:
            print('main process writing lmdb')
            self.create_lmdb(raw_list)
        else:
            print('not main process skip writing')

    def append(self, item):
        raise NotImplementedError(f"{self.__class__} do not support online writing")
        # with self.db.begin(write=True) as txn:
        #     txn.put(str(self.size).encode(), self.map_write(item))
        #     self.size += 1

    def create_lmdb(self, raw_list):
        env = lmdb.open(self.lmdb_path, map_size=1099511627776)
        with env.begin(write=True) as txn:
            for i, content in enumerate(raw_list):
                content = self.map_write(content)
                txn.put(str(i).encode(), content.encode())
            txn.put('n_dataset'.encode(), str(1).encode())
        env.close()

    def open_lmdb(self):
        # self.add_process_count()
        self.env = lmdb.open(self.lmdb_path, map_size=1099511627776, lock=False, readonly=True, readahead=False,
                             meminit=False, max_readers=512)

    def close_lmdb(self):
        self.env.close()
        self.env = None

    def __len__(self):
        return self.length

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, index):
        if self.env is None:
            self.open_lmdb()
        with self.env.begin() as txn:
            data = txn.get(str(index).encode()).decode()
        # self.close_lmdb()
        data = self.map_read(data)
        return data

    # def __del__(self):
    #     self.minus_process_count()
    #     print('dataset deleted, rm lmdb')
    #     shutil.rmtree(self.lmdb_path)
