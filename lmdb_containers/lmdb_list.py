import lmdb
from collections.abc import Sequence
import multiprocessing as mp
from multiprocessing import Process, Manager, Value
import os
import shutil

lock = mp.RLock()


class LMDBList(Sequence):
    def __init__(self, raw_list, lmdb_path, map_write=lambda x: x,
                 map_read=lambda x: x):
        n = len(raw_list)
        self.length = n
        self.lmdb_path = lmdb_path

        self.map_write = map_write
        self.map_read = map_read

        self.env = None
        lock.acquire()  # for sync dataset in different nodes
        self.create_lmdb(raw_list)
        lock.release()

    def append(self, item):
        raise NotImplementedError(f"{self.__class__} do not support online write")
        # with self.db.begin(write=True) as txn:
        #     txn.put(str(self.size).encode(), self.map_write(item))
        #     self.size += 1

    def add_process_count(self):
        lock.acquire()
        env = lmdb.open(self.lmdb_path, map_size=1099511627776)
        with env.begin(write=True) as txn:
            n_dataset = int(txn.get('n_dataset'.encode()).decode()) + 1
            txn.put('n_dataset'.encode(), str(n_dataset).encode())
            print(f'add 1 reading process, now process: {n_dataset}')
        env.close()
        lock.release()

    def minus_process_count(self):
        lock.acquire()
        env = lmdb.open(self.lmdb_path, map_size=1099511627776)
        with env.begin(write=True) as txn:
            n_dataset = int(txn.get('n_dataset'.encode()).decode())
            if n_dataset == 0:
                raise ValueError('n_dataset is 0 before minor!!!')
            n_dataset -= 1
            txn.put('n_dataset'.encode(), str(n_dataset).encode())
            print(f'minus 1 process, now process: {n_dataset}')
        env.close()
        # if n_dataset == 0:
        #     print('no process, rm lmdb')
        #     shutil.rmtree(self.lmdb_path)
        lock.release()

    def create_lmdb(self, raw_list):
        if os.path.exists(self.lmdb_path):
            print('lmdb exist, skip creating')
            return

        print('Writing lmdb')
        env = lmdb.open(self.lmdb_path, map_size=1099511627776)
        with env.begin(write=True) as txn:
            for i, content in enumerate(raw_list):
                content = self.map_write(content)
                txn.put(str(i).encode(), content.encode())
            txn.put('n_dataset'.encode(), str(1).encode())
        env.close()

    def open_lmdb(self):
        self.add_process_count()
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

    def __del__(self):
        self.minus_process_count()
        #     print('dataset deleted, rm lmdb')
        #     shutil.rmtree(self.lmdb_path)
