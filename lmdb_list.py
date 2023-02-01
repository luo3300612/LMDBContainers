import lmdb
from collections.abc import Sequence


class LMDBList(Sequence):
    def __init__(self, raw_list, lmdb_path, map_write=lambda x: x,
                 map_read=lambda x: x):
        n = len(raw_list)
        self.length = n
        self.lmdb_path = lmdb_path

        self.map_write = map_write
        self.map_read = map_read

    def append(self, item):
        raise NotImplementedError(f"{self.__class__} do not support online write")
        # with self.db.begin(write=True) as txn:
        #     txn.put(str(self.size).encode(), self.map_write(item))
        #     self.size += 1

    def create_lmdb(self, raw_list):
        env = lmdb.open(self.lmdb_path, map_size=1099511627776)
        txn = env.begin(write=True)
        for i, content in enumerate(raw_list):
            string = self.map_write(content)
            txn.put(str(i).encode(), content.encode())

    def open(self):
        self.db = lmdb.open(self.lmdb_path, map_size=1099511627776)

    def close(self):
        self.db.close()

    def __len__(self):
        return self.length

    def __iter__(self):
        for i in range(self.size):
            yield self[i]

    def __getitem__(self, index):
        with self.db.begin() as txn:
            data = txn.get(str(index).encode())
        data = self.map_read(data)
        return data


class LmdbList():
    def __init__(self):
        self.db = lmdb.open('list', map_size=1099511627776)
        self.size = 0

    def __len__(self):
        return self.size

    def __getitem__(self, index):
        with self.db.begin() as txn:
            data = txn.get(str(index).encode())
            return pickle.loads(data)

    def __iter__(self):
        for i in range(self.size):
            yield self[i]

    def close(self):
        self.db.close()
