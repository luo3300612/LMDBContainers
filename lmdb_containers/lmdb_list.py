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

        self.env = None
        self.create_lmdb(raw_list)

    def append(self, item):
        raise NotImplementedError(f"{self.__class__} do not support online write")
        # with self.db.begin(write=True) as txn:
        #     txn.put(str(self.size).encode(), self.map_write(item))
        #     self.size += 1

    def create_lmdb(self, raw_list):
        env = lmdb.open(self.lmdb_path, map_size=1099511627776)
        with env.begin(write=True) as txn:
            for i, content in enumerate(raw_list):
                content = self.map_write(content)
                txn.put(str(i).encode(), content.encode())

    def open_lmdb(self):
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
