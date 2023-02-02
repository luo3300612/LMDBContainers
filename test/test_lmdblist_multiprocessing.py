import os.path
import unittest
from lmdb_containers.lmdb_list import LMDBList
from torch.utils.data import DataLoader, Dataset
import multiprocessing as mp
import shutil


class TestDataset(Dataset):
    def __init__(self, data):
        self.data = LMDBList(data, './lmdb_test')

    def __len__(self):
        return len(self.data)

    def __getitem__(self, index):
        data = self.data[index]
        return data


class MyTestCase(unittest.TestCase):
    def test_single_process_mutli_workers(self):
        if os.path.exists('./lmdb_test'):
            shutil.rmtree('./lmdb_test')
        data = [str(i) for i in range(50)]
        dataset = TestDataset(data)
        dataloader = DataLoader(dataset, batch_size=10, num_workers=2, persistent_workers=True)
        for epoch in range(2):
            for number in dataloader:
                print('data:', number)
                # pass

        print('before last line')


if __name__ == '__main__':
    unittest.main()
