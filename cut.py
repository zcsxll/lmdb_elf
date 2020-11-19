import os
import sys
import io
import lmdb
from tqdm import tqdm

def cur(lmdb_path_src, lmdb_path_des, cnt):
    env_src = lmdb.open(lmdb_path_src,
            readonly=True,
            lock=False,
            readahead=True,
            map_size=1099511627776)
    txn_src = env_src.begin(write=False)

    env_des = lmdb.open(lmdb_path_des, map_size=1099511627776)
    txn_des = env_des.begin(write=True)
    for i in tqdm(range(cnt)):
        data = txn_src.get('{}'.format(i).encode(encoding='utf-8'))
        if data is None:
            print('[%s] not found' % key)
            break
        txn_des.put(str(i).encode(encoding='utf-8'), data)
    env_src.close()

    txn_des.commit()
    env_des.close()

if __name__ == '__main__':
    cur(sys.argv[1], sys.argv[2], int(sys.argv[3]))
