import os
import sys
import io
import lmdb
import soundfile
import numpy as np

def get_total(lmdb_path):
    env = lmdb.open(lmdb_path,
            readonly=True,
            lock=False,
            readahead=False,
            meminit=False,
            map_size=1099511627776)
    n = env.stat()['entries']
    #samples = [['lmdb', source, i] for i in range(n)]
    env.close()

    return n#, samples

def peek(lmdb_path, count, max_key = 500):
    env = lmdb.open(lmdb_path,
            readonly=True,
            lock=False,
            readahead=True,
            map_size=1099511627776)
    txn = env.begin(write=False)

    tmin = 1000
    tmax = 0
    tavg = 0
    for key in range(count):
        data = txn.get('{}'.format(key).encode(encoding='utf-8'))
        if data is None:
            print('[%s] not found' % key)
            break
        bio = io.BytesIO(data)
        sf = soundfile.SoundFile(bio)
        t = len(sf.read()) / 16000
        tavg += t
        tmin = min(tmin, t)
        tmax = max(tmax, t)
        #print(sf.samplerate, len(sf.read()) / 16000)
        print(key, tmin, tmax, tavg / (key + 1))

        if key > max_key:
            break

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: %s <lmdb dir>' % sys.argv[0])
        sys.exit()
    try:
        total = get_total(sys.argv[1])
        print(total)
        peek(sys.argv[1], total)
    except Exception as e:
        print(e)
        raise e
