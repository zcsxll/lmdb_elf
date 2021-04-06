import os
import sys
import io
import lmdb
import soundfile
import numpy as np

def peek(lmdb_path, key):
    env = lmdb.open(lmdb_path,
            readonly=True,
            lock=False,
            readahead=True,
            map_size=1099511627776)
    txn = env.begin(write=False)
    data = txn.get('{}'.format(key).encode(encoding='utf-8'))
    if data is None:
        print('[%s] not found' % key)
        return
    bio = io.BytesIO(data)
    sf = soundfile.SoundFile(bio)
    print(sf.samplerate)
    pcm = sf.read()
    print(pcm.shape)
    soundfile.write('./peek.wav', pcm, 16000)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage: %s <lmdb dir> <key to lmdb>' % sys.argv[0])
    else:
        try:
            peek(sys.argv[1], sys.argv[2])
        except Exception as e:
            print(e)
            raise(e)
