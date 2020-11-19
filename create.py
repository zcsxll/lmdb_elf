import os
import sys
import io
import lmdb
import soundfile
from tqdm import tqdm

def create(lmdb_path, files):
    env = lmdb.open(lmdb_path, map_size=1099511627776)
    txn = env.begin(write=True)
    for idx, f in enumerate(tqdm(files)):
        pcm, samplerate = soundfile.read(f)
        bio = io.BytesIO()
        soundfile.write(bio, pcm, samplerate, format='flac')
        txn.put(str(idx).encode(encoding='utf-8'), bio.getvalue())
    txn.commit()
    env.close()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage: %s <lmdb_path> <dir or list_file>' % sys.argv[0])
    elif os.path.isdir(sys.argv[2]):
        files = os.listdir(sys.argv[2])
        files = [os.path.join(sys.argv[2], f) for f in files if f.endswith('.wav')]
        create(sys.argv[1], files)
    elif os.path.isfile(sys.argv[2]):
        raise NotImplementedError()
    else:
        print('usage: %s <dir or list_file>' % sys.argv[0])
