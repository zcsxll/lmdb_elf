import os
import sys
import io
import lmdb
import soundfile
from tqdm import tqdm

def create(lmdb_name, files):
    env = lmdb.open(lmdb_name, map_size=1099511627776)
    txn = env.begin(write=True)
    for idx, f in enumerate(tqdm(files)):
        pcm, samplerate = soundfile.read(f)
        #print(pcm.shape)
        bio = io.BytesIO()
        soundfile.write(bio, pcm, samplerate, format='flac')
        txn.put(str(idx).encode(encoding='utf-8'), bio.getvalue())
    txn.commit()
    env.close()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: %s <dir or list_file>' % sys.argv[0])
        sys.exit()
    full_path = sys.argv[1]
    if full_path.endswith('/'): #如果输入的是路径，且最后是/，则basename是‘’
        full_path = full_path.strip('/')
    path, basename = os.path.split(full_path)
    basename = os.path.splitext(basename)[0]
    assert len(basename) > 0
    lmdb_name = '.'.join((basename, 'lmdb'))
    if os.path.exists(lmdb_name):
        print('%s already exists' % lmdb_name)
        sys.exit()

    if os.path.isdir(sys.argv[1]):
        files = os.listdir(sys.argv[1])
        files = [os.path.join(sys.argv[1], f) for f in files if f.endswith('.wav')]
        create(lmdb_name, files)
    elif os.path.isfile(sys.argv[1]):
        with open(sys.argv[1], 'r') as fp:
            files = fp.read().split()
        create(lmdb_name, files)
    else:
        print('usage: %s <dir or list_file>' % sys.argv[0])
