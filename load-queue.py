import argparse
from datetime import datetime
import multiprocessing
import orjson as json
import os
import sqlite3
import sys

import zst


conn = sqlite3.connect(sys.argv[1])
conn.execute('PRAGMA page_size = 32768;')
conn.execute('PRAGMA journal_mode = off;')
conn.execute('CREATE TABLE IF NOT EXISTS submissions (sub TEXT, time INTEGER, author TEXT, body BLOB)')



def read(queue, file_, sub=None, file_size=None):
    global file_lines
    global bad_lines
    global bytes_processed
    accumulated = [ ]

    for i, (line, file_bytes_processed) in enumerate(zst.read_lines_zst(file_)):
        file_lines += 1
        accumulated.append((sub, line))
        if file_lines % 100000 == 0:
            #created = datetime.utcfromtimestamp(int(obj['created_utc']))
            print(f"{sub} "
                  #f"{created.strftime('%Y-%m-%d %H:%M:%S')} : "
                  f"{file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}: "
                  f"{(file_bytes_processed / file_size) * 100:.0f}% "
                  f"({((file_bytes_processed + bytes_processed) / bytes_total) * 100:.0f}%) "
                  f"({queue.qsize()})"
                  )
            queue.put(accumulated)
            accumulated = [ ]
    queue.put(accumulated)
    print("done with file")
    bytes_processed += file_bytes_processed
    return

def decode(queue_in, queue_out):
    accumulated = [ ]
    while True:
        x = queue_in.get()
        #print(x)
        print(f'    decoding ({queue_in.qsize()} waiting)')
        if x == 'DONE':
            print('d-breaking')
            queue_out.put('DONE')
            break

        for sub, line in x:
            try:
                obj = json.loads(line)
            except (KeyError, json.JSONDecodeError) as err:
                bad_lines += 1
            accumulated.append((sub, obj['created_utc'], obj['author'], line))
        queue_out.put(accumulated)
        accumulated = [ ]
    queue_out.put(accumulated)
    accumulated = [ ]



def insert(queue):
    print('i0')
    def get():
        while True:
            x = queue.get()
            if x == 'DONE':
                print('i-breaking')
                break
            print(f'        inserting ({queue.qsize()} waiting)')
            #print(x)
            yield from x

    print('i1')
    conn.executemany('INSERT INTO submissions VALUES(?, ?, ?, ?)', get())
    print('i2')


parser = argparse.ArgumentParser()
parser.add_argument('db')
parser.add_argument('files', nargs='+')
args = parser.parse_args()


bytes_processed = 0
bytes_total = sum(os.stat(file_).st_size for file_ in args.files)
file_lines = 0
bad_lines = 0

queue1 = multiprocessing.Queue(maxsize=50)
queue2 = multiprocessing.Queue(maxsize=50)

# start inserting process
p_insert = multiprocessing.Process(target=insert, args=(queue2,))
p_insert.start()

# start decoding process
p_decode = multiprocessing.Process(target=decode, args=(queue1, queue2,))
p_decode.start()


for file_ in args.files:

    sub = os.path.basename(file_).rsplit('_', 1)[0]
    file_size = os.stat(file_).st_size

    p_read = multiprocessing.Process(target=read, args=(queue1, file_, sub, file_size))
    p_read.start()
    print('a')
    p_read.join()
    print('b')
    queue1.put('DONE')
    print('c')
    p_decode.join()
    print('d')
    p_insert.join()
    print('e')


    #conn.executemany('INSERT INTO submissions VALUES(?, ?, ?, ?)', iter_data(file_, sub=sub, file_size=file_size))

