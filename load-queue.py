import argparse
from collections import namedtuple
from datetime import datetime
import multiprocessing
import orjson as json
import os
import sqlite3
import sys

import zst

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument('db')
parser.add_argument('files', nargs='+')
args = parser.parse_args()


# Open and set up database
conn = sqlite3.connect(args.db)
conn.execute('PRAGMA page_size = 32768;')
conn.execute('PRAGMA journal_mode = off;') # or WAL


# Make columns, etc.
conn.execute('CREATE TABLE IF NOT EXISTS submissions (sub TEXT, time INTEGER, author TEXT, body BLOB)')


Message1 = namedtuple('Message1', ('sub', 'line'))
Message2 = namedtuple('Message2', ('sub', 'created_utc', 'author', 'line'))



def read(queue, file_, sub=None, file_size=None):
    """Read and decompress lines from file, put in queue
    """
    global file_lines
    global bad_lines
    global bytes_processed
    accumulated = [ ]

    print(f"read: starting {file_}")
    for i, (line, file_bytes_processed) in enumerate(zst.read_lines_zst(file_)):
        file_lines += 1
        accumulated.append((sub, line))
        # Every 100000 lines, print status and push into queue.
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
    # Put all the last stuff into queue
    queue.put(accumulated)
    bytes_processed += file_bytes_processed
    print("read: done with {file_}")



def decode(queue_in, queue_out):
    """Read from queue, decode JSON and make fields, add to next queue"""
    accumulated = [ ]
    # While there is stuff in the queue...
    while True:
        x = queue_in.get()
        print(f' '*7, f'decode: ({queue_in.qsize()} waiting)')
        # This is our sentinel to end processing.  It seems the queue
        # should raise ValueError once closed, but I haven't gotten
        # that to work.  Maybe it needs to be closed in every process.
        if x == 'DONE':
            print(' '*7, 'decode: done')
            break

        # For each line, load JSON and accumulate whatever our final
        # values will be.
        for sub, line in x:
            try:
                obj = json.loads(line)
            except (KeyError, json.JSONDecodeError) as err:
                bad_lines += 1
            accumulated.append((sub, obj['created_utc'], obj['author'], line))
        queue_out.put(accumulated)
        accumulated = [ ]
    # Don't forget to push the final stuff through.
    queue_out.put(accumulated)
    accumulated = [ ]
    queue_out.put('DONE')



def insert(queue):
    """Read from queue and insert into the database"""
    def get():
        """Generator to indefinitely return stuff to insert into the database"""
        while True:
            x = queue.get()
            if x == 'DONE':
                print(' '*15, 'insert: done')
                break
            print(f' '*15, f'insert ({queue.qsize()} waiting)')
            yield from x

    conn.executemany('INSERT INTO submissions VALUES(?, ?, ?, ?)', get())


# Status variables for our progress
bytes_processed = 0
bytes_total = sum(os.stat(file_).st_size for file_ in args.files)
file_lines = 0
bad_lines = 0

# Queues
queue1 = multiprocessing.Queue(maxsize=50)
queue2 = multiprocessing.Queue(maxsize=50)

# start inserting process
p_insert = multiprocessing.Process(target=insert, args=(queue2,))
p_insert.start()

# start decoding process
p_decode = multiprocessing.Process(target=decode, args=(queue1, queue2,))
p_decode.start()


# For every file
for file_ in args.files:

    sub = os.path.basename(file_).rsplit('_', 1)[0]
    file_size = os.stat(file_).st_size

    p_read = multiprocessing.Process(target=read, args=(queue1, file_, sub, file_size))
    p_read.start()
    p_read.join()
    print("reading: done")
    queue1.put('DONE')
    p_decode.join()
    p_insert.join()



#conn.execute('PRAGMA journal_mode = WAL;')
