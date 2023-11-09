import argparse
import ctypes
from datetime import datetime
import glob
import logging
import multiprocessing
import os
import sqlite3
import sys
import time

import orjson as json
import zst


# Arguments
parser = argparse.ArgumentParser()
parser.add_argument('db')
parser.add_argument('files', nargs='+')
parser.add_argument('--readers', type=int, default=1, help="num reader processes")
parser.add_argument('--decoders', type=int, default=1, help="num decoder processes")
parser.add_argument('--comments', action='store_true', help="process comments files")
parser.add_argument('--index', action='store_true', help="Do nothing but create indexes")
parser.add_argument('--thin', action='store_true', help="Insert fewer columns")
args = parser.parse_args()
args.files = sum((glob.glob(f) for f in args.files), [])
#print(args.files[:5])

logger_root = logging.getLogger('')
fh = logging.FileHandler('load-queue.log')
fh.setLevel(logging.INFO)
logger_root.addHandler(fh)
log = logging.getLogger(__name__)

columns_submissions = [
    ('subreddit', 'TEXT'),
    ('id', 'TEXT', lambda x: 't3_'+x['id']),
    ('created_utc', 'INTEGER'),
    ('author', 'TEXT'),
    ('is_self', 'INTEGER'),
    ('title', 'TEXT'),
    ('score', 'INTEGER'),
    ('num_comments', 'INTEGER'),
]
if not args.thin:
    columns_submissions.extend([
    ('subreddit_id', 'TEXT'),
    ('hidden', 'INTEGER'),
    ('domain', 'TEXT'),
    ('over_18', 'INTEGER'),
    ('url', 'TEXT'),
    ('selftext', 'TEXT'),
    ])

columns_comments = [
    ('subreddit', 'TEXT'),
    ('author', 'TEXT'),
    ('id', 'TEXT', lambda x: 't1_'+x['id']),
    ('link_id', 'TEXT'),
    ('created_utc', 'INTEGER'),
    ('score', 'INTEGER'),
    ]
if not args.thin:
    columns_submissions.extend([
    ('parent_id', 'TEXT'),
    ('controversiality', 'INTEGER'),
    ('distinguished', 'INTEGER'),
    ('ups', ''),
    ('downs', 'INTEGER'),
    ('gilded', 'INTEGER'),
    ('score_hidden', 'INTEGER'),
    ('subreddit_id', 'TEXT'),
    ('name', 'TEXT'),
    ('body', 'TEXT'),
    ])



# Hackish way to select if we import submissions or comments
TABLE = 'submissions'
COLUMNS = columns_submissions
if args.comments:
    TABLE = 'comments'
    COLUMNS = columns_comments


# Open and set up database
conn = sqlite3.connect(args.db)
conn.execute(f'PRAGMA page_size = 32768;')
conn.execute(f'PRAGMA mmap_size = {200 * 2**30}')
conn.execute(f'PRAGMA journal_mode = off;') # or WAL
conn.commit()


# --index: don't do anything else, but make indexes
if args.index:
    indexes = [
        ('submissions', 'subreddit, created_utc'),
        ('submissions', 'subreddit, author'),
        ('submissions', 'id'),
        ('submissions', 'author'),
        ('comments', 'subreddit, created_utc'),
        ('comments', 'subreddit, author'),
        ('comments', 'subreddit, author, score'),
        ('comments', 'author'),
        ('comments', 'id'),
        ('comments', 'link_id'),
        ('comments', 'parent_id'),
        ]

    conn.execute('PRAGMA journal_mode = WAL;') # or WAL
    for i, (table, cols) in enumerate(indexes):
        name = '_'.join(x[:3] for x in cols.split(', '))
        cmd = f"CREATE INDEX IF NOT EXISTS idx_{table[:3]}_{name} ON {table} ({cols})"
        print(cmd)
        conn.execute(cmd)
        conn.commit()
    print("ANALYZE;")
    conn.execute("ANALYZE;")
    conn.commit()
    exit(0)



# Make columns, etc.
#conn.execute('CREATE TABLE IF NOT EXISTS submissions (sub TEXT, time INTEGER, author TEXT, body BLOB)')
conn.execute(f'CREATE TABLE IF NOT EXISTS {TABLE} ('
             f'{", ".join(" ".join(x[:2]) for x in COLUMNS)}'
             f')')
conn.commit()

# Store the history of this table
conn.execute(f'CREATE TABLE IF NOT EXISTS history ('
             f'time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, command TEXT'
             f')')
conn.commit()
conn.execute("INSERT INTO history (command) VALUES (?)", (json.dumps(sys.argv), ))
conn.commit()


class Averager:
    def __init__(self, alpha=.01):
        self.n = multiprocessing.Value(ctypes.c_long, 0)
        self.a = multiprocessing.Value(ctypes.c_double, 0)
    def add(self, x):
        with self.n.get_lock():
            self.n.value += 1
            with self.a.get_lock():
                if self.n == 1:
                    self.a.value = x
                else:
                    self.a.value = x*alpha + self.a*(1-alpha)
time_read = Averager()
time_decode = Averager()
time_insert = Averager()



def read(file_):
    """Read and decompress lines from file, put in queue
    """
    queue = queue1
    file_size = os.stat(file_).st_size
    sub = os.path.basename(file_).rsplit('_', 1)[0]
    lines_file = 0
    accumulated = [ ]

    #print(f"read: starting {file_}")
    for i, (line, file_bytes_processed) in enumerate(zst.read_lines_zst(file_)):
        lines_file += 1
        accumulated.append((i, line))
        # Every 100000 lines, print status and push into queue.
        if lines_file % 100000 == 0:
            #created = datetime.utcfromtimestamp(int(obj['created_utc']))
            print(f"{sub} "
                  #f"{created.strftime('%Y-%m-%d %H:%M:%S')} : "
                  f"{lines_file:,} ln : {lines_total.value:,} ln tot : {lines_bad.value:,} ln bad : {file_bytes_processed:,} B: "
                  f"{(file_bytes_processed / file_size) * 100:.0f}% "
                  f"({((file_bytes_processed + bytes_processed.value) / bytes_total) * 100:.0f}%) "
                  f"({queue1.qsize()}, {queue2.qsize()}) "
                  f"({n_decode.value/runtime():3.1f}/s, {n_insert.value/runtime():3.1f}/s)"
                  )
            queue.put((file_, accumulated))
            accumulated = [ ]
    # Put all the last stuff into queue
    queue.put((file_, accumulated))
    with bytes_processed.get_lock():
        bytes_processed.value += file_bytes_processed
    with lines_total.get_lock():
        lines_total.value += lines_file
    #print(f"read: done with {file_}")
    #queue.close()


def decode(queue_in, queue_out):
    """Read from queue, decode JSON and make fields, add to next queue"""
    accumulated = [ ]
    # While there is stuff in the queue...
    while True:
        x = queue_in.get()
        # This is our sentinel to end processing.  It seems the queue
        # should raise ValueError once closed, but I haven't gotten
        # that to work.  Maybe it needs to be closed in every process.
        if x == 'DONE':
            print(' '*7, 'decode: done')
            break

        # For each line, load JSON and accumulate whatever our final
        # values will be.
        file_, lines = x
        print(f' '*7, f'decode: {len(lines)} ({queue_in.qsize()} waiting)')
        for lineno, line in lines:
            try:
                obj = json.loads(line)
            except (KeyError, json.JSONDecodeError) as err:
                with lines_bad.get_lock():
                    lines_bad.value += 1
                log.warning("bad line: %s: %s", file_, lineno)

            db_row = tuple(col[2](obj)  if  len(col)>2   else   obj.get(col[0], None)
                           for col in COLUMNS)
            accumulated.append(db_row)

        with n_decode.get_lock():
            n_decode.value += 1

        queue_out.put(accumulated)
        accumulated = [ ]
    # Don't forget to push the final stuff through.
    queue_out.put(accumulated)



INSERT = f'INSERT INTO {TABLE} VALUES({",".join(["?"]*len(COLUMNS))})'
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
            with n_insert.get_lock():
                n_insert.value += 1
            yield from x

    conn.executemany(INSERT, get())
    conn.commit()



# Verify that --comments is used for comment files and vice versa
if args.comments:
    assert all(x.endswith('_comments.zst') for x in args.files), "--comments but not all files end in _comments.zst"
else:
    assert all(x.endswith('_submissions.zst') for x in args.files), "not all files end in _submissions.zst"


# Status variables for our progress

bytes_total = sum(os.stat(file_).st_size for file_ in args.files)
bytes_processed = multiprocessing.Value(ctypes.c_long, 0)
lines_total = multiprocessing.Value(ctypes.c_long, 0)
lines_bad = multiprocessing.Value(ctypes.c_long, 0)
n_decode = multiprocessing.Value(ctypes.c_long, 0)
n_insert = multiprocessing.Value(ctypes.c_long, 0)
start = time.time()
def runtime():
    return time.time() - start

# Queues
queue1 = multiprocessing.Queue(maxsize=50)
queue2 = multiprocessing.Queue(maxsize=50)

# start decoding process
decode_ps = [ multiprocessing.Process(target=decode, args=(queue1, queue2,)) for _ in range(args.decoders) ]
for p in decode_ps:
    p.start()

# start inserting process
insert_p = multiprocessing.Process(target=insert, args=(queue2,))
insert_p.start()


# For every file, via multiprocessing.Pool
#p_read = multiprocessing.Process(target=read, args=(queue1, file_))
read_pool = multiprocessing.Pool(processes=args.readers)
read_pool.map(read, args.files)
read_pool.close()
read_pool.join()
print("reading: done")

# Close all decoders
for _ in range(args.decoders):
    queue1.put('DONE')
queue1.close()
for p in decode_ps:
    p.join()

# Close all joiners
queue2.put('DONE')
insert_p.join()

conn.execute('PRAGMA journal_mode = WAL;')
