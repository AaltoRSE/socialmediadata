import argparse
import ctypes
from datetime import datetime
import glob
import itertools
import logging
import multiprocessing
import os
import re
import sqlite3
import sys
import time

import orjson as json
import zst


# default CPUs
cpus = 1
if 'SLURM_CPUS_PER_TASK' in os.environ:
    cpus = int(os.environ['SLURM_CPUS_PER_TASK'])

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument('db')
parser.add_argument('files', nargs='+')
parser.add_argument('--sub-list', type=str, help="FILES is base and this argument is list of subs to add to the FILES argument.")
parser.add_argument('--readers', type=int, default=(cpus//5)+1, help="num reader processes")
parser.add_argument('--decoders', type=int, default=((3*cpus)//4)+1, help="num decoder processes")
parser.add_argument('--chunk-lines', type=int, default=100000, help="Lines per each chuck: how often things are inserted")
parser.add_argument('--print-every', type=int, default=1, help="How often to print status")
parser.add_argument('--insert-batch', type=int, default=10, help="Runs inserts every this many chunks")
parser.add_argument('--comments', action='store_true', help="process comments files")
parser.add_argument('--index', action='store_true', help="Do nothing but create indexes")
parser.add_argument('--thin', action='store_true', help="Insert fewer columns")
args = parser.parse_args()
args.files = sum((glob.glob(f) for f in args.files), [])
#print(args.files[:5])
print(f"Readers: {args.readers}, Decoders: {args.decoders}")

logger_root = logging.getLogger('')
fh = logging.FileHandler('load-queue.log')
fh.setLevel(logging.INFO)
logger_root.addHandler(fh)
log = logging.getLogger(__name__)

columns_submissions = [
    ('subreddit', 'TEXT'), # important
    ('id', 'TEXT', lambda x: 't3_'+x['id']), # important
    ('created_utc', 'INTEGER'), # important
    ('author', 'TEXT'), # important
    ('is_self', 'INTEGER'), # important
    ('title', 'TEXT'), # important
    ('score', 'INTEGER'), # important
    ('num_comments', 'INTEGER'), # important
]
if not args.thin:
    columns_submissions.extend([
    #('subreddit_id', 'TEXT'),
    ('hidden', 'INTEGER'),
    #('domain', 'TEXT'),
    ('over_18', 'INTEGER'), # important
    ('url', 'TEXT'), # important
    ('selftext', 'TEXT'), # important
    ('author_flair_text', 'TEXT')
    ])

columns_comments = [
    ('subreddit', 'TEXT'), # important
    ('author', 'TEXT'), # important
    ('id', 'TEXT', lambda x: 't1_'+x['id']), # important
    ('link_id', 'TEXT'), # important
    ('created_utc', 'INTEGER'), # important
    ('score', 'INTEGER'), # important
    ]
if not args.thin:
    columns_comments.extend([
    ('parent_id', 'TEXT'), # important
    ('controversiality', 'INTEGER'), # important
    ('distinguished', 'INTEGER'), # important
    #('ups', ''),
    #('downs', 'INTEGER'),
    ('gilded', 'INTEGER'), # important
    #('score_hidden', 'INTEGER'),
    #('subreddit_id', 'TEXT'),
    #('name', 'TEXT'),
    ('body', 'TEXT'), # important
    ('author_flair_text', 'TEXT')
    ])



# Hackish way to select if we import submissions or comments
TABLE = 'submissions'
COLUMNS = columns_submissions
TYPE = 'S'
if args.comments:
    TABLE = 'comments'
    COLUMNS = columns_comments
    TYPE = 'C'


# Open and set up database
conn = sqlite3.connect(args.db)
conn.execute(f'PRAGMA page_size = 16384;')
conn.execute(f'PRAGMA mmap_size = {2 * 2**30}')
conn.execute(f'PRAGMA journal_mode = wal;') # or WAL
conn.commit()


# --index: don't do anything else, but make indexes
if args.index:
    indexes = [
        ('submissions', 'subreddit, created_utc'),
        ('submissions', 'subreddit, author'),
        ('submissions', 'created_utc'),
        ('submissions', 'id'),
        ('submissions', 'author'),
        ('comments', 'subreddit, created_utc'),
        ('comments', 'subreddit, author'),
        ('comments', 'subreddit, author, score'),
        ('comments', 'created_utc'),
        ('comments', 'author'),
        ('comments', 'id'),
        ('comments', 'link_id'),
        ]
    if not args.thin:
        indexes.extend([
        ('comments', 'parent_id'),
        ])

    #conn.execute('PRAGMA journal_mode = WAL;') # or WAL
    for i, (table, cols) in enumerate(indexes):
        name = '_'.join(x[:3] for x in cols.split(', '))
        cmd = f"CREATE INDEX IF NOT EXISTS idx_{table[:3]}_{name} ON {table} ({cols})"
        print(cmd, flush=True)
        conn.execute(cmd)
        conn.commit()
    print("ANALYZE;", flush=True)
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
        self.alpha = alpha
        self.n = multiprocessing.Value(ctypes.c_long, 0)
        self.a = multiprocessing.Value(ctypes.c_double, float('nan'))
    def add(self, x):
        with self.n.get_lock():
            self.n.value += 1
            with self.a.get_lock():
                if self.n.value == 1:
                    self.a.value = x
                else:
                    self.a.value = x*self.alpha + self.a.value*(1-self.alpha)
    @property
    def avg(self):
        return self.a.value
time_read = Averager()
time_decode = Averager()
time_insert = Averager()
class RateAvg:
    def __init__(self, alpha=.01):
        self.interval = Averager(alpha=alpha)
        self.last = multiprocessing.Value(ctypes.c_double, 0)
        self.last.value = time.time()
    def mark(self):
        self.interval.add(time.time() - self.last.value)
        self.last.value = time.time()
    @property
    def rate(self):
        return 1./self.interval.avg
rate_read = RateAvg()
rate_decode = RateAvg()
rate_insert = RateAvg()



def print_status(extra=''):
    print(TYPE,
          f"Queues D/I: {queue1.qsize():3d} {queue2.qsize():3d}  "
          f"Rates R/D/I: {1/time_read.avg:5.2f}/s {1/time_decode.avg:5.2f}/s {1/time_insert.avg:5.2f}/s ({rate_read.rate:5.2f}/s {rate_decode.rate:5.2f}/s {rate_insert.rate:5.2f}/s) " + extra
                  )


def read(file_):
    """Read and decompress lines from file, put in queue
    """
    os.nice(5)
    queue = queue1
    file_size = os.stat(file_).st_size
    sub = os.path.basename(file_).rsplit('_', 1)[0]
    lines_file = 0
    accumulated = [ ]
    start = time.time()

    #print(f"read: starting {file_}")
    for i, (line, file_bytes_processed) in enumerate(zst.read_lines_zst(file_)):
        lines_file += 1
        accumulated.append((i, line))
        # Every 100000 lines, print status and push into queue.
        if lines_file % (args.chunk_lines*args.print_every) == 0:
            #created = datetime.utcfromtimestamp(int(obj['created_utc']))
            print_status(f"{sub:20s} "
                  #f"{created.strftime('%Y-%m-%d %H:%M:%S')} : "
                  f"Tot%: {((file_bytes_processed + bytes_processed.value) / bytes_total) * 100:5.1f}% "
                  f"(bad: {lines_bad.value:,}) "
                  f"File%: {(file_bytes_processed / file_size) * 100:3.0f}% "
                  f"Line {lines_file:,} ({lines_total.value:,}) "
                  )
            sys.stdout.flush()
            time_read.add(time.time() - start)
            rate_read.mark()
            start = time.time()
            queue.put((file_, accumulated))
            accumulated = [ ]
    # Put all the last stuff into queue
    time_read.add(time.time() - start)
    rate_read.mark()
    queue.put((file_, accumulated))
    with bytes_processed.get_lock():
        bytes_processed.value += file_bytes_processed
    with lines_total.get_lock():
        lines_total.value += lines_file
    sys.stdout.flush()
    print_status(f"{sub:20s} "
          #f"{created.strftime('%Y-%m-%d %H:%M:%S')} : "
          f"Tot%: {((bytes_processed.value) / bytes_total) * 100:5.1f}% "
          f"(bad: {lines_bad.value:,}) "
          )
    #print(f"read: done with {file_}")
    #queue.close()


def decode(queue_in, queue_out):
    """Read from queue, decode JSON and make fields, add to next queue"""
    os.nice(5)
    accumulated = [ ]
    # While there is stuff in the queue...
    for i in itertools.count():
        x = queue_in.get()
        start = time.time()
        # This is our sentinel to end processing.  It seems the queue
        # should raise ValueError once closed, but I haven't gotten
        # that to work.  Maybe it needs to be closed in every process.
        if x == 'DONE':
            print(' '*7, 'decode: done')
            break

        # For each line, load JSON and accumulate whatever our final
        # values will be.
        file_, lines = x
        #if i % args.print_every == 0:
        #    print_status(f'decode: {len(lines)}')
        #    sys.stdout.flush()
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
        time_decode.add(time.time() - start)
        rate_decode.mark()

        queue_out.put(accumulated)
        accumulated = [ ]
    # Don't forget to push the final stuff through.
    queue_out.put(accumulated)



def batched(iterable, n, *, strict=False):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError('batched(): incomplete batch')
        yield batch


INSERT = f'INSERT INTO {TABLE} VALUES({",".join(["?"]*len(COLUMNS))})'
def insert(queue):
    """Read from queue and insert into the database"""
    def get():
        """Generator to indefinitely return stuff to insert into the database"""
        for i in itertools.count():
            x = queue.get()
            start = time.time()
            if x == 'DONE':
                print(' '*15, 'insert: done')
                break
            #if i % args.print_every == 0:
            #    print_status('insert')
            #    sys.stdout.flush()
            with n_insert.get_lock():
                n_insert.value += 1
            # Doing it here:
            #yield from x
            # insert here:
            #conn.executemany(INSERT, x)
            #conn.commit()
            #conn.execute('PRAGMA shrink_memory;')
            #print(f"Committed batch {i}")
            #sys.stdout.flush()
            #
            # Direct inserts here:
            conn.executemany(INSERT, x)
            if (i+1) % args.insert_batch == 0:
                conn.commit()
                conn.execute('PRAGMA shrink_memory;')
                print(f"Committed batch {i}")
            #
            time_insert.add(time.time() - start)
            rate_insert.mark()
        conn.commit()
        conn.execute('PRAGMA shrink_memory;')
        print(f"Committed batch FINAL")
    get()
    #for i_batch, batch in enumerate(batched(get(), args.insert_batch)):
    #    conn.executemany(INSERT, get())
    #    conn.commit()
    #    conn.execute('PRAGMA shrink_memory;')
    #    print(f"Committed batch {i_batch}")
    #    sys.stdout.flush()



# Verify that --comments is used for comment files and vice versa
def noneint(x): return x if x is None else int(x)
start = stop = interval = None
if args.sub_list and (m := re.search(r'^(?P<base>[^\[]*)(\[(?P<start>\d)*(:(?P<stop>\d*)(:(?P<interval>\d*))?)?\])', args.sub_list)):
    args.sub_list = m['base']
    start, stop, interval = noneint(m['start']), noneint(m['stop']), noneint(m['interval'])
if args.sub_list == '*':
    base = args.files[0]
    if args.comments:
        args.files = glob.glob(os.path.join(base, '*_comments.zst'))
    else:
        args.files = glob.glob(os.path.join(base, '*_submissions.zst'))
elif args.sub_list:
    base = args.files[0]
    sub_list = open(args.sub_list).read().split()
    if args.comments:
        args.files = [os.path.join(base, sub+'_comments.zst') for sub in sub_list]
    else:
        args.files = [os.path.join(base, sub+'_submissions.zst') for sub in sub_list]
else:
    print(args.files)
    print(args.sub_list)
    if args.comments:
        assert all(x.endswith('_comments.zst') for x in args.files), "--comments but not all files end in _comments.zst"
    else:
        assert all(x.endswith('_submissions.zst') for x in args.files), "not all files end in _submissions.zst"
for file_ in args.files:
    assert os.path.exists(file_)
if start is not None or stop is not None or interval is not None:
    args.files = args.files[start:stop:interval]


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
queue1 = multiprocessing.Queue(maxsize=10)
queue2 = multiprocessing.Queue(maxsize=10)

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

conn.execute('PRAGMA journal_mode = delete;')

print(f"Total walltime: {runtime()} s")
print(f"Total process time (user+sys): {sum(os.times()[:4])} s") # user+child processes
