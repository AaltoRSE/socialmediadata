import argparse
from datetime import datetime
import ujson as json
import os
import sqlite3
import sys

import zst


conn = sqlite3.connect(sys.argv[1])
conn.execute('PRAGMA page_size = 32768;')
conn.execute('PRAGMA journal_mode = off;')
conn.execute('CREATE TABLE IF NOT EXISTS submissions (sub TEXT, time INTEGER, author TEXT, body BLOB)')



def iter_data(file_, sub=None, file_size=None):
    global file_lines
    global bad_lines
    global bytes_processed
    for i, (line, file_bytes_processed) in enumerate(zst.read_lines_zst(file_)):
        try:
            obj = json.loads(line)
            #pprint(obj)
            #if i > 10: break
            #created = datetime.utcfromtimestamp(int(obj['created_utc']))
            #temp = obj[field] == value
        except (KeyError, json.JSONDecodeError) as err:
            bad_lines += 1
        file_lines += 1
        if file_lines % 100000 == 0:
            created = datetime.utcfromtimestamp(int(obj['created_utc']))
            print(f"{sub} {created.strftime('%Y-%m-%d %H:%M:%S')} : "
                      f"{file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}: "
                      f"{(file_bytes_processed / file_size) * 100:.0f}% "
                      f"({((file_bytes_processed + bytes_processed) / bytes_total) * 100:.0f}%)"
                      )
        yield (sub, obj['created_utc'], obj['author'], line)
    bytes_processed += file_bytes_processed


parser = argparse.ArgumentParser()
parser.add_argument('db')
parser.add_argument('files', nargs='+')
args = parser.parse_args()


bytes_processed = 0
bytes_total = sum(os.stat(file_).st_size for file_ in args.files)
file_lines = 0
bad_lines = 0

for file_ in args.files:
    sub = os.path.basename(file_).rsplit('_', 1)[0]
    file_size = os.stat(file_).st_size

    conn.executemany('INSERT INTO submissions VALUES(?, ?, ?, ?)', iter_data(file_, sub=sub, file_size=file_size))

