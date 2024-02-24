#!/usr/bin/env python
# coding=UTF-8

import argparse
import glob
import os
from pathlib import Path
from resource import getrlimit, setrlimit, RLIMIT_NOFILE
import sys
import time

import duckdb

BATCHSIZE = 10000


def batched(iter, n):
    """like itertools.batched but that is python 3.12+"""
    return (iter[i:i + n] for i in range(0, len(iter), n))

def populate_db(db, file_paths, ncpus=1, memlimit=None):
    settings = f'compression=zstd, format=newline_delimited, union_by_name=true, ignore_errors=true'

    db = duckdb.connect(db, read_only=False)
    db.execute('SET enable_progress_bar=true')
    if ncpus > 0:
        db.execute(f'SET threads TO {ncpus}')
        db.execute(f'SET external_threads TO {ncpus}')
        print(f"Setting CPUs to {ncpus}")

    # Manage alreday-loaded files
    db.execute('CREATE TABLE IF NOT EXISTS loaded_files (file VARCHAR, time INT)')
    loaded_files = { x[0] for x in db.execute('SELECT file FROM loaded_files').fetchall() }
    #print(loaded_files)
    db.commit()

    # Split files based on whether they are comments or submissions and write paths in the correct format

    comments_paths = []
    submissions_paths = []

    n_already_loaded = 0
    for fp in file_paths:
        if fp in loaded_files:
            #print(f"File already loaded: {fp}")
            n_already_loaded += 1
            continue
        if fp.endswith('comments.zst'):
            comments_paths.append(fp)
        elif fp.endswith('submissions.zst'):
            submissions_paths.append(fp)
        else:
            raise Exception(f'Strange path {fp} encountered!')
    print(f"Files already loaded: {n_already_loaded}")
    sys.stdout.flush()

    #comments_paths = ['"{}"'.format(x) for x in comments_paths]
    #submissions_paths = ['"{}"'.format(x) for x in submissions_paths]

    #comments_paths = ', '.join(comments_paths)
    #submissions_paths = ', '.join(submissions_paths)

    # See type names in types API:
    # https://duckdb.org/docs/api/python/types

    submissions_schema = """{
    subreddit: 'VARCHAR',
    id: 'VARCHAR',
    created_utc: 'BIGINT',
    author: 'VARCHAR',
    hidden: 'BIGINT',
    is_self: 'BOOLEAN',
    domain: 'VARCHAR',
    num_comments: 'BIGINT',
    over_18: 'BOOLEAN',
    score: 'BIGINT',
    subreddit_id: 'VARCHAR',
    title: 'VARCHAR',
    url: 'VARCHAR',
    selftext: 'VARCHAR',
    }"""

    comments_schema = """{
    subreddit: 'VARCHAR',
    author: 'VARCHAR',
    id: 'VARCHAR',
    link_id: 'VARCHAR',
    parent_id: 'VARCHAR',
    controversiality: 'BIGINT',
    created_utc: 'BIGINT',
    distinguished: 'BIGINT',
    downs: 'BIGINT',
    gilded: 'BIGINT',
    score: 'BIGINT',
    score_hidden: 'BOOLEAN',
    subreddit_id: 'VARCHAR',
    name: 'VARCHAR',
    body: 'VARCHAR',
    moderator: 'VARCHAR',
    }"""

    # Infer memory limit
    if not memlimit and 'SLURM_MEM_PER_NODE' in os.environ:
        memlimit = int(os.environ['SLURM_MEM_PER_NODE']) - 500
        memlimit = f'{memlimit}M'
        print(f"Memory from SLURM_MEM_PER_NODE: {memlimit")
    if not memlimit and 'SLURM_MEM_PER_CPU' in os.environ:
        memlimit = int(os.environ['SLURM_MEM_PER_CPU']) * int(os.environ.get('SLURM_CPUS_PER_TASK', 1)) - 500
        memlimit = f'{memlimit}M'
        print(f"Memory from SLURM_MEM_PER_CPU: {memlimit}")
    if memlimit:
        db.execute(f"SET memory_limit = '{memlimit}'")
        print(f"Setting mem limit to {memlimit}")

    print(ncpus)
    if len(comments_paths) > 0:
        comments_paths_batched = batched(comments_paths, BATCHSIZE)
        comments = next(comments_paths_batched)
        comments = sort_by_size(comments)
        print(f'{time.ctime()} Ingesting comments (batch 1 of {len(comments_paths)//BATCHSIZE+1}): {comments[0]} > ?')
        sizes = ' '.join(str(Path(c).stat().st_size) for c in comments)
        print(f'Sizes: {sizes}')
        sys.stdout.flush()
        comments_str = ",".join(f'"{x}"' for x in comments)
        db.begin()
        db.execute(
            f'CREATE TABLE IF NOT EXISTS comments '
            f'AS (SELECT * FROM '
            f'read_json_auto([{comments_str}], {settings}, columns={comments_schema}))'
            )
        db.executemany('INSERT INTO loaded_files VALUES (?, ?)', [(c, time.time()) for c in comments])
        db.commit()
        for i, comments in enumerate(comments_paths_batched, start=2):
            comments = sort_by_size(comments)
            print(f'{time.ctime()} Ingesting comments (batch {i} of {len(comments_paths)//BATCHSIZE+1}): {comments[0]} > ?')
            sizes = ' '.join(str(Path(c).stat().st_size) for c in comments)
            print(f'Sizes: {sizes}')
            sys.stdout.flush()
            comments_str = ",".join(f'"{x}"' for x in comments)
            db.begin()
            db.execute(
                f'INSERT INTO comments '
                f'(SELECT * FROM '
                f'read_json_auto([{comments_str}], {settings}, columns={comments_schema}))'
                )
            db.executemany('INSERT INTO loaded_files VALUES (?, ?)', [(c, time.time()) for c in comments])
            db.commit()

    if len(submissions_paths) > 0:
        submissions_paths_batched = batched(submissions_paths, BATCHSIZE)
        submissions = next(submissions_paths_batched)
        submissions = sort_by_size(submissions)
        print(f'{time.ctime()} Ingesting submissions (batch 1 of {len(submissions_paths)//BATCHSIZE+1}): {submissions[0]} > ?')
        sizes = ' '.join(str(Path(s).stat().st_size) for s in submissions)
        print(f'Sizes: {sizes}')
        sys.stdout.flush()
        submissions_str = ",".join(f'"{x}"' for x in submissions)
        db.begin()
        db.execute(
            f'CREATE TABLE IF NOT EXISTS submissions '
            f'AS (SELECT * FROM '
            f'read_json_auto([{submissions_str}], {settings}, columns={submissions_schema}))'
            )
        db.executemany('INSERT INTO loaded_files VALUES (?, ?)', [(s, time.time()) for s in submissions])
        db.commit()
        for i, submissions in enumerate(submissions_paths_batched, start=2):
            submissions = sort_by_size(submissions)
            print(f'{time.ctime()} Ingesting submissions (batch {i} of {len(submissions_paths)//BATCHSIZE+1}): {submissions[0]} > ?')
            sizes = ' '.join(str(Path(s).stat().st_size) for s in submissions)
            print(f'Sizes: {sizes}')
            sys.stdout.flush()
            submissions_str = ",".join(f'"{x}"' for x in submissions)
            db.begin()
            db.execute(
                f'CREATE TABLE IF NOT EXISTS submissions '
                f'AS (SELECT * FROM '
                f'read_json_auto([{submissions_str}], {settings}, columns={submissions_schema}))'
                )
            db.executemany('INSERT INTO loaded_files VALUES (?, ?)', [(s, time.time()) for s in submissions])
            db.commit()
    db.close()

def sort_by_size(files):
    sizes_x = [ (Path(x).stat().st_size, x) for x in files ]
    sizes_x.sort(reverse=True)
    return [ x[1] for x in sizes_x ]


if __name__ == "__main__":

    file_paths = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument('db', type=str, help="DuckDB database file")
    parser.add_argument('files', nargs='+', help="Files to ingest")
    parser.add_argument('-c','--cpus', type=int, default=int(os.getenv("SLURM_CPUS_PER_TASK",1)), help="How many CPUs to use, default %(default)s")
    parser.add_argument('-b','--batchsize', type=int, default=BATCHSIZE, help="How many files to import at once")
    parser.add_argument('--memlimit', help="Set a memory limit")
    args = parser.parse_args()

    BATCHSIZE = args.batchsize
    setrlimit(RLIMIT_NOFILE, (max(BATCHSIZE+1000, getrlimit(RLIMIT_NOFILE)[0]), getrlimit(RLIMIT_NOFILE)[1]))

    files = args.files = sum((glob.glob(f) for f in args.files), [])
    populate_db(args.db, args.files, ncpus=args.cpus, memlimit=args.memlimit)
