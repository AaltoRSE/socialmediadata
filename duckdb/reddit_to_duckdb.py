#!/usr/bin/env python
# coding=UTF-8

import argparse
import glob
import os
import sys

import duckdb


def populate_db(db, file_paths, ncpus=1):

    # Split files based on whether they are comments or submissions and write paths in the correct format

    comments_paths = []
    submissions_paths = []

    for fp in file_paths:
        if 'comments' in fp:
            comments_paths.append(fp)
        elif 'submissions' in fp:
            submissions_paths.append(fp)
        else:
            raise Exception(f'Strange path {fp} encountered!')

    comments_paths = ['"{}"'.format(x) for x in comments_paths]
    submissions_paths = ['"{}"'.format(x) for x in submissions_paths]

    comments_paths = ', '.join(comments_paths)
    submissions_paths = ', '.join(submissions_paths)

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

    settings = f'compression=zstd, format=newline_delimited, union_by_name=true, ignore_errors=true'

    db = duckdb.connect(db, read_only=False)
    db.execute('SET enable_progress_bar=true')
    db.execute(f'SET threads TO {ncpus}')

    print(ncpus)
    if len(comments_paths) > 0:
        print(f'Ingesting these comments paths: {comments_paths}')
        db.execute(f'CREATE TABLE IF NOT EXISTS comments AS (SELECT * FROM read_json_auto([{comments_paths}], {settings}, columns={comments_schema}))')
    if len(submissions_paths) > 0:
        print(f'Ingesting these submissions paths: {submissions_paths}')
        db.execute(f'CREATE TABLE IF NOT EXISTS submissions AS (SELECT * FROM read_json_auto([{submissions_paths}], {settings}, columns={submissions_schema}))')
    db.close()


if __name__ == "__main__":

    file_paths = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument('db', type=str, help="DuckDB database file")
    parser.add_argument('files', nargs='+', help="Files to ingest")
    parser.add_argument('-c','--cpus', type=int, default=int(os.getenv("SLURM_CPUS_PER_TASK",1)), help="How many CPUs to use")
    args = parser.parse_args()

    #files = args.files = sum((glob.glob(f) for f in args.files), [])
    populate_db(args.db, args.files, ncpus=args.cpus)
