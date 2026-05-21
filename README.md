# Social media data import tools

These include some tools to import social media data into sqlite3
databases.  This allows more efficient access compared to parsing csv
files directly.


## Hints on using the data

### Why databases?

You will be given some database files for your use.

The basic use is by executing queries and getting tabular data
out. Imagine it's like reading a CSV file, but you can do
pre-filtering in the database engine *before* you get the data.  Since
the database engine is very fast and designed for this, it saves you
time and you can focus on only implementing interesting analyzes
yourself.

Advantages include:

- faster since it doesn't have to parse strings into numbers, etc
- knows more about types than raw csv (ints come out as ints, etc)
- if you want to do some basic filtering without reading everything
  in, you can do that (and since it pushes it to C and the database
  layer, it's must faster than reading all and filtering). You don't
  need as many types of intermediate files.
- (then if you want to get fancier, you can slowly add more fancy queries, but   isn't needed)

Disadvantages include:

- Having to learn something new to use it (but it's a really valuable
  skill anyway)
- Having to do a lot of work to insert the data into the database (but
  a lot of that work is basic data pre-processing - doing it once
  might be a worthy time investment anyway)



### Usage

Interactively open from the command line.  The `sqlite` module
provides a command line interface for testing queries, and we almost
always start by building queries this way before going to Python.

```console
$ module load ml triton/2025.1-gcc sqlite/3.46.0
$ sqlite3 /PATH/TO/THE/DATABASE.sqlite3
```

When you need to access data from Python, there are various options.
You can directly iterate over rows that come out (if you don't need to
store them all in memory at once):
```python
conn = sqlite3.connect("file:/PATH/TO/THE/DATABASE.sqlite3?immutable=1&mode=ro", uri=True)
for sub, title, author, time in conn.execute('SELECT subreddit, title, author, created_utc FROM submissions LIMIT 10'):
    print(f"{author} posted {title} to {sub}")
```

Reading the data directly into a pandas dataframe:
```python
import sqlite3
import pandas as pd
# could give plain filename, but this lets us open read-only.
db = sqlite3.connect('file:/scratch/cs/socialmediadata/db.sqlite3?mode=ro&immutable=1', uri=True)
df = pd.read_sql("QUERY", db)
# do stuff with the dataframe
```

Loading into a networkx graph:
```python
import sqlite3
db = sqlite3.connect('file:/scratch/cs/socialmediadata/db.sqlite3?mode=ro&immutable=1', uri=True)
G = networkx.DiGraph()
G.add_edges_from(conn.execute('SELECT parent_id, id FROM comments '))
```

### Understanding what is in a database

First, one would usually understand the data within a database by
using the command line, before going to Python.  This allows quick
iteration and understanding.  (when the data is very long, sometimes
it is useful to add a SQL `WHERE` or `LIMIT` to reduce the amount of
data while building the query, before trying to run it on the whole
database).

From the command line, you can check amount of data by subreddit and
year:

```sqlite
sqlite> select strftime('%Y', created_utc, 'unixepoch') as year, subreddit, count(*), count(distinct(author)) from submissions group by year, subreddit order by year, subreddit;
year  subreddit           count(*)  count(distinct(author))
----  ------------------  --------  -----------------------
2022  aaa                 4744      1838
2022  bbbbb               10447     5530
2023  aaa                 30        9
2023  bbbbb               206064    63465
```

You can find the first and latest post on a subreddit:
```sqlite
sqlite> select min(strftime('%Y-%m-%d', created_utc, 'unixepoch')) as first_post, max(strftime('%Y-%m-%d', created_utc, 'unixepoch')) as latest_post, subreddit from submissions group by subreddit order by subreddit;
first_post  latest_post  subreddit
----------  -----------  ------------------
2023-03-28  2025-12-31   aaa
2022-08-26  2025-12-31   bbbbb
```



### Common queies

From here on out, it's all about making the right queries you need.
SQL is fairly standardized so whatever you may know, can be used here.
A lot will probably be learned by asking your colleagues what they
use, since the list below can never be kept up to date.

TODO: add sample queries (ask your colleagues for now)

Below are two queries that fetch comments/posts within a certain month in a certain subreddit:

```python
import sqlite, datetime
conn = sqlite3.connect("file:/scratch/FILEPATH.sqlite3?immutable=1&mode=ro", uri=True)

year = 2020
month = 10
ts_start = datetime.datetime(year, month, 1, 0).timestamp()
ts_end = datetime.datetime(year, month + 1, 1, 0).timestamp()

df_comment = pd.read_sql("SELECT subreddit, author, created_utc, score, body, id, parent_id, link_id FROM comments WHERE subreddit='%s' AND created_utc > %d AND created_utc < %d" % (subreddit, ts_start, ts_end), conn)
df_post = pd.read_sql("SELECT subreddit, author, created_utc, score, title, selftext, id FROM submissions WHERE subreddit='%s' AND created_utc > %d AND created_utc < %d" % (subreddit, ts_start, ts_end), conn)
```
