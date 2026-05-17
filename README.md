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

Interactively open from the command line:

```console
$ module load sqlite
$ sqlite3 /PATH/TO/THE/DATABASE.sqlite3
```

You can directly iterate over rows that come out:
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

### Common qeries

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
