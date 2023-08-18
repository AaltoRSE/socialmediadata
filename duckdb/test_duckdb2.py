import duckdb

import pandas

if __name__ == "__main__":

    db = duckdb.connect('reddit.db', read_only=True)
    print(db.sql("select subreddit, author, count(*) as count, avg(score) as avgscore from comments where typeof(score)=='BIGINT' group by subreddit,author order by subreddit, -avgscore").df())
