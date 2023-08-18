import duckdb

if __name__ == "__main__":

    db = duckdb.connect('reddit.db', read_only=True)
    print(db.sql("select author, count(*) as cnt from comments where subreddit='The_Donald' group by author order by -cnt limit 10").fetchall())
