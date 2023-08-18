# duckdb

## Create environment

```sh
mamba env create -f environment.yml
```

## Ingesting data

```sh
python reddit_to_duckdb.py path_to_file.zst
```

## Test duckdb

```
time python test_duckdb.py
time python test_duckdb2.py
```
